/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.bolt.connection.routed.impl.cluster;

import static java.lang.String.format;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.net.ssl.SSLHandshakeException;
import org.neo4j.bolt.connection.BoltConnection;
import org.neo4j.bolt.connection.BoltConnectionParameters;
import org.neo4j.bolt.connection.BoltConnectionSource;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.ClusterComposition;
import org.neo4j.bolt.connection.DomainNameResolver;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.ResponseHandler;
import org.neo4j.bolt.connection.RoutedBoltConnectionParameters;
import org.neo4j.bolt.connection.exception.BoltDiscoveryException;
import org.neo4j.bolt.connection.exception.BoltFailureException;
import org.neo4j.bolt.connection.exception.BoltProtocolException;
import org.neo4j.bolt.connection.exception.BoltServiceUnavailableException;
import org.neo4j.bolt.connection.exception.BoltUnsupportedFeatureException;
import org.neo4j.bolt.connection.exception.MinVersionAcquisitionException;
import org.neo4j.bolt.connection.message.Messages;
import org.neo4j.bolt.connection.observation.ImmutableObservation;
import org.neo4j.bolt.connection.routed.ClusterCompositionLookupResult;
import org.neo4j.bolt.connection.routed.Rediscovery;
import org.neo4j.bolt.connection.routed.RoutingTable;
import org.neo4j.bolt.connection.routed.impl.util.FutureUtil;
import org.neo4j.bolt.connection.summary.RouteSummary;

public class RediscoveryImpl implements Rediscovery {
    private static final String NO_ROUTERS_AVAILABLE =
            "Could not perform discovery for database '%s'. No routing server available.";
    private static final String RECOVERABLE_ROUTING_ERROR = "Failed to update routing table with server '%s'.";
    private static final String RECOVERABLE_DISCOVERY_ERROR_WITH_SERVER =
            "Received a recoverable discovery error with server '%s', "
                    + "will continue discovery with other routing servers if available. "
                    + "Complete failure is reported separately from this entry.";
    private static final String TRANSACTION_INVALID_BOOKMARK_CODE = "Neo.ClientError.Transaction.InvalidBookmark";
    private static final String TRANSACTION_INVALID_BOOKMARK_MIXTURE_CODE =
            "Neo.ClientError.Transaction.InvalidBookmarkMixture";
    private static final String STATEMENT_ARGUMENT_ERROR_CODE = "Neo.ClientError.Statement.ArgumentError";
    private static final String REQUEST_INVALID_CODE = "Neo.ClientError.Request.Invalid";
    private static final String STATEMENT_TYPE_ERROR_CODE = "Neo.ClientError.Statement.TypeError";

    private final BoltServerAddress initialRouter;
    private final System.Logger log;
    private final Function<BoltServerAddress, Set<BoltServerAddress>> resolver;
    private final DomainNameResolver domainNameResolver;
    private final List<Class<? extends Throwable>> discoveryAbortingErrors;

    public RediscoveryImpl(
            BoltServerAddress initialRouter,
            Function<BoltServerAddress, Set<BoltServerAddress>> resolver,
            LoggingProvider logging,
            DomainNameResolver domainNameResolver,
            List<Class<? extends Throwable>> discoveryAbortingErrors) {
        this.initialRouter = initialRouter;
        this.log = logging.getLog(getClass());
        this.resolver = resolver;
        this.domainNameResolver = requireNonNull(domainNameResolver);
        this.discoveryAbortingErrors = requireNonNull(discoveryAbortingErrors);
    }

    @Override
    public CompletionStage<ClusterCompositionLookupResult> lookupClusterComposition(
            RoutingTable routingTable,
            Function<BoltServerAddress, BoltConnectionSource<BoltConnectionParameters>> connectionSourceGetter,
            RoutedBoltConnectionParameters parameters,
            ImmutableObservation parentObservation) {
        var result = new CompletableFuture<ClusterCompositionLookupResult>();
        // if we failed discovery, we will chain all errors into this one.
        var baseError = new BoltServiceUnavailableException(
                String.format(NO_ROUTERS_AVAILABLE, routingTable.database().description()));
        lookupClusterComposition(
                routingTable, connectionSourceGetter, result, parameters, baseError, parentObservation);
        return result;
    }

    private void lookupClusterComposition(
            RoutingTable routingTable,
            Function<BoltServerAddress, BoltConnectionSource<BoltConnectionParameters>> connectionProviderGetter,
            CompletableFuture<ClusterCompositionLookupResult> result,
            RoutedBoltConnectionParameters parameters,
            Throwable baseError,
            ImmutableObservation parentObservation) {
        lookup(routingTable, connectionProviderGetter, parameters, baseError, parentObservation)
                .whenComplete((compositionLookupResult, completionError) -> {
                    var error = FutureUtil.completionExceptionCause(completionError);
                    if (error != null) {
                        result.completeExceptionally(error);
                    } else if (compositionLookupResult != null) {
                        result.complete(compositionLookupResult);
                    } else {
                        result.completeExceptionally(baseError);
                    }
                });
    }

    private CompletionStage<ClusterCompositionLookupResult> lookup(
            RoutingTable routingTable,
            Function<BoltServerAddress, BoltConnectionSource<BoltConnectionParameters>> connectionProviderGetter,
            RoutedBoltConnectionParameters parameters,
            Throwable baseError,
            ImmutableObservation parentObservation) {
        CompletionStage<ClusterCompositionLookupResult> compositionStage;

        if (routingTable.preferInitialRouter()) {
            compositionStage = lookupOnInitialRouterThenOnKnownRouters(
                    routingTable, connectionProviderGetter, parameters, baseError, parentObservation);
        } else {
            compositionStage = lookupOnKnownRoutersThenOnInitialRouter(
                    routingTable, connectionProviderGetter, parameters, baseError, parentObservation);
        }

        return compositionStage;
    }

    private CompletionStage<ClusterCompositionLookupResult> lookupOnKnownRoutersThenOnInitialRouter(
            RoutingTable routingTable,
            Function<BoltServerAddress, BoltConnectionSource<BoltConnectionParameters>> connectionProviderGetter,
            RoutedBoltConnectionParameters parameters,
            Throwable baseError,
            ImmutableObservation parentObservation) {
        Set<BoltServerAddress> seenServers = new HashSet<>();
        return lookupOnKnownRouters(
                        routingTable, connectionProviderGetter, seenServers, parameters, baseError, parentObservation)
                .thenCompose(compositionLookupResult -> {
                    if (compositionLookupResult != null) {
                        return completedFuture(compositionLookupResult);
                    }
                    return lookupOnInitialRouter(
                            routingTable,
                            connectionProviderGetter,
                            seenServers,
                            parameters,
                            baseError,
                            parentObservation);
                });
    }

    private CompletionStage<ClusterCompositionLookupResult> lookupOnInitialRouterThenOnKnownRouters(
            RoutingTable routingTable,
            Function<BoltServerAddress, BoltConnectionSource<BoltConnectionParameters>> connectionProviderGetter,
            RoutedBoltConnectionParameters parameters,
            Throwable baseError,
            ImmutableObservation parentObservation) {
        Set<BoltServerAddress> seenServers = emptySet();
        return lookupOnInitialRouter(
                        routingTable, connectionProviderGetter, seenServers, parameters, baseError, parentObservation)
                .thenCompose(compositionLookupResult -> {
                    if (compositionLookupResult != null) {
                        return completedFuture(compositionLookupResult);
                    }
                    return lookupOnKnownRouters(
                            routingTable,
                            connectionProviderGetter,
                            new HashSet<>(),
                            parameters,
                            baseError,
                            parentObservation);
                });
    }

    private CompletionStage<ClusterCompositionLookupResult> lookupOnKnownRouters(
            RoutingTable routingTable,
            Function<BoltServerAddress, BoltConnectionSource<BoltConnectionParameters>> connectionProviderGetter,
            Set<BoltServerAddress> seenServers,
            RoutedBoltConnectionParameters parameters,
            Throwable baseError,
            ImmutableObservation parentObservation) {
        CompletableFuture<ClusterComposition> result = CompletableFuture.completedFuture(null);
        for (var address : routingTable.routers()) {
            result = result.thenCompose(composition -> {
                if (composition != null) {
                    return completedFuture(composition);
                } else {
                    return lookupOnRouter(
                            address,
                            true,
                            routingTable,
                            connectionProviderGetter,
                            seenServers,
                            parameters,
                            baseError,
                            parentObservation);
                }
            });
        }
        return result.thenApply(
                composition -> composition != null ? new ClusterCompositionLookupResult(composition) : null);
    }

    private CompletionStage<ClusterCompositionLookupResult> lookupOnInitialRouter(
            RoutingTable routingTable,
            Function<BoltServerAddress, BoltConnectionSource<BoltConnectionParameters>> connectionProviderGetter,
            Set<BoltServerAddress> seenServers,
            RoutedBoltConnectionParameters parameters,
            Throwable baseError,
            ImmutableObservation parentObservation) {
        List<BoltServerAddress> resolvedRouters;
        try {
            resolvedRouters = resolve();
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        Set<BoltServerAddress> resolvedRouterSet = new HashSet<>(resolvedRouters);
        resolvedRouters.removeAll(seenServers);

        CompletableFuture<ClusterComposition> result = CompletableFuture.completedFuture(null);
        for (var address : resolvedRouters) {
            result = result.thenCompose(composition -> {
                if (composition != null) {
                    return completedFuture(composition);
                }
                return lookupOnRouter(
                        address,
                        false,
                        routingTable,
                        connectionProviderGetter,
                        null,
                        parameters,
                        baseError,
                        parentObservation);
            });
        }
        return result.thenApply(composition ->
                composition != null ? new ClusterCompositionLookupResult(composition, resolvedRouterSet) : null);
    }

    private CompletionStage<ClusterComposition> lookupOnRouter(
            BoltServerAddress routerAddress,
            boolean resolveAddress,
            RoutingTable routingTable,
            Function<BoltServerAddress, BoltConnectionSource<BoltConnectionParameters>> connectionProviderGetter,
            Set<BoltServerAddress> seenServers,
            RoutedBoltConnectionParameters parameters,
            Throwable baseError,
            ImmutableObservation parentObservation) {
        var addressFuture = CompletableFuture.completedFuture(routerAddress);

        var future = new CompletableFuture<ClusterComposition>();
        var compositionFuture = new CompletableFuture<ClusterComposition>();
        var connectionRef = new AtomicReference<BoltConnection>();

        addressFuture
                .thenApply(address ->
                        resolveAddress ? resolveByDomainNameOrThrowCompletionException(address, routingTable) : address)
                .thenApply(address -> addAndReturn(seenServers, address))
                .thenCompose(address -> connectionProviderGetter.apply(address).getConnection(parameters))
                .thenApply(connection -> {
                    connectionRef.set(connection);
                    return connection;
                })
                .thenCompose(connection -> connection.writeAndFlush(
                        new ResponseHandler() {
                            ClusterComposition clusterComposition;
                            Throwable throwable;

                            @Override
                            public void onError(Throwable throwable) {
                                this.throwable = throwable;
                            }

                            @Override
                            public void onRouteSummary(RouteSummary summary) {
                                clusterComposition = summary.clusterComposition();
                            }

                            @Override
                            public void onComplete() {
                                if (throwable != null) {
                                    compositionFuture.completeExceptionally(throwable);
                                } else {
                                    compositionFuture.complete(clusterComposition);
                                }
                            }
                        },
                        Messages.route(
                                routingTable.database().databaseName().orElse(null),
                                parameters.impersonatedUser(),
                                parameters.bookmarks()),
                        parentObservation))
                .thenCompose(ignored -> compositionFuture)
                .thenApply(clusterComposition -> {
                    if (clusterComposition.routers().isEmpty()
                            || clusterComposition.readers().isEmpty()) {
                        throw new CompletionException(
                                new BoltProtocolException(
                                        "Failed to parse result received from server due to no router or reader found in response."));
                    } else {
                        return clusterComposition;
                    }
                })
                .whenComplete((clusterComposition, throwable) -> {
                    var connection = connectionRef.get();
                    var connectionCloseStage =
                            connection != null ? connection.close() : CompletableFuture.completedStage(null);
                    var cause = FutureUtil.completionExceptionCause(throwable);
                    if (cause != null) {
                        connectionCloseStage.whenComplete((ignored1, ignored2) -> {
                            try {
                                var composition = handleRoutingProcedureError(
                                        FutureUtil.completionExceptionCause(throwable),
                                        routingTable,
                                        routerAddress,
                                        baseError);
                                future.complete(composition);
                            } catch (Throwable abortError) {
                                future.completeExceptionally(abortError);
                            }
                        });
                    } else {
                        connectionCloseStage.whenComplete((ignored1, ignored2) -> future.complete(clusterComposition));
                    }
                });

        return future;
    }

    @SuppressWarnings({"ThrowableNotThrown", "SameReturnValue"})
    private ClusterComposition handleRoutingProcedureError(
            Throwable error, RoutingTable routingTable, BoltServerAddress routerAddress, Throwable baseError) {
        if (mustAbortDiscovery(error)) {
            throw new CompletionException(error);
        }

        // Retryable error happened during discovery.
        var discoveryError = new BoltDiscoveryException(format(RECOVERABLE_ROUTING_ERROR, routerAddress), error);
        FutureUtil.combineErrors(baseError, discoveryError); // we record each failure here
        log.log(System.Logger.Level.WARNING, RECOVERABLE_DISCOVERY_ERROR_WITH_SERVER, routerAddress);
        log.log(
                System.Logger.Level.DEBUG,
                format(RECOVERABLE_DISCOVERY_ERROR_WITH_SERVER, routerAddress),
                discoveryError);
        routingTable.forget(routerAddress);
        return null;
    }

    @SuppressWarnings("SwitchStatementWithTooFewBranches")
    private boolean mustAbortDiscovery(Throwable throwable) {
        var abort = false;

        if (throwable instanceof BoltFailureException boltFailureException) {
            var code = boltFailureException.code();
            abort = switch (extractErrorClass(code)) {
                case "ClientError" -> {
                    if ("Security".equals(extractErrorSubClass(code))) {
                        yield !"Neo.ClientError.Security.AuthorizationExpired".equalsIgnoreCase(code);
                    } else {
                        if ("Neo.ClientError.Database.DatabaseNotFound".equalsIgnoreCase(code)) {
                            yield true;
                        } else {
                            yield switch (code) {
                                case TRANSACTION_INVALID_BOOKMARK_CODE,
                                        TRANSACTION_INVALID_BOOKMARK_MIXTURE_CODE,
                                        STATEMENT_ARGUMENT_ERROR_CODE,
                                        REQUEST_INVALID_CODE,
                                        STATEMENT_TYPE_ERROR_CODE -> true;
                                default -> false;
                            };
                        }
                    }
                }
                default -> false;};
        } else if (throwable instanceof IllegalStateException
                && "Connection provider is closed.".equals(throwable.getMessage())) {
            abort = true;
        } else if (throwable instanceof BoltUnsupportedFeatureException) {
            abort = true;
        } else if (throwable instanceof MinVersionAcquisitionException) {
            abort = true;
        } else if (throwable instanceof SSLHandshakeException) {
            abort = true;
        } else {
            for (var errorType : discoveryAbortingErrors) {
                if (errorType.isAssignableFrom(throwable.getClass())) {
                    abort = true;
                    break;
                }
            }
        }

        return abort;
    }

    private static String extractErrorClass(String code) {
        var parts = code.split("\\.");
        if (parts.length < 2) {
            return "";
        }
        return parts[1];
    }

    private static String extractErrorSubClass(String code) {
        var parts = code.split("\\.");
        if (parts.length < 3) {
            return "";
        }
        return parts[2];
    }

    @Override
    public List<BoltServerAddress> resolve() throws UnknownHostException {
        List<BoltServerAddress> resolvedAddresses = new LinkedList<>();
        UnknownHostException exception = null;
        for (var serverAddress : resolver.apply(initialRouter)) {
            try {
                resolveAllByDomainName(serverAddress).unicastStream().forEach(resolvedAddresses::add);
            } catch (UnknownHostException e) {
                if (exception == null) {
                    exception = e;
                } else {
                    exception.addSuppressed(e);
                }
            }
        }

        // give up only if there are no addresses to work with at all
        if (resolvedAddresses.isEmpty() && exception != null) {
            throw exception;
        }

        return resolvedAddresses;
    }

    private <T> T addAndReturn(Collection<T> collection, T element) {
        if (collection != null) {
            collection.add(element);
        }
        return element;
    }

    private BoltServerAddress resolveByDomainNameOrThrowCompletionException(
            BoltServerAddress address, RoutingTable routingTable) {
        try {
            var resolvedAddress = resolveAllByDomainName(address);
            routingTable.replaceRouterIfPresent(address, resolvedAddress);
            return resolvedAddress
                    .unicastStream()
                    .findFirst()
                    .orElseThrow(
                            () -> new IllegalStateException(
                                    "Unexpected condition, the ResolvedBoltServerAddress must always have at least one unicast address"));
        } catch (Throwable e) {
            throw new CompletionException(e);
        }
    }

    private ResolvedBoltServerAddress resolveAllByDomainName(BoltServerAddress address) throws UnknownHostException {
        return new ResolvedBoltServerAddress(
                address.host(), address.port(), domainNameResolver.resolve(address.host()));
    }
}
