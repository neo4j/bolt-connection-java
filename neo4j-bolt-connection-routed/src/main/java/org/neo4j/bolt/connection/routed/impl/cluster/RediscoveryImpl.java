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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.net.ssl.SSLHandshakeException;
import org.neo4j.bolt.connection.AccessMode;
import org.neo4j.bolt.connection.AuthToken;
import org.neo4j.bolt.connection.BoltAgent;
import org.neo4j.bolt.connection.BoltConnection;
import org.neo4j.bolt.connection.BoltConnectionProvider;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.ClusterComposition;
import org.neo4j.bolt.connection.DomainNameResolver;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.ResponseHandler;
import org.neo4j.bolt.connection.RoutingContext;
import org.neo4j.bolt.connection.SecurityPlan;
import org.neo4j.bolt.connection.exception.BoltDiscoveryException;
import org.neo4j.bolt.connection.exception.BoltFailureException;
import org.neo4j.bolt.connection.exception.BoltProtocolException;
import org.neo4j.bolt.connection.exception.BoltServiceUnavailableException;
import org.neo4j.bolt.connection.exception.BoltUnsupportedFeatureException;
import org.neo4j.bolt.connection.exception.MinVersionAcquisitionException;
import org.neo4j.bolt.connection.message.Messages;
import org.neo4j.bolt.connection.routed.ClusterCompositionLookupResult;
import org.neo4j.bolt.connection.routed.Rediscovery;
import org.neo4j.bolt.connection.routed.RoutingTable;
import org.neo4j.bolt.connection.routed.impl.AuthTokenManagerExecutionException;
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
    private final RoutingContext routingContext;
    private final BoltAgent boltAgent;
    private final String userAgent;
    private final int connectTimeoutMillis;

    public RediscoveryImpl(
            BoltServerAddress initialRouter,
            Function<BoltServerAddress, Set<BoltServerAddress>> resolver,
            LoggingProvider logging,
            DomainNameResolver domainNameResolver,
            RoutingContext routingContext,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis) {
        this.initialRouter = initialRouter;
        this.log = logging.getLog(getClass());
        this.resolver = resolver;
        this.domainNameResolver = requireNonNull(domainNameResolver);
        this.routingContext = routingContext;
        this.boltAgent = boltAgent;
        this.userAgent = userAgent;
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    @Override
    public CompletionStage<ClusterCompositionLookupResult> lookupClusterComposition(
            SecurityPlan securityPlan,
            RoutingTable routingTable,
            Function<BoltServerAddress, BoltConnectionProvider> connectionProviderGetter,
            Set<String> bookmarks,
            String impersonatedUser,
            Supplier<CompletionStage<AuthToken>> authTokenStageSupplier,
            BoltProtocolVersion minVersion) {
        var result = new CompletableFuture<ClusterCompositionLookupResult>();
        // if we failed discovery, we will chain all errors into this one.
        var baseError = new BoltServiceUnavailableException(
                String.format(NO_ROUTERS_AVAILABLE, routingTable.database().description()));
        lookupClusterComposition(
                securityPlan,
                routingTable,
                connectionProviderGetter,
                result,
                bookmarks,
                impersonatedUser,
                authTokenStageSupplier,
                minVersion,
                baseError);
        return result;
    }

    private void lookupClusterComposition(
            SecurityPlan securityPlan,
            RoutingTable routingTable,
            Function<BoltServerAddress, BoltConnectionProvider> connectionProviderGetter,
            CompletableFuture<ClusterCompositionLookupResult> result,
            Set<String> bookmarks,
            String impersonatedUser,
            Supplier<CompletionStage<AuthToken>> authTokenStageSupplier,
            BoltProtocolVersion minVersion,
            Throwable baseError) {
        lookup(
                        securityPlan,
                        routingTable,
                        connectionProviderGetter,
                        bookmarks,
                        impersonatedUser,
                        authTokenStageSupplier,
                        minVersion,
                        baseError)
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
            SecurityPlan securityPlan,
            RoutingTable routingTable,
            Function<BoltServerAddress, BoltConnectionProvider> connectionProviderGetter,
            Set<String> bookmarks,
            String impersonatedUser,
            Supplier<CompletionStage<AuthToken>> authTokenStageSupplier,
            BoltProtocolVersion minVersion,
            Throwable baseError) {
        CompletionStage<ClusterCompositionLookupResult> compositionStage;

        if (routingTable.preferInitialRouter()) {
            compositionStage = lookupOnInitialRouterThenOnKnownRouters(
                    securityPlan,
                    routingTable,
                    connectionProviderGetter,
                    bookmarks,
                    impersonatedUser,
                    authTokenStageSupplier,
                    minVersion,
                    baseError);
        } else {
            compositionStage = lookupOnKnownRoutersThenOnInitialRouter(
                    securityPlan,
                    routingTable,
                    connectionProviderGetter,
                    bookmarks,
                    impersonatedUser,
                    authTokenStageSupplier,
                    minVersion,
                    baseError);
        }

        return compositionStage;
    }

    private CompletionStage<ClusterCompositionLookupResult> lookupOnKnownRoutersThenOnInitialRouter(
            SecurityPlan securityPlan,
            RoutingTable routingTable,
            Function<BoltServerAddress, BoltConnectionProvider> connectionProviderGetter,
            Set<String> bookmarks,
            String impersonatedUser,
            Supplier<CompletionStage<AuthToken>> authTokenStageSupplier,
            BoltProtocolVersion minVersion,
            Throwable baseError) {
        Set<BoltServerAddress> seenServers = new HashSet<>();
        return lookupOnKnownRouters(
                        securityPlan,
                        routingTable,
                        connectionProviderGetter,
                        seenServers,
                        bookmarks,
                        impersonatedUser,
                        authTokenStageSupplier,
                        minVersion,
                        baseError)
                .thenCompose(compositionLookupResult -> {
                    if (compositionLookupResult != null) {
                        return completedFuture(compositionLookupResult);
                    }
                    return lookupOnInitialRouter(
                            securityPlan,
                            routingTable,
                            connectionProviderGetter,
                            seenServers,
                            bookmarks,
                            impersonatedUser,
                            authTokenStageSupplier,
                            minVersion,
                            baseError);
                });
    }

    private CompletionStage<ClusterCompositionLookupResult> lookupOnInitialRouterThenOnKnownRouters(
            SecurityPlan securityPlan,
            RoutingTable routingTable,
            Function<BoltServerAddress, BoltConnectionProvider> connectionProviderGetter,
            Set<String> bookmarks,
            String impersonatedUser,
            Supplier<CompletionStage<AuthToken>> authTokenStageSupplier,
            BoltProtocolVersion minVersion,
            Throwable baseError) {
        Set<BoltServerAddress> seenServers = emptySet();
        return lookupOnInitialRouter(
                        securityPlan,
                        routingTable,
                        connectionProviderGetter,
                        seenServers,
                        bookmarks,
                        impersonatedUser,
                        authTokenStageSupplier,
                        minVersion,
                        baseError)
                .thenCompose(compositionLookupResult -> {
                    if (compositionLookupResult != null) {
                        return completedFuture(compositionLookupResult);
                    }
                    return lookupOnKnownRouters(
                            securityPlan,
                            routingTable,
                            connectionProviderGetter,
                            new HashSet<>(),
                            bookmarks,
                            impersonatedUser,
                            authTokenStageSupplier,
                            minVersion,
                            baseError);
                });
    }

    private CompletionStage<ClusterCompositionLookupResult> lookupOnKnownRouters(
            SecurityPlan securityPlan,
            RoutingTable routingTable,
            Function<BoltServerAddress, BoltConnectionProvider> connectionProviderGetter,
            Set<BoltServerAddress> seenServers,
            Set<String> bookmarks,
            String impersonatedUser,
            Supplier<CompletionStage<AuthToken>> authTokenStageSupplier,
            BoltProtocolVersion minVersion,
            Throwable baseError) {
        CompletableFuture<ClusterComposition> result = CompletableFuture.completedFuture(null);
        for (var address : routingTable.routers()) {
            result = result.thenCompose(composition -> {
                if (composition != null) {
                    return completedFuture(composition);
                } else {
                    return lookupOnRouter(
                            securityPlan,
                            address,
                            true,
                            routingTable,
                            connectionProviderGetter,
                            seenServers,
                            bookmarks,
                            impersonatedUser,
                            authTokenStageSupplier,
                            minVersion,
                            baseError);
                }
            });
        }
        return result.thenApply(
                composition -> composition != null ? new ClusterCompositionLookupResult(composition) : null);
    }

    private CompletionStage<ClusterCompositionLookupResult> lookupOnInitialRouter(
            SecurityPlan securityPlan,
            RoutingTable routingTable,
            Function<BoltServerAddress, BoltConnectionProvider> connectionProviderGetter,
            Set<BoltServerAddress> seenServers,
            Set<String> bookmarks,
            String impersonatedUser,
            Supplier<CompletionStage<AuthToken>> authTokenStageSupplier,
            BoltProtocolVersion minVersion,
            Throwable baseError) {
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
                        securityPlan,
                        address,
                        false,
                        routingTable,
                        connectionProviderGetter,
                        null,
                        bookmarks,
                        impersonatedUser,
                        authTokenStageSupplier,
                        minVersion,
                        baseError);
            });
        }
        return result.thenApply(composition ->
                composition != null ? new ClusterCompositionLookupResult(composition, resolvedRouterSet) : null);
    }

    private CompletionStage<ClusterComposition> lookupOnRouter(
            SecurityPlan securityPlan,
            BoltServerAddress routerAddress,
            boolean resolveAddress,
            RoutingTable routingTable,
            Function<BoltServerAddress, BoltConnectionProvider> connectionProviderGetter,
            Set<BoltServerAddress> seenServers,
            Set<String> bookmarks,
            String impersonatedUser,
            Supplier<CompletionStage<AuthToken>> authTokenStageSupplier,
            BoltProtocolVersion minVersion,
            Throwable baseError) {
        var addressFuture = CompletableFuture.completedFuture(routerAddress);

        var future = new CompletableFuture<ClusterComposition>();
        var compositionFuture = new CompletableFuture<ClusterComposition>();
        var connectionRef = new AtomicReference<BoltConnection>();

        addressFuture
                .thenApply(address ->
                        resolveAddress ? resolveByDomainNameOrThrowCompletionException(address, routingTable) : address)
                .thenApply(address -> addAndReturn(seenServers, address))
                .thenCompose(address -> connectionProviderGetter
                        .apply(address)
                        .connect(
                                address,
                                routingContext,
                                boltAgent,
                                userAgent,
                                connectTimeoutMillis,
                                securityPlan,
                                null,
                                authTokenStageSupplier,
                                AccessMode.READ,
                                bookmarks,
                                null,
                                minVersion,
                                null,
                                (ignored) -> {},
                                Collections.emptyMap()))
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
                                routingTable.database().databaseName().orElse(null), impersonatedUser, bookmarks)))
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
        } else if (throwable instanceof AuthTokenManagerExecutionException) {
            abort = true;
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
