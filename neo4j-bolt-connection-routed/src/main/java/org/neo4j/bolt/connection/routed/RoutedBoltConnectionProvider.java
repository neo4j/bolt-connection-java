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
package org.neo4j.bolt.connection.routed;

import static java.lang.String.format;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
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
import org.neo4j.bolt.connection.DatabaseName;
import org.neo4j.bolt.connection.DatabaseNameUtil;
import org.neo4j.bolt.connection.DomainNameResolver;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.MetricsListener;
import org.neo4j.bolt.connection.NotificationConfig;
import org.neo4j.bolt.connection.RoutingContext;
import org.neo4j.bolt.connection.SecurityPlan;
import org.neo4j.bolt.connection.exception.BoltConnectionAcquisitionException;
import org.neo4j.bolt.connection.exception.BoltFailureException;
import org.neo4j.bolt.connection.exception.BoltServiceUnavailableException;
import org.neo4j.bolt.connection.routed.impl.AuthTokenManagerExecutionException;
import org.neo4j.bolt.connection.routed.impl.RoutedBoltConnection;
import org.neo4j.bolt.connection.routed.impl.cluster.RediscoveryImpl;
import org.neo4j.bolt.connection.routed.impl.cluster.RoutingTableHandler;
import org.neo4j.bolt.connection.routed.impl.cluster.RoutingTableRegistry;
import org.neo4j.bolt.connection.routed.impl.cluster.RoutingTableRegistryImpl;
import org.neo4j.bolt.connection.routed.impl.cluster.loadbalancing.LeastConnectedLoadBalancingStrategy;
import org.neo4j.bolt.connection.routed.impl.cluster.loadbalancing.LoadBalancingStrategy;
import org.neo4j.bolt.connection.routed.impl.util.FutureUtil;

public class RoutedBoltConnectionProvider implements BoltConnectionProvider {
    private static final String CONNECTION_ACQUISITION_COMPLETION_FAILURE_MESSAGE =
            "Connection acquisition failed for all available addresses.";
    private static final String CONNECTION_ACQUISITION_COMPLETION_EXCEPTION_MESSAGE =
            "Failed to obtain connection towards %s server. Known routing table is: %s";
    private static final String CONNECTION_ACQUISITION_ATTEMPT_FAILURE_MESSAGE =
            "Failed to obtain a connection towards address %s, will try other addresses if available. Complete failure is reported separately from this entry.";
    private final System.Logger log;
    private final Function<BoltServerAddress, BoltConnectionProvider> boltConnectionProviderFunction;
    private final Map<BoltServerAddress, BoltConnectionProvider> addressToProvider = new HashMap<>();
    private final Map<BoltServerAddress, Integer> addressToInUseCount = new HashMap<>();
    private final LoadBalancingStrategy loadBalancingStrategy;
    private final RoutingTableRegistry registry;
    private final RoutingContext routingContext;
    private final BoltAgent boltAgent;
    private final String userAgent;
    private final int connectTimeoutMillis;

    private Rediscovery rediscovery;
    private CompletableFuture<Void> closeFuture;

    public RoutedBoltConnectionProvider(
            Function<BoltServerAddress, BoltConnectionProvider> boltConnectionProviderFunction,
            Function<BoltServerAddress, Set<BoltServerAddress>> resolver,
            DomainNameResolver domainNameResolver,
            long routingTablePurgeDelayMs,
            Rediscovery rediscovery,
            Clock clock,
            LoggingProvider logging,
            BoltServerAddress address,
            RoutingContext routingContext,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            MetricsListener metricsListener) {
        this.boltConnectionProviderFunction = Objects.requireNonNull(boltConnectionProviderFunction);
        this.log = logging.getLog(getClass());
        this.loadBalancingStrategy = new LeastConnectedLoadBalancingStrategy(this::getInUseCount, logging);
        this.rediscovery = rediscovery;
        this.routingContext = routingContext;
        this.boltAgent = boltAgent;
        this.userAgent = userAgent;
        this.connectTimeoutMillis = connectTimeoutMillis;
        if (this.rediscovery == null) {
            this.rediscovery = new RediscoveryImpl(
                    address,
                    resolver,
                    logging,
                    domainNameResolver,
                    routingContext,
                    boltAgent,
                    userAgent,
                    connectTimeoutMillis);
        }
        this.registry = new RoutingTableRegistryImpl(
                this::get, this.rediscovery, clock, logging, routingTablePurgeDelayMs, this::shutdownUnusedProviders);
    }

    @Override
    public CompletionStage<BoltConnection> connect(
            BoltServerAddress ignoredAddress,
            RoutingContext ignoredRoutingContext,
            BoltAgent ignoredBoltAgent,
            String ignoredUserAgent,
            int ignoredConnectTimeoutMillis,
            SecurityPlan securityPlan,
            DatabaseName databaseName,
            Supplier<CompletionStage<AuthToken>> authTokenStageSupplier,
            AccessMode mode,
            Set<String> bookmarks,
            String impersonatedUser,
            BoltProtocolVersion minVersion,
            NotificationConfig notificationConfig,
            Consumer<DatabaseName> databaseNameConsumer,
            Map<String, Object> additionalParameters) {
        RoutingTableRegistry registry;
        var homeDatabaseHintObj = additionalParameters.get("homeDatabase");
        var homeDatabaseHint = homeDatabaseHintObj instanceof String ? (String) homeDatabaseHintObj : null;
        synchronized (this) {
            if (closeFuture != null) {
                return CompletableFuture.failedFuture(new IllegalStateException("Connection provider is closed."));
            }
            registry = this.registry;
        }

        Supplier<CompletionStage<AuthToken>> supplier =
                () -> authTokenStageSupplier.get().exceptionally(throwable -> {
                    throw new AuthTokenManagerExecutionException(throwable);
                });

        var handlerRef = new AtomicReference<RoutingTableHandler>();
        var databaseNameFuture = databaseName == null
                ? new CompletableFuture<DatabaseName>()
                : CompletableFuture.completedFuture(databaseName);
        databaseNameFuture.whenComplete((name, throwable) -> {
            if (name != null) {
                databaseNameConsumer.accept(name);
            }
        });
        return registry.ensureRoutingTable(
                        securityPlan,
                        databaseNameFuture,
                        mode,
                        bookmarks,
                        impersonatedUser,
                        supplier,
                        minVersion,
                        homeDatabaseHint)
                .thenApply(routingTableHandler -> {
                    handlerRef.set(routingTableHandler);
                    return routingTableHandler;
                })
                .thenCompose(routingTableHandler -> acquire(
                        securityPlan,
                        mode,
                        routingTableHandler.routingTable(),
                        supplier,
                        routingTableHandler.routingTable().database(),
                        Set.of(),
                        impersonatedUser,
                        minVersion,
                        notificationConfig))
                .thenApply(boltConnection ->
                        (BoltConnection) new RoutedBoltConnection(boltConnection, handlerRef.get(), mode, this))
                .exceptionally(throwable -> {
                    throwable = FutureUtil.completionExceptionCause(throwable);
                    if (throwable instanceof AuthTokenManagerExecutionException) {
                        throwable = throwable.getCause();
                    }
                    if (throwable instanceof RuntimeException runtimeException) {
                        throw runtimeException;
                    } else {
                        throw new CompletionException(throwable);
                    }
                });
    }

    @Override
    public CompletionStage<Void> verifyConnectivity(
            BoltServerAddress ignoredAddress,
            RoutingContext ignoredRoutingContext,
            BoltAgent ignoredBoltAgent,
            String ignoredUserAgent,
            int ignoredConnectTimeoutMillis,
            SecurityPlan securityPlan,
            AuthToken authToken) {
        RoutingTableRegistry registry;
        synchronized (this) {
            registry = this.registry;
        }
        return supportsMultiDb(null, null, null, null, 0, securityPlan, authToken)
                .thenCompose(supports -> registry.ensureRoutingTable(
                        securityPlan,
                        supports
                                ? CompletableFuture.completedFuture(DatabaseNameUtil.database("system"))
                                : CompletableFuture.completedFuture(DatabaseNameUtil.defaultDatabase()),
                        AccessMode.READ,
                        Collections.emptySet(),
                        null,
                        () -> CompletableFuture.completedStage(authToken),
                        null,
                        null))
                .handle((ignored, error) -> {
                    if (error != null) {
                        var cause = FutureUtil.completionExceptionCause(error);
                        if (cause instanceof BoltServiceUnavailableException) {
                            throw FutureUtil.asCompletionException(new BoltServiceUnavailableException(
                                    "Unable to connect to database management service, ensure the database is running and that there is a working network connection to it.",
                                    cause));
                        }
                        throw FutureUtil.asCompletionException(cause);
                    }
                    return null;
                });
    }

    @Override
    public CompletionStage<Boolean> supportsMultiDb(
            BoltServerAddress ignoredAddress,
            RoutingContext ignoredRoutingContext,
            BoltAgent ignoredBoltAgent,
            String ignoredUserAgent,
            int ignoredConnectTimeoutMillis,
            SecurityPlan securityPlan,
            AuthToken authToken) {
        return detectFeature(
                securityPlan,
                authToken,
                "Failed to perform multi-databases feature detection with the following servers: ",
                (boltConnection -> boltConnection.protocolVersion().compareTo(new BoltProtocolVersion(4, 0)) >= 0));
    }

    @Override
    public CompletionStage<Boolean> supportsSessionAuth(
            BoltServerAddress ignoredAddress,
            RoutingContext ignoredRoutingContext,
            BoltAgent ignoredBoltAgent,
            String ignoredUserAgent,
            int ignoredConnectTimeoutMillis,
            SecurityPlan securityPlan,
            AuthToken authToken) {
        return detectFeature(
                securityPlan,
                authToken,
                "Failed to perform session auth feature detection with the following servers: ",
                (boltConnection -> new BoltProtocolVersion(5, 1).compareTo(boltConnection.protocolVersion()) <= 0));
    }

    private synchronized void shutdownUnusedProviders(Set<BoltServerAddress> addressesToRetain) {
        var iterator = addressToProvider.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            var address = entry.getKey();
            if (!addressesToRetain.contains(address) && getInUseCount(address) == 0) {
                entry.getValue().close();
                iterator.remove();
            }
        }
    }

    private CompletionStage<Boolean> detectFeature(
            SecurityPlan securityPlan,
            AuthToken authToken,
            String baseErrorMessagePrefix,
            Function<BoltConnection, Boolean> featureDetectionFunction) {
        Rediscovery rediscovery;
        synchronized (this) {
            rediscovery = this.rediscovery;
        }

        List<BoltServerAddress> addresses;
        try {
            addresses = rediscovery.resolve();
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        CompletableFuture<Boolean> result = CompletableFuture.completedFuture(null);
        Throwable baseError = new BoltServiceUnavailableException(baseErrorMessagePrefix + addresses);

        Function<BoltFailureException, Boolean> isSecurityException =
                boltFailureException -> boltFailureException.code().startsWith("Neo.ClientError.Security.");

        for (var address : addresses) {
            result = FutureUtil.onErrorContinue(result, baseError, completionError -> {
                // We fail fast on security errors
                var error = FutureUtil.completionExceptionCause(completionError);
                if (error instanceof BoltFailureException boltFailureException) {
                    if (isSecurityException.apply(boltFailureException)) {
                        return CompletableFuture.failedFuture(error);
                    }
                } else if (error instanceof SSLHandshakeException) {
                    return CompletableFuture.failedFuture(error);
                }
                return get(address)
                        .connect(
                                address,
                                routingContext,
                                boltAgent,
                                userAgent,
                                connectTimeoutMillis,
                                securityPlan,
                                null,
                                () -> CompletableFuture.completedStage(authToken),
                                AccessMode.WRITE,
                                Collections.emptySet(),
                                null,
                                null,
                                null,
                                (ignored) -> {},
                                Collections.emptyMap())
                        .thenCompose(boltConnection -> {
                            var featureDetected = featureDetectionFunction.apply(boltConnection);
                            return boltConnection.close().thenApply(ignored -> featureDetected);
                        });
            });
        }
        return FutureUtil.onErrorContinue(result, baseError, completionError -> {
            // If we failed with security errors, then we rethrow the security error out, otherwise we throw the chained
            // errors.
            var error = FutureUtil.completionExceptionCause(completionError);
            if (error instanceof BoltFailureException boltFailureException) {
                if (isSecurityException.apply(boltFailureException)) {
                    return CompletableFuture.failedFuture(error);
                }
            } else if (error instanceof SSLHandshakeException) {
                return CompletableFuture.failedFuture(error);
            }
            return CompletableFuture.failedFuture(baseError);
        });
    }

    private CompletionStage<BoltConnection> acquire(
            SecurityPlan securityPlan,
            AccessMode mode,
            RoutingTable routingTable,
            Supplier<CompletionStage<AuthToken>> authTokenStageSupplier,
            DatabaseName database,
            Set<String> bookmarks,
            String impersonatedUser,
            BoltProtocolVersion minVersion,
            NotificationConfig notificationConfig) {
        var result = new CompletableFuture<BoltConnection>();
        List<Throwable> attemptExceptions = new ArrayList<>();
        acquire(
                securityPlan,
                mode,
                routingTable,
                result,
                authTokenStageSupplier,
                attemptExceptions,
                database,
                bookmarks,
                impersonatedUser,
                minVersion,
                notificationConfig);
        return result;
    }

    private void acquire(
            SecurityPlan securityPlan,
            AccessMode mode,
            RoutingTable routingTable,
            CompletableFuture<BoltConnection> result,
            Supplier<CompletionStage<AuthToken>> authTokenStageSupplier,
            List<Throwable> attemptErrors,
            DatabaseName database,
            Set<String> bookmarks,
            String impersonatedUser,
            BoltProtocolVersion minVersion,
            NotificationConfig notificationConfig) {
        var addresses = getAddressesByMode(mode, routingTable);
        log.log(System.Logger.Level.DEBUG, "Addresses: " + addresses);
        var address = selectAddress(mode, addresses);
        log.log(System.Logger.Level.DEBUG, "Selected address: " + address);

        if (address == null) {
            var completionError = new BoltConnectionAcquisitionException(
                    format(CONNECTION_ACQUISITION_COMPLETION_EXCEPTION_MESSAGE, mode, routingTable));
            attemptErrors.forEach(completionError::addSuppressed);
            log.log(System.Logger.Level.ERROR, CONNECTION_ACQUISITION_COMPLETION_FAILURE_MESSAGE, completionError);
            result.completeExceptionally(completionError);
            return;
        }

        get(address)
                .connect(
                        address,
                        routingContext,
                        boltAgent,
                        userAgent,
                        connectTimeoutMillis,
                        securityPlan,
                        database,
                        authTokenStageSupplier,
                        mode,
                        bookmarks,
                        impersonatedUser,
                        minVersion,
                        notificationConfig,
                        (ignored) -> {},
                        Collections.emptyMap())
                .whenComplete((connection, completionError) -> {
                    var error = FutureUtil.completionExceptionCause(completionError);
                    if (error != null) {
                        if (error instanceof BoltServiceUnavailableException) {
                            var attemptMessage = format(CONNECTION_ACQUISITION_ATTEMPT_FAILURE_MESSAGE, address);
                            log.log(System.Logger.Level.WARNING, attemptMessage);
                            log.log(System.Logger.Level.DEBUG, attemptMessage, error);
                            attemptErrors.add(error);
                            routingTable.forget(address);
                            CompletableFuture.runAsync(() -> acquire(
                                    securityPlan,
                                    mode,
                                    routingTable,
                                    result,
                                    authTokenStageSupplier,
                                    attemptErrors,
                                    database,
                                    bookmarks,
                                    impersonatedUser,
                                    minVersion,
                                    notificationConfig));
                        } else {
                            result.completeExceptionally(error);
                        }
                    } else {
                        incrementInUseCount(address);
                        result.complete(connection);
                    }
                });
    }

    private BoltServerAddress selectAddress(AccessMode mode, List<BoltServerAddress> addresses) {
        return switch (mode) {
            case READ -> loadBalancingStrategy.selectReader(addresses);
            case WRITE -> loadBalancingStrategy.selectWriter(addresses);
        };
    }

    private static List<BoltServerAddress> getAddressesByMode(AccessMode mode, RoutingTable routingTable) {
        return switch (mode) {
            case READ -> routingTable.readers();
            case WRITE -> routingTable.writers();
        };
    }

    private synchronized int getInUseCount(BoltServerAddress address) {
        return addressToInUseCount.getOrDefault(address, 0);
    }

    private synchronized void incrementInUseCount(BoltServerAddress address) {
        addressToInUseCount.merge(address, 1, Integer::sum);
    }

    public synchronized void decrementInUseCount(BoltServerAddress address) {
        addressToInUseCount.compute(address, (ignored, value) -> {
            if (value == null) {
                return null;
            } else {
                value--;
                return value > 0 ? value : null;
            }
        });
    }

    @Override
    public CompletionStage<Void> close() {
        CompletableFuture<Void> closeFuture;
        synchronized (this) {
            if (this.closeFuture == null) {
                @SuppressWarnings({"rawtypes", "RedundantSuppression"})
                var futures = new CompletableFuture[addressToProvider.size()];
                var iterator = addressToProvider.values().iterator();
                var index = 0;
                while (iterator.hasNext()) {
                    futures[index++] = iterator.next().close().toCompletableFuture();
                    iterator.remove();
                }
                this.closeFuture = CompletableFuture.allOf(futures);
            }
            closeFuture = this.closeFuture;
        }
        return closeFuture;
    }

    private synchronized BoltConnectionProvider get(BoltServerAddress address) {
        var provider = addressToProvider.get(address);
        if (provider == null) {
            provider = boltConnectionProviderFunction.apply(address);
            addressToProvider.put(address, provider);
        }
        return provider;
    }
}
