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

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.net.ssl.SSLHandshakeException;
import org.neo4j.bolt.connection.AccessMode;
import org.neo4j.bolt.connection.BoltConnection;
import org.neo4j.bolt.connection.BoltConnectionParameters;
import org.neo4j.bolt.connection.BoltConnectionSource;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.DatabaseName;
import org.neo4j.bolt.connection.DatabaseNameUtil;
import org.neo4j.bolt.connection.DomainNameResolver;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.RoutedBoltConnectionParameters;
import org.neo4j.bolt.connection.exception.BoltClientException;
import org.neo4j.bolt.connection.exception.BoltConnectionAcquisitionException;
import org.neo4j.bolt.connection.exception.BoltFailureException;
import org.neo4j.bolt.connection.exception.BoltServiceUnavailableException;
import org.neo4j.bolt.connection.routed.impl.RoutedBoltConnection;
import org.neo4j.bolt.connection.routed.impl.cluster.RediscoveryImpl;
import org.neo4j.bolt.connection.routed.impl.cluster.RoutingTableHandler;
import org.neo4j.bolt.connection.routed.impl.cluster.RoutingTableRegistry;
import org.neo4j.bolt.connection.routed.impl.cluster.RoutingTableRegistryImpl;
import org.neo4j.bolt.connection.routed.impl.cluster.loadbalancing.LeastConnectedLoadBalancingStrategy;
import org.neo4j.bolt.connection.routed.impl.cluster.loadbalancing.LoadBalancingStrategy;
import org.neo4j.bolt.connection.routed.impl.util.FutureUtil;

public class RoutedBoltConnectionSource implements BoltConnectionSource<RoutedBoltConnectionParameters> {
    private static final String CONNECTION_ACQUISITION_COMPLETION_FAILURE_MESSAGE =
            "Connection acquisition failed for all available addresses.";
    private static final String CONNECTION_ACQUISITION_COMPLETION_EXCEPTION_MESSAGE =
            "Failed to obtain connection towards %s server. Known routing table is: %s";
    private static final String CONNECTION_ACQUISITION_ATTEMPT_FAILURE_MESSAGE =
            "Failed to obtain a connection towards address %s, will try other addresses if available. Complete failure is reported separately from this entry.";
    private final System.Logger log;
    private final BoltConnectionSourceFactory boltConnectionSourceFactory;
    private final String providerScheme;
    private final Map<BoltServerAddress, BoltConnectionSource<BoltConnectionParameters>> addressToProvider =
            new HashMap<>();
    private final Map<BoltServerAddress, Integer> addressToInUseCount = new HashMap<>();
    private final LoadBalancingStrategy loadBalancingStrategy;
    private final RoutingTableRegistry registry;
    private final Rediscovery rediscovery;

    private CompletableFuture<Void> closeFuture;

    public RoutedBoltConnectionSource(
            BoltConnectionSourceFactory boltConnectionSourceFactory,
            Function<BoltServerAddress, Set<BoltServerAddress>> resolver,
            DomainNameResolver domainNameResolver,
            long routingTablePurgeDelayMs,
            Rediscovery rediscovery,
            Clock clock,
            LoggingProvider logging,
            URI uri) {
        this.boltConnectionSourceFactory = Objects.requireNonNull(boltConnectionSourceFactory);
        this.log = logging.getLog(getClass());
        this.loadBalancingStrategy = new LeastConnectedLoadBalancingStrategy(this::getInUseCount, logging);
        this.rediscovery = rediscovery != null
                ? rediscovery
                : new RediscoveryImpl(new BoltServerAddress(uri), resolver, logging, domainNameResolver);
        this.registry = new RoutingTableRegistryImpl(
                this::get, this.rediscovery, clock, logging, routingTablePurgeDelayMs, this::shutdownUnusedProviders);
        this.providerScheme = switch (uri.getScheme()) {
            case "neo4j" -> "bolt";
            case "neo4j+ssc" -> "bolt+ssc";
            case "neo4j+s" -> "bolt+s";
            default -> throw new IllegalArgumentException("Unsupported URI scheme: " + uri.getScheme());};
    }

    private static <T> T getConfigEntry(Map<String, ?> config, String key, Class<T> type, Supplier<T> defaultValue) {
        var value = config.get(key);
        return (value != null && type.isAssignableFrom(value.getClass())) ? type.cast(value) : defaultValue.get();
    }

    @Override
    public CompletionStage<BoltConnection> getConnection() {
        return getConnection(RoutedBoltConnectionParameters.defaultParameters());
    }

    @Override
    public CompletionStage<BoltConnection> getConnection(RoutedBoltConnectionParameters parameters) {
        RoutingTableRegistry registry;
        synchronized (this) {
            if (closeFuture != null) {
                return CompletableFuture.failedFuture(new IllegalStateException("Connection provider is closed."));
            }
            registry = this.registry;
        }

        var handlerRef = new AtomicReference<RoutingTableHandler>();
        var databaseName = parameters.databaseName();
        var databaseNameFuture = databaseName == null
                ? new CompletableFuture<DatabaseName>()
                : CompletableFuture.completedFuture(databaseName);
        databaseNameFuture.whenComplete((name, throwable) -> {
            if (name != null) {
                parameters.databaseNameConsumer().accept(name);
            }
        });
        return registry.ensureRoutingTable(databaseNameFuture, parameters)
                .thenApply(routingTableHandler -> {
                    handlerRef.set(routingTableHandler);
                    return routingTableHandler;
                })
                .thenCompose(routingTableHandler ->
                        acquire(parameters.accessMode(), routingTableHandler.routingTable(), parameters))
                .thenApply(boltConnection -> (BoltConnection)
                        new RoutedBoltConnection(boltConnection, handlerRef.get(), parameters.accessMode(), this))
                .exceptionally(throwable -> {
                    throwable = FutureUtil.completionExceptionCause(throwable);
                    if (throwable instanceof RuntimeException runtimeException) {
                        throw runtimeException;
                    } else {
                        throw new CompletionException(throwable);
                    }
                });
    }

    @Override
    public CompletionStage<Void> verifyConnectivity() {
        RoutingTableRegistry registry;
        synchronized (this) {
            registry = this.registry;
        }
        return detectFeature(
                        "Failed to perform multi-databases feature detection with the following servers: ",
                        boltConnection ->
                                boltConnection.protocolVersion().compareTo(new BoltProtocolVersion(4, 0)) >= 0)
                .thenCompose(supports -> registry.ensureRoutingTable(
                        supports
                                ? CompletableFuture.completedFuture(DatabaseNameUtil.database("system"))
                                : CompletableFuture.completedFuture(DatabaseNameUtil.defaultDatabase()),
                        RoutedBoltConnectionParameters.builder()
                                .withAccessMode(AccessMode.READ)
                                .build()))
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
    public CompletionStage<Boolean> supportsMultiDb() {
        return detectFeature(
                "Failed to perform multi-databases feature detection with the following servers: ",
                (boltConnection -> boltConnection.protocolVersion().compareTo(new BoltProtocolVersion(4, 0)) >= 0));
    }

    @Override
    public CompletionStage<Boolean> supportsSessionAuth() {
        return detectFeature(
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
            String baseErrorMessagePrefix, Function<BoltConnection, Boolean> featureDetectionFunction) {
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
                return get(address).getConnection().thenCompose(boltConnection -> {
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
            AccessMode mode, RoutingTable routingTable, BoltConnectionParameters parameters) {
        var result = new CompletableFuture<BoltConnection>();
        List<Throwable> attemptExceptions = new ArrayList<>();
        acquire(mode, routingTable, result, attemptExceptions, parameters);
        return result;
    }

    private void acquire(
            AccessMode mode,
            RoutingTable routingTable,
            CompletableFuture<BoltConnection> result,
            List<Throwable> attemptErrors,
            BoltConnectionParameters parameters) {
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

        get(address).getConnection(parameters).whenComplete((connection, completionError) -> {
            var error = FutureUtil.completionExceptionCause(completionError);
            if (error != null) {
                if (error instanceof BoltServiceUnavailableException) {
                    var attemptMessage = format(CONNECTION_ACQUISITION_ATTEMPT_FAILURE_MESSAGE, address);
                    log.log(System.Logger.Level.WARNING, attemptMessage);
                    log.log(System.Logger.Level.DEBUG, attemptMessage, error);
                    attemptErrors.add(error);
                    routingTable.forget(address);
                    CompletableFuture.runAsync(() -> acquire(mode, routingTable, result, attemptErrors, parameters));
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

    private synchronized BoltConnectionSource<BoltConnectionParameters> get(BoltServerAddress address) {
        var provider = addressToProvider.get(address);
        if (provider == null) {
            URI uri;
            try {
                uri = new URI(providerScheme, null, address.connectionHost(), address.port(), null, null, null);
            } catch (URISyntaxException e) {
                throw new BoltClientException("Failed to create URI for address: " + address, e);
            }
            String hostnameForVerification = null;
            if (!address.host().equals(address.connectionHost())) {
                hostnameForVerification = address.host();
            }
            provider = boltConnectionSourceFactory.create(uri, hostnameForVerification);
            addressToProvider.put(address, provider);
        }
        return provider;
    }
}
