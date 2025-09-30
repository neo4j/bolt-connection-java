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
import static org.neo4j.bolt.connection.routed.impl.util.LockUtil.executeWithLock;

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
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import javax.net.ssl.SSLHandshakeException;
import org.neo4j.bolt.connection.AccessMode;
import org.neo4j.bolt.connection.BoltConnection;
import org.neo4j.bolt.connection.BoltConnectionParameters;
import org.neo4j.bolt.connection.BoltConnectionSource;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.DatabaseName;
import org.neo4j.bolt.connection.DomainNameResolver;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.RoutedBoltConnectionParameters;
import org.neo4j.bolt.connection.exception.BoltClientException;
import org.neo4j.bolt.connection.exception.BoltConnectionAcquisitionException;
import org.neo4j.bolt.connection.exception.BoltFailureException;
import org.neo4j.bolt.connection.exception.BoltServiceUnavailableException;
import org.neo4j.bolt.connection.observation.ImmutableObservation;
import org.neo4j.bolt.connection.observation.ObservationProvider;
import org.neo4j.bolt.connection.routed.impl.RoutedBoltConnection;
import org.neo4j.bolt.connection.routed.impl.cluster.RediscoveryImpl;
import org.neo4j.bolt.connection.routed.impl.cluster.RoutingTableHandler;
import org.neo4j.bolt.connection.routed.impl.cluster.RoutingTableRegistry;
import org.neo4j.bolt.connection.routed.impl.cluster.RoutingTableRegistryImpl;
import org.neo4j.bolt.connection.routed.impl.cluster.loadbalancing.LeastConnectedLoadBalancingStrategy;
import org.neo4j.bolt.connection.routed.impl.cluster.loadbalancing.LoadBalancingStrategy;
import org.neo4j.bolt.connection.routed.impl.util.FutureUtil;
import org.neo4j.bolt.connection.routed.impl.util.LockUtil;

/**
 * A routed {@link BoltConnectionSource} implementation that implements Neo4j client-side routing that is typically
 * expected from the {@code neo4j} URI schemes.
 *
 * @since 4.0.0
 */
public class RoutedBoltConnectionSource implements BoltConnectionSource<RoutedBoltConnectionParameters> {
    private static final String CONNECTION_ACQUISITION_COMPLETION_FAILURE_MESSAGE =
            "Connection acquisition failed for all available addresses.";
    private static final String CONNECTION_ACQUISITION_COMPLETION_EXCEPTION_MESSAGE =
            "Failed to obtain connection towards %s server. Known routing table is: %s";
    private static final String CONNECTION_ACQUISITION_ATTEMPT_FAILURE_MESSAGE =
            "Failed to obtain a connection towards address %s, will try other addresses if available. Complete failure is reported separately from this entry.";
    private final System.Logger log;
    private final Lock lock = new ReentrantLock();
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private final BoltConnectionSourceFactory boltConnectionSourceFactory;
    private final URI uri;
    private final long acquisitionTimeout;
    private final Map<BoltServerAddress, BoltConnectionSource<BoltConnectionParameters>> addressToSource =
            new HashMap<>();
    private final Map<BoltServerAddress, Integer> addressToInUseCount = new HashMap<>();
    private final LoadBalancingStrategy loadBalancingStrategy;
    private final RoutingTableRegistry registry;
    private final Rediscovery rediscovery;
    private final ObservationProvider observationProvider;

    private CompletableFuture<Void> closeFuture;

    public RoutedBoltConnectionSource(
            BoltConnectionSourceFactory boltConnectionSourceFactory,
            Function<BoltServerAddress, Set<BoltServerAddress>> resolver,
            DomainNameResolver domainNameResolver,
            long routingTablePurgeDelayMs,
            Rediscovery rediscovery,
            Clock clock,
            LoggingProvider logging,
            URI uri,
            long acquisitionTimeout,
            List<Class<? extends Throwable>> discoveryAbortingErrors,
            ObservationProvider observationProvider) {
        this.boltConnectionSourceFactory = Objects.requireNonNull(boltConnectionSourceFactory);
        this.log = logging.getLog(getClass());
        this.loadBalancingStrategy = new LeastConnectedLoadBalancingStrategy(this::getInUseCount, logging);
        this.rediscovery = rediscovery != null
                ? rediscovery
                : new RediscoveryImpl(
                        new BoltServerAddress(uri), resolver, logging, domainNameResolver, discoveryAbortingErrors);
        this.registry = new RoutingTableRegistryImpl(
                this::get, this.rediscovery, clock, logging, routingTablePurgeDelayMs, this::shutdownUnusedProviders);
        this.uri = Objects.requireNonNull(uri);
        this.acquisitionTimeout = acquisitionTimeout;
        this.observationProvider = Objects.requireNonNull(observationProvider);
    }

    @Override
    public CompletionStage<BoltConnection> getConnection() {
        return getConnection(RoutedBoltConnectionParameters.defaultParameters());
    }

    @Override
    public CompletionStage<BoltConnection> getConnection(RoutedBoltConnectionParameters parameters) {
        RoutingTableRegistry registry;
        lock.lock();
        try {
            if (closeFuture != null) {
                return CompletableFuture.failedFuture(new IllegalStateException("Connection provider is closed."));
            }
            registry = this.registry;
        } finally {
            lock.unlock();
        }

        var parentObservation = observationProvider.scopedObservation();
        var acquisitionFuture = new CompletableFuture<BoltConnection>();
        var timeoutFuture = acquisitionTimeout > 0 ? scheduleTimeout(acquisitionFuture, acquisitionTimeout) : null;
        var handlerRef = new AtomicReference<RoutingTableHandler>();
        var databaseName = parameters.databaseName();
        CompletableFuture<DatabaseName> databaseNameFuture;
        if (databaseName == null) {
            databaseNameFuture = new CompletableFuture<>();
            databaseNameFuture.whenComplete((name, throwable) -> {
                if (name != null) {
                    parameters.databaseNameListener().accept(name);
                }
            });
        } else {
            databaseNameFuture = CompletableFuture.completedFuture(databaseName);
        }
        registry.ensureRoutingTable(databaseNameFuture, parameters, parentObservation)
                .thenApply(routingTableHandler -> {
                    handlerRef.set(routingTableHandler);
                    return routingTableHandler;
                })
                .thenCompose(routingTableHandler -> {
                    if (acquisitionTimedOut(timeoutFuture)) {
                        return CompletableFuture.failedFuture(acquisitionTimeoutException());
                    }
                    return acquire(
                            parameters.accessMode(), routingTableHandler.routingTable(), parameters, parentObservation);
                })
                .thenApply(boltConnection -> (BoltConnection)
                        new RoutedBoltConnection(boltConnection, handlerRef.get(), parameters.accessMode(), this))
                .thenCompose(boltConnection -> {
                    if (parameters.homeDatabaseHint() != null
                            && !boltConnection.serverSideRoutingEnabled()
                            && !databaseNameFuture.isDone()) {
                        // home database was requested with hint, but the returned connection does not have SSR enabled
                        if (acquisitionTimedOut(timeoutFuture)) {
                            return CompletableFuture.failedFuture(acquisitionTimeoutException());
                        }
                        var parametersWithoutHomeDatabaseHint = RoutedBoltConnectionParameters.builder()
                                .withAuthToken(parameters.authToken())
                                .withMinVersion(parameters.minVersion())
                                .withAccessMode(parameters.accessMode())
                                .withDatabaseName(parameters.databaseName())
                                .withDatabaseNameListener(parameters.databaseNameListener())
                                .withHomeDatabaseHint(null)
                                .withBookmarks(parameters.bookmarks())
                                .withImpersonatedUser(parameters.impersonatedUser())
                                .build();
                        return boltConnection
                                .close()
                                .thenCompose(ignored -> getConnection(parametersWithoutHomeDatabaseHint));
                    } else {
                        return CompletableFuture.completedStage(boltConnection);
                    }
                })
                .whenComplete((connection, throwable) -> {
                    if (throwable != null) {
                        throwable = FutureUtil.completionExceptionCause(throwable);
                        acquisitionFuture.completeExceptionally(throwable);
                        connection.close();
                    } else {
                        if (!acquisitionFuture.complete(connection)) {
                            connection.close();
                        }
                    }
                });
        return acquisitionFuture;
    }

    @Override
    public CompletionStage<Void> verifyConnectivity() {
        RoutingTableRegistry registry;
        lock.lock();
        try {
            if (closeFuture != null) {
                return CompletableFuture.failedFuture(new IllegalStateException("Connection provider is closed."));
            }
            registry = this.registry;
        } finally {
            lock.unlock();
        }
        var observation = observationProvider.scopedObservation();
        return supportsMultiDb()
                .thenCompose(supports -> registry.ensureRoutingTable(
                        supports
                                ? CompletableFuture.completedFuture(DatabaseName.systemDatabase())
                                : CompletableFuture.completedFuture(DatabaseName.defaultDatabase()),
                        RoutedBoltConnectionParameters.builder()
                                .withAccessMode(AccessMode.READ)
                                .build(),
                        observation))
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

    private void shutdownUnusedProviders(Set<BoltServerAddress> addressesToRetain) {
        executeWithLock(lock, () -> {
            var iterator = addressToSource.entrySet().iterator();
            while (iterator.hasNext()) {
                var entry = iterator.next();
                var address = entry.getKey();
                if (!addressesToRetain.contains(address) && getInUseCount(address) == 0) {
                    entry.getValue().close();
                    iterator.remove();
                }
            }
        });
    }

    private CompletionStage<Boolean> detectFeature(
            String baseErrorMessagePrefix, Function<BoltConnection, Boolean> featureDetectionFunction) {
        Rediscovery rediscovery;
        lock.lock();
        try {
            if (closeFuture != null) {
                return CompletableFuture.failedFuture(new IllegalStateException("Connection provider is closed."));
            }
            rediscovery = this.rediscovery;
        } finally {
            lock.unlock();
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
            AccessMode mode,
            RoutingTable routingTable,
            BoltConnectionParameters parameters,
            ImmutableObservation observation) {
        var result = new CompletableFuture<BoltConnection>();
        List<Throwable> attemptExceptions = new ArrayList<>();
        acquire(mode, routingTable, result, attemptExceptions, parameters, observation);
        return result;
    }

    private void acquire(
            AccessMode mode,
            RoutingTable routingTable,
            CompletableFuture<BoltConnection> result,
            List<Throwable> attemptErrors,
            BoltConnectionParameters parameters,
            ImmutableObservation observation) {
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

        var source = get(address);
        observationProvider
                .supplyInScope(observation, () -> source.getConnection(parameters))
                .whenComplete((connection, completionError) -> {
                    var error = FutureUtil.completionExceptionCause(completionError);
                    if (error != null) {
                        if (error instanceof BoltServiceUnavailableException) {
                            var attemptMessage = format(CONNECTION_ACQUISITION_ATTEMPT_FAILURE_MESSAGE, address);
                            log.log(System.Logger.Level.WARNING, attemptMessage);
                            log.log(System.Logger.Level.DEBUG, attemptMessage, error);
                            attemptErrors.add(error);
                            routingTable.forget(address);
                            CompletableFuture.runAsync(
                                    () -> acquire(mode, routingTable, result, attemptErrors, parameters, observation));
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

    private int getInUseCount(BoltServerAddress address) {
        return executeWithLock(lock, () -> addressToInUseCount.getOrDefault(address, 0));
    }

    private void incrementInUseCount(BoltServerAddress address) {
        executeWithLock(lock, () -> addressToInUseCount.merge(address, 1, Integer::sum));
    }

    public void decrementInUseCount(BoltServerAddress address) {
        executeWithLock(
                lock,
                () -> addressToInUseCount.compute(address, (ignored, value) -> {
                    if (value == null) {
                        return null;
                    } else {
                        value--;
                        return value > 0 ? value : null;
                    }
                }));
    }

    @Override
    public CompletionStage<Void> close() {
        CompletableFuture<Void> closeFuture;
        lock.lock();
        try {
            if (this.closeFuture == null) {
                @SuppressWarnings({"rawtypes", "RedundantSuppression"})
                var futures = new CompletableFuture[addressToSource.size()];
                var iterator = addressToSource.values().iterator();
                var index = 0;
                while (iterator.hasNext()) {
                    futures[index++] = iterator.next().close().toCompletableFuture();
                    iterator.remove();
                }
                this.closeFuture = CompletableFuture.allOf(futures).whenComplete((ignored, throwable) -> executorService
                        .shutdownNow()
                        .forEach(runnable -> {
                            try {
                                runnable.run();
                            } catch (Exception ignoredException) {
                            }
                        }));
            }
            closeFuture = this.closeFuture;
        } finally {
            lock.unlock();
        }
        return closeFuture;
    }

    private BoltConnectionSource<BoltConnectionParameters> get(BoltServerAddress address) {
        return executeWithLock(lock, () -> {
            var provider = addressToSource.get(address);
            if (provider == null) {
                URI uri;
                try {
                    uri = new URI(
                            this.uri.getScheme(),
                            null,
                            address.connectionHost(),
                            address.port(),
                            null,
                            this.uri.getQuery(),
                            null);
                } catch (URISyntaxException e) {
                    throw new BoltClientException("Failed to create URI for address: " + address, e);
                }
                String expectedHostname = null;
                if (!address.host().equals(address.connectionHost())) {
                    expectedHostname = address.host();
                }
                provider = boltConnectionSourceFactory.create(uri, expectedHostname);
                addressToSource.put(address, provider);
            }
            return provider;
        });
    }

    private ScheduledFuture<?> scheduleTimeout(
            CompletableFuture<BoltConnection> acquisitionFuture, long acquisitionTimeout) {
        return executorService.schedule(
                () -> {
                    boolean closeInitiated = LockUtil.executeWithLock(lock, () -> closeFuture != null);
                    acquisitionFuture.completeExceptionally(
                            closeInitiated
                                    ? new BoltClientException("The connection source is closing or is already closed")
                                    : acquisitionTimeoutException());
                },
                acquisitionTimeout,
                TimeUnit.MILLISECONDS);
    }

    private TimeoutException acquisitionTimeoutException() {
        return new TimeoutException("Unable to acquire connection from the pool within configured maximum time of "
                + acquisitionTimeout + "ms");
    }

    private boolean acquisitionTimedOut(ScheduledFuture<?> timeoutFuture) {
        return timeoutFuture != null && timeoutFuture.getDelay(TimeUnit.MILLISECONDS) <= 0L;
    }
}
