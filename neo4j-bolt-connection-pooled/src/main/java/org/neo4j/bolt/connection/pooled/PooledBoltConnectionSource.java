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
package org.neo4j.bolt.connection.pooled;

import java.net.URI;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.neo4j.bolt.connection.AuthToken;
import org.neo4j.bolt.connection.BasicResponseHandler;
import org.neo4j.bolt.connection.BoltAgent;
import org.neo4j.bolt.connection.BoltConnection;
import org.neo4j.bolt.connection.BoltConnectionParameters;
import org.neo4j.bolt.connection.BoltConnectionProvider;
import org.neo4j.bolt.connection.BoltConnectionSource;
import org.neo4j.bolt.connection.BoltConnectionState;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.NotificationConfig;
import org.neo4j.bolt.connection.SecurityPlan;
import org.neo4j.bolt.connection.exception.BoltConnectionInitialisationTimeoutException;
import org.neo4j.bolt.connection.exception.BoltFailureException;
import org.neo4j.bolt.connection.exception.BoltTransientException;
import org.neo4j.bolt.connection.exception.MinVersionAcquisitionException;
import org.neo4j.bolt.connection.message.Messages;
import org.neo4j.bolt.connection.observation.ImmutableObservation;
import org.neo4j.bolt.connection.pooled.impl.PooledBoltConnection;
import org.neo4j.bolt.connection.pooled.impl.util.FutureUtil;
import org.neo4j.bolt.connection.pooled.observation.PoolObservationProvider;

/**
 * A pooled {@link BoltConnectionSource} implementation that automatically pools {@link BoltConnection} instances.
 *
 * @since 4.0.0
 */
public class PooledBoltConnectionSource implements BoltConnectionSource<BoltConnectionParameters> {
    private final System.Logger log;
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private final BoltConnectionProvider boltConnectionProvider;
    private final List<ConnectionEntry> pooledConnectionEntries;
    private final Queue<CompletableFuture<PooledBoltConnection>> pendingAcquisitions;
    private final int maxSize;
    private final long acquisitionTimeout;
    private final long maxLifetime;
    private final long idleBeforeTest;
    private final Clock clock;
    private final PoolObservationProvider observationProvider;
    private final URI uri;
    private final BoltServerAddress address;
    private final String routingContextAddress;
    private final BoltAgent boltAgent;
    private final String userAgent;
    private final int connectTimeoutMillis;
    private final String poolId;
    private final AuthTokenManager authTokenManager;
    private final SecurityPlanSupplier securityPlanSupplier;
    private final NotificationConfig notificationConfig;
    private final TimeoutPolicy timeoutPolicy;

    private CompletionStage<Void> closeStage;
    private long minAuthTimestamp;

    public PooledBoltConnectionSource(
            LoggingProvider loggingProvider,
            Clock clock,
            URI uri,
            BoltConnectionProvider boltConnectionProvider,
            AuthTokenManager authTokenManager,
            SecurityPlanSupplier securityPlanSupplier,
            int maxSize,
            long acquisitionTimeout,
            long maxLifetime,
            long idleBeforeTest,
            PoolObservationProvider observationProvider,
            String routingContextAddress,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            NotificationConfig notificationConfig,
            TimeoutPolicy timeoutPolicy) {
        this.uri = Objects.requireNonNull(uri);
        this.address = switch (uri.getScheme()) {
            case "bolt", "bolt+s", "bolt+ssc", "neo4j", "neo4j+s", "neo4j+ssc" -> new BoltServerAddress(uri);
            case "bolt+unix" -> new BoltServerAddress(Path.of(uri.getPath()));
            default -> new BoltServerAddress(uri.getHost(), 0);};
        this.poolId = poolId(address);
        this.observationProvider = Objects.requireNonNull(observationProvider);
        this.maxSize = maxSize;
        var createObservation = observationProvider.connectionPoolCreate(poolId, uri, maxSize);
        try {
            this.boltConnectionProvider = Objects.requireNonNull(boltConnectionProvider);
            this.pooledConnectionEntries = new ArrayList<>();
            this.pendingAcquisitions = new ArrayDeque<>(100);
            this.acquisitionTimeout = acquisitionTimeout;
            this.maxLifetime = maxLifetime;
            this.idleBeforeTest = idleBeforeTest;
            this.clock = Objects.requireNonNull(clock);
            this.log = loggingProvider.getLog(getClass());
            this.routingContextAddress = routingContextAddress;
            this.boltAgent = Objects.requireNonNull(boltAgent);
            this.userAgent = Objects.requireNonNull(userAgent);
            this.connectTimeoutMillis = connectTimeoutMillis;
            this.authTokenManager = Objects.requireNonNull(authTokenManager);
            this.securityPlanSupplier = Objects.requireNonNull(securityPlanSupplier);
            this.notificationConfig = Objects.requireNonNull(notificationConfig);
            this.timeoutPolicy = Objects.requireNonNull(timeoutPolicy);
        } catch (RuntimeException ex) {
            createObservation.error(ex);
            throw ex;
        } finally {
            createObservation.stop();
        }
    }

    @Override
    public CompletionStage<BoltConnection> getConnection() {
        return getConnection(BoltConnectionParameters.defaultParameters());
    }

    @SuppressWarnings({"ReassignedVariable"})
    @Override
    public CompletionStage<BoltConnection> getConnection(BoltConnectionParameters parameters) {
        synchronized (this) {
            if (closeStage != null) {
                return CompletableFuture.failedFuture(new IllegalStateException("Connection source is closed."));
            }
        }

        var parentObservation = observationProvider.scopedObservation();
        var acquireObservation = observationProvider.pooledConnectionAcquire(poolId, uri);
        var acquisitionFuture = new CompletableFuture<PooledBoltConnection>();
        var timeoutFuture = acquisitionTimeout > 0 && timeoutPolicy.equals(TimeoutPolicy.DEFAULT)
                ? scheduleTimeout(acquisitionFuture, acquisitionTimeout)
                : null;
        CompletionStage<AuthToken> authTokenSupplier;
        boolean overrideAuthToken;
        if (parameters.authToken() != null) {
            authTokenSupplier = CompletableFuture.completedStage(parameters.authToken());
            overrideAuthToken = true;
        } else {
            authTokenSupplier = authTokenManager.getToken();
            overrideAuthToken = false;
        }
        authTokenSupplier.whenComplete((authToken, authThrowable) -> {
            if (authThrowable != null) {
                acquisitionFuture.completeExceptionally(authThrowable);
                return;
            }

            acquisitionFuture.whenComplete((connection, throwable) -> {
                throwable = FutureUtil.completionExceptionCause(throwable);
                if (throwable != null) {
                    acquireObservation.error(throwable);
                }
                acquireObservation.stop();
            });
            connect(
                    acquisitionFuture,
                    timeoutFuture,
                    authToken,
                    overrideAuthToken,
                    parameters.minVersion(),
                    notificationConfig,
                    acquireObservation);
        });

        return acquisitionFuture.thenApply(boltConnection -> {
            boltConnection.onUsageStart(parentObservation);
            return boltConnection;
        });
    }

    @SuppressWarnings({"DuplicatedCode", "ConstantValue"})
    private void connect(
            CompletableFuture<PooledBoltConnection> acquisitionFuture,
            ScheduledFuture<?> timeoutFuture,
            AuthToken authToken,
            boolean overrideAuthToken,
            BoltProtocolVersion minVersion,
            NotificationConfig notificationConfig,
            ImmutableObservation parentObservation) {

        ConnectionEntryWithMetadata connectionEntryWithMetadata = null;
        Throwable pendingAcquisitionsFull = null;
        var empty = new AtomicBoolean();
        synchronized (this) {
            try {
                empty.set(pooledConnectionEntries.isEmpty());
                try {
                    // go over existing entries first
                    connectionEntryWithMetadata = acquireExistingEntry(authToken, minVersion);
                } catch (MinVersionAcquisitionException e) {
                    acquisitionFuture.completeExceptionally(e);
                    return;
                }

                if (connectionEntryWithMetadata == null) {
                    // no entry found
                    if (pooledConnectionEntries.size() < maxSize) {
                        // space is available, reserve
                        var acquiredEntry = new ConnectionEntry();
                        pooledConnectionEntries.add(acquiredEntry);
                        connectionEntryWithMetadata = new ConnectionEntryWithMetadata(acquiredEntry, false);
                    } else {
                        // fallback to queue
                        if (pendingAcquisitions.size() < 100 && !acquisitionFuture.isDone()) {
                            switch (timeoutPolicy) {
                                case DEFAULT -> {
                                    if (timeoutFuture == null || timeoutFuture.getDelay(TimeUnit.MILLISECONDS) > 0) {
                                        pendingAcquisitions.add(acquisitionFuture);
                                    }
                                }
                                case LEGACY -> {
                                    if (acquisitionTimeout > 0) {
                                        pendingAcquisitions.add(acquisitionFuture);
                                        scheduleTimeout(acquisitionFuture, acquisitionTimeout);
                                    } else {
                                        executorService.execute(timeoutRunnable(acquisitionFuture));
                                    }
                                }
                            }
                        } else {
                            pendingAcquisitionsFull =
                                    new BoltTransientException("Connection pool pending acquisition queue is full.");
                        }
                    }
                }

            } catch (Throwable throwable) {
                if (connectionEntryWithMetadata != null) {
                    if (connectionEntryWithMetadata.connectionEntry.connection != null) {
                        // not new entry, make it available
                        connectionEntryWithMetadata.connectionEntry.available = true;
                    } else {
                        // new empty entry
                        pooledConnectionEntries.remove(connectionEntryWithMetadata.connectionEntry);
                    }
                }
                pendingAcquisitions.remove(acquisitionFuture);
                acquisitionFuture.completeExceptionally(throwable);
            }
        }

        if (pendingAcquisitionsFull != null) {
            // no space in queue was available
            acquisitionFuture.completeExceptionally(pendingAcquisitionsFull);
        } else if (connectionEntryWithMetadata != null) {
            if (connectionEntryWithMetadata.connectionEntry.connection != null) {
                // entry with connection
                var entryWithMetadata = connectionEntryWithMetadata;
                var entry = entryWithMetadata.connectionEntry;

                livenessCheckStage(entry, parentObservation).whenComplete((ignored, throwable) -> {
                    if (throwable != null) {
                        // liveness check failed
                        purge(entry);
                        connect(
                                acquisitionFuture,
                                timeoutFuture,
                                authToken,
                                overrideAuthToken,
                                minVersion,
                                notificationConfig,
                                parentObservation);
                    } else {
                        // liveness check green or not needed
                        var pooledConnection = new PooledBoltConnection(
                                entry.connection,
                                this,
                                () -> release(entry),
                                () -> purge(entry),
                                observationParent ->
                                        observationProvider.pooledConnectionInUse(observationParent, poolId, uri));
                        reauthStage(entryWithMetadata, authToken).whenComplete((ignored2, throwable2) -> {
                            if (!acquisitionFuture.complete(pooledConnection)) {
                                // acquisition timed out
                                CompletableFuture<PooledBoltConnection> pendingAcquisition;
                                synchronized (this) {
                                    pendingAcquisition = pendingAcquisitions.poll();
                                    if (pendingAcquisition == null) {
                                        // nothing pending, just make the entry available
                                        entry.available = true;
                                    }
                                }
                                if (pendingAcquisition != null) {
                                    pendingAcquisition.complete(pooledConnection);
                                }
                            }
                        });
                    }
                });
            } else {
                var createObservation = observationProvider.pooledConnectionCreate(poolId, uri);
                // get reserved entry
                var entry = connectionEntryWithMetadata.connectionEntry;
                var authStage = securityPlanSupplier.getPlan().thenCompose(securityPlan -> {
                    if (overrideAuthToken || empty.get()) {
                        return CompletableFuture.completedStage(new SecurityPlanAndAuthToken(securityPlan, authToken));
                    } else {
                        return authTokenManager
                                .getToken()
                                .thenApply(token -> new SecurityPlanAndAuthToken(securityPlan, token));
                    }
                });
                authStage
                        .thenCompose(auth -> boltConnectionProvider.connect(
                                uri,
                                routingContextAddress,
                                boltAgent,
                                userAgent,
                                connectTimeoutMillis,
                                switch (timeoutPolicy) {
                                    case DEFAULT -> acquisitionTimeout; // while the acquisition timeout is implemented
                                        // by this source, it is also used as initialisation timeout to make sure there
                                        // is
                                        // a limit
                                    case LEGACY -> connectTimeoutMillis;
                                },
                                auth.securityPlan(),
                                auth.authToken(),
                                minVersion,
                                notificationConfig,
                                createObservation))
                        .whenComplete((boltConnection, throwable) -> {
                            var error = FutureUtil.completionExceptionCause(throwable);
                            if (error != null) {
                                synchronized (this) {
                                    pooledConnectionEntries.remove(entry);
                                }
                                if (error instanceof BoltFailureException boltFailureException) {
                                    var usedAuth =
                                            authStage.toCompletableFuture().getNow(null);
                                    if (usedAuth != null) {
                                        error = authTokenManager.handleBoltFailureException(
                                                usedAuth.authToken(), boltFailureException);
                                    }
                                } else if (error instanceof BoltConnectionInitialisationTimeoutException) {
                                    error = switch (timeoutPolicy) {
                                        case DEFAULT -> timeoutException();
                                        case LEGACY -> error;};
                                }
                                createObservation.error(error);
                                createObservation.stop();
                                acquisitionFuture.completeExceptionally(error);
                            } else {
                                synchronized (this) {
                                    entry.connection = boltConnection;
                                    entry.createdTimestamp = clock.millis();
                                }
                                createObservation.stop();
                                var pooledConnection = new PooledBoltConnection(
                                        boltConnection,
                                        this,
                                        () -> release(entry),
                                        () -> purge(entry),
                                        observationParent -> observationProvider.pooledConnectionInUse(
                                                observationParent, poolId, uri));
                                if (!acquisitionFuture.complete(pooledConnection)) {
                                    // acquisition timed out
                                    CompletableFuture<PooledBoltConnection> pendingAcquisition;
                                    synchronized (this) {
                                        pendingAcquisition = pendingAcquisitions.poll();
                                        if (pendingAcquisition == null) {
                                            // nothing pending, just make the entry available
                                            entry.available = true;
                                        }
                                    }
                                    if (pendingAcquisition != null) {
                                        pendingAcquisition.complete(pooledConnection);
                                    }
                                }
                            }
                        });
            }
        }
    }

    private synchronized ConnectionEntryWithMetadata acquireExistingEntry(
            AuthToken authToken, BoltProtocolVersion minVersion) {
        ConnectionEntryWithMetadata connectionEntryWithMetadata = null;
        var iterator = pooledConnectionEntries.iterator();
        while (iterator.hasNext()) {
            var connectionEntry = iterator.next();

            // unavailable
            if (!connectionEntry.available) {
                continue;
            }

            var connection = connectionEntry.connection;
            // unusable
            if (connection.state() != BoltConnectionState.OPEN) {
                connection.close();
                iterator.remove();
                continue;
            }

            // lower version is present
            if (minVersion != null && minVersion.compareTo(connection.protocolVersion()) > 0) {
                throw new MinVersionAcquisitionException("lower version", connection.protocolVersion());
            }

            // exceeded max lifetime
            if (maxLifetime > 0) {
                var currentTime = clock.millis();
                if (currentTime - connectionEntry.createdTimestamp > maxLifetime) {
                    iterator.remove();
                    var closeObservation = observationProvider.pooledConnectionClose(poolId, uri);
                    connection.close().whenComplete((ignored, throwable) -> closeObservation.stop());
                    continue;
                }
            }

            // the pool must not have unauthenticated connections
            var authInfo = connection.authInfo().toCompletableFuture().getNow(null);

            var expiredByError = minAuthTimestamp > 0 && authInfo.authAckMillis() <= minAuthTimestamp;
            var authMatches = authToken.equals(authInfo.authToken());
            var reauthNeeded = expiredByError || !authMatches;

            if (reauthNeeded) {
                if (new BoltProtocolVersion(5, 1).compareTo(connectionEntry.connection.protocolVersion()) > 0) {
                    log.log(System.Logger.Level.DEBUG, "reauth is not supported, the connection is voided");
                    iterator.remove();
                    var observation = observationProvider.pooledConnectionClose(poolId, uri);
                    connectionEntry.connection.close().whenComplete((ignored, throwable) -> {
                        if (throwable != null) {
                            log.log(
                                    System.Logger.Level.WARNING,
                                    "Connection close has failed with %s.",
                                    throwable.getClass().getCanonicalName());
                        }
                        observation.stop();
                    });
                    continue;
                }
            }
            log.log(System.Logger.Level.DEBUG, "Connection acquired from the pool. " + address);
            connectionEntry.available = false;
            connectionEntryWithMetadata = new ConnectionEntryWithMetadata(connectionEntry, reauthNeeded);
            break;
        }
        return connectionEntryWithMetadata;
    }

    private CompletionStage<Void> reauthStage(
            ConnectionEntryWithMetadata connectionEntryWithMetadata, AuthToken authToken) {
        CompletionStage<Void> stage;
        if (connectionEntryWithMetadata.reauthNeeded) {
            stage = connectionEntryWithMetadata
                    .connectionEntry
                    .connection
                    .write(List.of(Messages.logoff(), Messages.logon(authToken)))
                    .handle((ignored, throwable) -> {
                        if (throwable != null) {
                            connectionEntryWithMetadata.connectionEntry.connection.close();
                            synchronized (this) {
                                pooledConnectionEntries.remove(connectionEntryWithMetadata.connectionEntry);
                            }
                        }
                        return null;
                    });
        } else {
            stage = CompletableFuture.completedStage(null);
        }
        return stage;
    }

    private CompletionStage<Void> livenessCheckStage(ConnectionEntry entry, ImmutableObservation parentObservation) {
        CompletionStage<Void> stage;
        if (idleBeforeTest >= 0 && entry.lastUsedTimestamp + idleBeforeTest < clock.millis()) {
            var resetHandler = new BasicResponseHandler();
            stage = entry.connection
                    .writeAndFlush(resetHandler, Messages.reset(), parentObservation)
                    .thenCompose(ignored -> resetHandler.summaries())
                    .thenApply(ignored -> null);
        } else {
            stage = CompletableFuture.completedStage(null);
        }
        return stage;
    }

    @Override
    public CompletionStage<Void> verifyConnectivity() {
        return getConnection().thenCompose(BoltConnection::close);
    }

    @Override
    public CompletionStage<Boolean> supportsMultiDb() {
        return getConnection().thenCompose(boltConnection -> {
            var supports = boltConnection.protocolVersion().compareTo(new BoltProtocolVersion(4, 0)) >= 0;
            return boltConnection.close().thenApply(ignored -> supports);
        });
    }

    @Override
    public CompletionStage<Boolean> supportsSessionAuth() {
        return getConnection().thenCompose(boltConnection -> {
            var supports = new BoltProtocolVersion(5, 1).compareTo(boltConnection.protocolVersion()) <= 0;
            return boltConnection.close().thenApply(ignored -> supports);
        });
    }

    @Override
    public CompletionStage<Void> close() {
        CompletionStage<Void> closeStage;
        synchronized (this) {
            if (this.closeStage == null) {
                var closeObservation = observationProvider.connectionPoolClose(poolId, uri);
                this.closeStage = CompletableFuture.completedStage(null);
                var iterator = pooledConnectionEntries.iterator();
                while (iterator.hasNext()) {
                    var entry = iterator.next();
                    if (entry.connection != null && entry.connection.state() == BoltConnectionState.OPEN) {
                        this.closeStage = this.closeStage.thenCompose(
                                ignored -> entry.connection.close().exceptionally(throwable -> null));
                    }
                    iterator.remove();
                }
                this.closeStage = this.closeStage
                        .thenCompose(ignored -> boltConnectionProvider.close())
                        .exceptionally(throwable -> null)
                        .whenComplete((ignored, throwable) -> {
                            executorService.shutdown();
                            closeObservation.stop();
                        });
            }
            closeStage = this.closeStage;
        }
        return closeStage;
    }

    synchronized int size() {
        return pooledConnectionEntries.size();
    }

    synchronized int inUse() {
        return pooledConnectionEntries.stream()
                .filter(entry -> !entry.available)
                .toList()
                .size();
    }

    private String poolId(BoltServerAddress serverAddress) {
        return serverAddress.port() <= 0
                ? String.format("%s-%d", serverAddress.host(), this.hashCode())
                : String.format("%s:%d-%d", serverAddress.host(), serverAddress.port(), this.hashCode());
    }

    private void release(ConnectionEntry entry) {
        CompletableFuture<PooledBoltConnection> pendingAcquisition;
        synchronized (this) {
            entry.lastUsedTimestamp = clock.millis();
            pendingAcquisition = pendingAcquisitions.poll();
            if (pendingAcquisition == null) {
                // nothing pending, just make the entry available
                entry.available = true;
            }
        }
        if (pendingAcquisition != null) {
            pendingAcquisition.complete(new PooledBoltConnection(
                    entry.connection,
                    this,
                    () -> release(entry),
                    () -> purge(entry),
                    observationParent -> observationProvider.pooledConnectionInUse(observationParent, poolId, uri)));
        }
        log.log(System.Logger.Level.DEBUG, "Connection released to the pool.");
    }

    private void purge(ConnectionEntry entry) {
        synchronized (this) {
            pooledConnectionEntries.remove(entry);
        }
        var closeObservation = observationProvider.pooledConnectionClose(poolId, uri);
        entry.connection.close().whenComplete((ignored, throwable) -> closeObservation.stop());
        log.log(System.Logger.Level.DEBUG, "Connection purged from the pool.");
    }

    public synchronized void onExpired() {
        var now = clock.millis();
        minAuthTimestamp = Math.max(minAuthTimestamp, now);
    }

    private ScheduledFuture<?> scheduleTimeout(
            CompletableFuture<PooledBoltConnection> acquisitionFuture, long acquisitionTimeout) {
        return executorService.schedule(
                () -> {
                    synchronized (this) {
                        pendingAcquisitions.remove(acquisitionFuture);
                    }
                    timeoutRunnable(acquisitionFuture).run();
                },
                acquisitionTimeout,
                TimeUnit.MILLISECONDS);
    }

    private Runnable timeoutRunnable(CompletableFuture<PooledBoltConnection> acquisitionFuture) {
        return () -> {
            try {
                acquisitionFuture.completeExceptionally(timeoutException());
            } catch (Throwable throwable) {
                log.log(System.Logger.Level.WARNING, "Unexpected error occurred.", throwable);
            }
        };
    }

    private TimeoutException timeoutException() {
        return new TimeoutException("Unable to acquire connection from the pool within configured maximum time of "
                + acquisitionTimeout + "ms");
    }

    private static class ConnectionEntry {
        private BoltConnection connection;
        private boolean available;
        private long createdTimestamp;
        private long lastUsedTimestamp;
    }

    private record SecurityPlanAndAuthToken(SecurityPlan securityPlan, AuthToken authToken) {}

    private record ConnectionEntryWithMetadata(ConnectionEntry connectionEntry, boolean reauthNeeded) {}

    public enum TimeoutPolicy {
        /**
         * The acquisition timeout starts immediately.
         */
        DEFAULT,
        /**
         * The acquisition timeout starts when the pool is busy.
         */
        LEGACY
    }
}
