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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
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
import org.neo4j.bolt.connection.MetricsListener;
import org.neo4j.bolt.connection.NotificationConfig;
import org.neo4j.bolt.connection.SecurityPlan;
import org.neo4j.bolt.connection.exception.BoltFailureException;
import org.neo4j.bolt.connection.exception.BoltTransientException;
import org.neo4j.bolt.connection.exception.MinVersionAcquisitionException;
import org.neo4j.bolt.connection.message.Messages;
import org.neo4j.bolt.connection.pooled.impl.PooledBoltConnection;
import org.neo4j.bolt.connection.pooled.impl.util.FutureUtil;

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
    private final MetricsListener metricsListener;
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
            MetricsListener metricsListener,
            String routingContextAddress,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            NotificationConfig notificationConfig) {
        this.boltConnectionProvider = Objects.requireNonNull(boltConnectionProvider);
        this.pooledConnectionEntries = new ArrayList<>();
        this.pendingAcquisitions = new ArrayDeque<>(100);
        this.maxSize = maxSize;
        this.acquisitionTimeout = acquisitionTimeout;
        this.maxLifetime = maxLifetime;
        this.idleBeforeTest = idleBeforeTest;
        this.clock = Objects.requireNonNull(clock);
        this.log = loggingProvider.getLog(getClass());
        this.metricsListener = Objects.requireNonNull(metricsListener);
        this.uri = Objects.requireNonNull(uri);
        this.address = switch (uri.getScheme()) {
            case "bolt", "bolt+s", "bolt+ssc", "neo4j", "neo4j+s", "neo4j+ssc" -> new BoltServerAddress(uri);
            default -> new BoltServerAddress(uri.getHost(), 0);};
        this.routingContextAddress = routingContextAddress;
        this.boltAgent = Objects.requireNonNull(boltAgent);
        this.userAgent = Objects.requireNonNull(userAgent);
        this.connectTimeoutMillis = connectTimeoutMillis;
        this.authTokenManager = Objects.requireNonNull(authTokenManager);
        this.securityPlanSupplier = Objects.requireNonNull(securityPlanSupplier);
        this.notificationConfig = Objects.requireNonNull(notificationConfig);
        this.poolId = poolId(address);
        metricsListener.registerPoolMetrics(
                poolId,
                address,
                () -> {
                    synchronized (this) {
                        return (int) pooledConnectionEntries.stream()
                                .filter(entry -> !entry.available)
                                .count();
                    }
                },
                () -> {
                    synchronized (this) {
                        return (int) pooledConnectionEntries.stream()
                                .filter(entry -> entry.available)
                                .count();
                    }
                });
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

        var acquisitionFuture = new CompletableFuture<PooledBoltConnection>();
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

            var beforeAcquiringOrCreatingEvent = metricsListener.createListenerEvent();
            metricsListener.beforeAcquiringOrCreating(poolId, beforeAcquiringOrCreatingEvent);
            acquisitionFuture.whenComplete((connection, throwable) -> {
                throwable = FutureUtil.completionExceptionCause(throwable);
                if (throwable != null) {
                    if (throwable instanceof TimeoutException) {
                        metricsListener.afterTimedOutToAcquireOrCreate(poolId);
                    }
                } else {
                    metricsListener.afterAcquiredOrCreated(poolId, beforeAcquiringOrCreatingEvent);
                }
                metricsListener.afterAcquiringOrCreating(poolId);
            });
            connect(acquisitionFuture, authToken, overrideAuthToken, parameters.minVersion(), notificationConfig);
        });

        return acquisitionFuture.thenApply(Function.identity());
    }

    @SuppressWarnings({"DuplicatedCode", "ConstantValue"})
    private void connect(
            CompletableFuture<PooledBoltConnection> acquisitionFuture,
            AuthToken authToken,
            boolean overrideAuthToken,
            BoltProtocolVersion minVersion,
            NotificationConfig notificationConfig) {

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
                            if (acquisitionTimeout > 0) {
                                pendingAcquisitions.add(acquisitionFuture);
                            }
                            // schedule timeout
                            executorService.schedule(
                                    () -> {
                                        synchronized (this) {
                                            pendingAcquisitions.remove(acquisitionFuture);
                                        }
                                        try {
                                            acquisitionFuture.completeExceptionally(new TimeoutException(
                                                    "Unable to acquire connection from the pool within configured maximum time of "
                                                            + acquisitionTimeout + "ms"));
                                        } catch (Throwable throwable) {
                                            log.log(
                                                    System.Logger.Level.WARNING,
                                                    "Unexpected error occurred.",
                                                    throwable);
                                        }
                                    },
                                    acquisitionTimeout,
                                    TimeUnit.MILLISECONDS);
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

                livenessCheckStage(entry).whenComplete((ignored, throwable) -> {
                    if (throwable != null) {
                        // liveness check failed
                        purge(entry);
                        connect(acquisitionFuture, authToken, overrideAuthToken, minVersion, notificationConfig);
                    } else {
                        // liveness check green or not needed
                        var inUseEvent = metricsListener.createListenerEvent();
                        var pooledConnection = new PooledBoltConnection(
                                entry.connection,
                                this,
                                () -> {
                                    release(entry);
                                    metricsListener.afterConnectionReleased(poolId, inUseEvent);
                                },
                                () -> {
                                    purge(entry);
                                    metricsListener.afterConnectionReleased(poolId, inUseEvent);
                                });
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
                                    if (pendingAcquisition.complete(pooledConnection)) {
                                        metricsListener.afterConnectionCreated(poolId, inUseEvent);
                                    }
                                }
                            } else {
                                metricsListener.afterConnectionCreated(poolId, inUseEvent);
                            }
                        });
                    }
                });
            } else {
                // get reserved entry
                var createEvent = metricsListener.createListenerEvent();
                metricsListener.beforeCreating(poolId, createEvent);
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
                                auth.securityPlan(),
                                auth.authToken(),
                                minVersion,
                                notificationConfig))
                        .whenComplete((boltConnection, throwable) -> {
                            var error = FutureUtil.completionExceptionCause(throwable);
                            if (error != null) {
                                synchronized (this) {
                                    pooledConnectionEntries.remove(entry);
                                }
                                metricsListener.afterFailedToCreate(poolId);
                                if (error instanceof BoltFailureException boltFailureException) {
                                    var usedAuth =
                                            authStage.toCompletableFuture().getNow(null);
                                    if (usedAuth != null) {
                                        error = authTokenManager.handleBoltFailureException(
                                                usedAuth.authToken(), boltFailureException);
                                    }
                                }
                                acquisitionFuture.completeExceptionally(error);
                            } else {
                                synchronized (this) {
                                    entry.connection = boltConnection;
                                    entry.createdTimestamp = clock.millis();
                                }
                                metricsListener.afterCreated(poolId, createEvent);
                                var inUseEvent = metricsListener.createListenerEvent();
                                var pooledConnection = new PooledBoltConnection(
                                        boltConnection,
                                        this,
                                        () -> {
                                            release(entry);
                                            metricsListener.afterConnectionReleased(poolId, inUseEvent);
                                        },
                                        () -> {
                                            purge(entry);
                                            metricsListener.afterConnectionReleased(poolId, inUseEvent);
                                        });
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
                                        if (pendingAcquisition.complete(pooledConnection)) {
                                            metricsListener.afterConnectionCreated(poolId, inUseEvent);
                                        }
                                    }
                                } else {
                                    metricsListener.afterConnectionCreated(poolId, inUseEvent);
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
                    connection.close();
                    iterator.remove();
                    metricsListener.afterClosed(poolId);
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
                    connectionEntry.connection.close().whenComplete((ignored, throwable) -> {
                        if (throwable != null) {
                            log.log(
                                    System.Logger.Level.WARNING,
                                    "Connection close has failed with %s.",
                                    throwable.getClass().getCanonicalName());
                        }
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

    private CompletionStage<Void> livenessCheckStage(ConnectionEntry entry) {
        CompletionStage<Void> stage;
        if (idleBeforeTest >= 0 && entry.lastUsedTimestamp + idleBeforeTest < clock.millis()) {
            var resetHandler = new BasicResponseHandler();
            stage = entry.connection
                    .writeAndFlush(resetHandler, Messages.reset())
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
                metricsListener.removePoolMetrics(poolId);
                this.closeStage = this.closeStage
                        .thenCompose(ignored -> boltConnectionProvider.close())
                        .exceptionally(throwable -> null)
                        .whenComplete((ignored, throwable) -> executorService.shutdown());
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
        return serverAddress.port() == 0
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
            var inUseEvent = metricsListener.createListenerEvent();
            if (pendingAcquisition.complete(new PooledBoltConnection(
                    entry.connection,
                    this,
                    () -> {
                        release(entry);
                        metricsListener.afterConnectionReleased(poolId, inUseEvent);
                    },
                    () -> {
                        purge(entry);
                        metricsListener.afterConnectionReleased(poolId, inUseEvent);
                    }))) {
                metricsListener.afterConnectionCreated(poolId, inUseEvent);
            }
        }
        log.log(System.Logger.Level.DEBUG, "Connection released to the pool.");
    }

    private void purge(ConnectionEntry entry) {
        synchronized (this) {
            pooledConnectionEntries.remove(entry);
        }
        metricsListener.afterClosed(poolId);
        entry.connection.close();
        log.log(System.Logger.Level.DEBUG, "Connection purged from the pool.");
    }

    public synchronized void onExpired() {
        var now = clock.millis();
        minAuthTimestamp = Math.max(minAuthTimestamp, now);
    }

    private static class ConnectionEntry {
        private BoltConnection connection;
        private boolean available;
        private long createdTimestamp;
        private long lastUsedTimestamp;
    }

    private record SecurityPlanAndAuthToken(SecurityPlan securityPlan, AuthToken authToken) {}

    private record ConnectionEntryWithMetadata(ConnectionEntry connectionEntry, boolean reauthNeeded) {}
}
