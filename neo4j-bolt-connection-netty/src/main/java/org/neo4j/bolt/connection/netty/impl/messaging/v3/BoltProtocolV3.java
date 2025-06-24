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
package org.neo4j.bolt.connection.netty.impl.messaging.v3;

import static org.neo4j.bolt.connection.netty.impl.async.connection.ChannelAttributes.messageDispatcher;
import static org.neo4j.bolt.connection.netty.impl.messaging.request.CommitMessage.COMMIT;
import static org.neo4j.bolt.connection.netty.impl.messaging.request.RollbackMessage.ROLLBACK;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.bolt.connection.AccessMode;
import org.neo4j.bolt.connection.BoltAgent;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.ClusterComposition;
import org.neo4j.bolt.connection.DatabaseName;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.NotificationConfig;
import org.neo4j.bolt.connection.exception.BoltException;
import org.neo4j.bolt.connection.exception.BoltUnsupportedFeatureException;
import org.neo4j.bolt.connection.netty.impl.RoutingContext;
import org.neo4j.bolt.connection.netty.impl.handlers.BeginTxResponseHandler;
import org.neo4j.bolt.connection.netty.impl.handlers.CommitTxResponseHandler;
import org.neo4j.bolt.connection.netty.impl.handlers.DiscardResponseHandler;
import org.neo4j.bolt.connection.netty.impl.handlers.HelloResponseHandler;
import org.neo4j.bolt.connection.netty.impl.handlers.PullResponseHandlerImpl;
import org.neo4j.bolt.connection.netty.impl.handlers.ResetResponseHandler;
import org.neo4j.bolt.connection.netty.impl.handlers.RollbackTxResponseHandler;
import org.neo4j.bolt.connection.netty.impl.handlers.RunResponseHandler;
import org.neo4j.bolt.connection.netty.impl.messaging.BoltProtocol;
import org.neo4j.bolt.connection.netty.impl.messaging.MessageFormat;
import org.neo4j.bolt.connection.netty.impl.messaging.MessageHandler;
import org.neo4j.bolt.connection.netty.impl.messaging.PullMessageHandler;
import org.neo4j.bolt.connection.netty.impl.messaging.request.BeginMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.DiscardMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.HelloMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.MultiDatabaseUtil;
import org.neo4j.bolt.connection.netty.impl.messaging.request.PullAllMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.ResetMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.RunWithMetadataMessage;
import org.neo4j.bolt.connection.netty.impl.spi.Connection;
import org.neo4j.bolt.connection.netty.impl.util.MetadataExtractor;
import org.neo4j.bolt.connection.observation.BoltExchangeObservation;
import org.neo4j.bolt.connection.summary.BeginSummary;
import org.neo4j.bolt.connection.summary.DiscardSummary;
import org.neo4j.bolt.connection.summary.PullSummary;
import org.neo4j.bolt.connection.summary.RouteSummary;
import org.neo4j.bolt.connection.summary.RunSummary;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.bolt.connection.values.ValueFactory;

public class BoltProtocolV3 implements BoltProtocol {
    public static final BoltProtocolVersion VERSION = new BoltProtocolVersion(3, 0);

    public static final BoltProtocol INSTANCE = new BoltProtocolV3();

    public static final MetadataExtractor METADATA_EXTRACTOR = new MetadataExtractor("t_first");

    private static final String ROUTING_CONTEXT = "context";
    private static final String GET_ROUTING_TABLE =
            "CALL dbms.cluster.routing.getRoutingTable($" + ROUTING_CONTEXT + ")";

    @Override
    public MessageFormat createMessageFormat() {
        return new MessageFormatV3();
    }

    @Override
    public CompletionStage<Channel> initializeChannel(
            Channel channel,
            String userAgent,
            BoltAgent boltAgent,
            Map<String, Value> authMap,
            RoutingContext routingContext,
            NotificationConfig notificationConfig,
            Clock clock,
            CompletableFuture<Long> latestAuthMillisFuture,
            ValueFactory valueFactory,
            BoltExchangeObservation observation) {
        var exception = verifyNotificationConfigSupported(notificationConfig);
        if (exception != null) {
            return CompletableFuture.failedStage(exception);
        }
        HelloMessage message;

        if (routingContext.isServerRoutingEnabled()) {
            message = new HelloMessage(
                    userAgent,
                    null,
                    authMap,
                    routingContext.toMap(),
                    includeDateTimeUtcPatchInHello(),
                    notificationConfig,
                    useLegacyNotifications(),
                    valueFactory);
        } else {
            message = new HelloMessage(
                    userAgent,
                    null,
                    authMap,
                    null,
                    includeDateTimeUtcPatchInHello(),
                    notificationConfig,
                    useLegacyNotifications(),
                    valueFactory);
        }

        var future = new CompletableFuture<String>();
        var handler = new HelloResponseHandler(future, channel, clock, latestAuthMillisFuture);
        messageDispatcher(channel).enqueue(handler);
        channel.writeAndFlush(message).addListener((ChannelFutureListener) writeFuture -> {
            if (writeFuture.isSuccess()) {
                observation.onWrite(message.name());
            }
        });
        return future.thenApply(ignored -> {
            observation.onSummary(message.name());
            return channel;
        });
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public CompletionStage<Void> route(
            Connection connection,
            Map<String, Value> routingContext,
            Set<String> bookmarks,
            String databaseName,
            String impersonatedUser,
            MessageHandler<RouteSummary> handler,
            Clock clock,
            LoggingProvider logging,
            ValueFactory valueFactory,
            BoltExchangeObservation observation) {
        var query = new Query(GET_ROUTING_TABLE, Map.of(ROUTING_CONTEXT, valueFactory.value(routingContext)));

        var runMessage = RunWithMetadataMessage.autoCommitTxRunMessage(
                query.query(),
                query.parameters(),
                null,
                Collections.emptyMap(),
                DatabaseName.defaultDatabase(),
                AccessMode.WRITE,
                Collections.emptySet(),
                null,
                NotificationConfig.defaultConfig(),
                useLegacyNotifications(),
                logging,
                valueFactory);
        var runFuture = new CompletableFuture<RunSummary>();
        var runHandler = new RunResponseHandler(runFuture, METADATA_EXTRACTOR);
        var pullFuture = new CompletableFuture<Map<String, Value>>();

        runFuture
                .thenCompose(ignored -> {
                    observation.onSummary(runMessage.name());
                    return pullFuture;
                })
                .thenApply(map -> {
                    var ttl = map.get("ttl").asLong();
                    var expirationTimestamp = clock.millis() + ttl * 1000;
                    if (ttl < 0 || ttl >= Long.MAX_VALUE / 1000L || expirationTimestamp < 0) {
                        expirationTimestamp = Long.MAX_VALUE;
                    }

                    Set<BoltServerAddress> readers = new LinkedHashSet<>();
                    Set<BoltServerAddress> writers = new LinkedHashSet<>();
                    Set<BoltServerAddress> routers = new LinkedHashSet<>();

                    for (var serversMap : map.get("servers").boltValues()) {
                        var role = serversMap.getBoltValue("role").asString();
                        for (var server : serversMap.getBoltValue("addresses").boltValues()) {
                            var address = new BoltServerAddress(server.asString());
                            switch (role) {
                                case "WRITE" -> writers.add(address);
                                case "READ" -> readers.add(address);
                                case "ROUTE" -> routers.add(address);
                            }
                        }
                    }
                    var db = map.get("db");
                    String name = null;
                    if (db != null && !db.isNull()) {
                        name = db.asString();
                    }

                    var clusterComposition =
                            new ClusterComposition(expirationTimestamp, readers, writers, routers, name);
                    return new RouteSummaryImpl(clusterComposition);
                })
                .whenComplete((summary, throwable) -> {
                    if (throwable != null) {
                        handler.onError(throwable);
                    } else {
                        handler.onSummary(summary);
                    }
                });

        return connection.write(runMessage, runHandler).thenCompose(ignored -> {
            observation.onWrite(runMessage.name());
            var pullMessage = PullAllMessage.PULL_ALL;
            var pullHandler = new PullResponseHandlerImpl(
                    new PullMessageHandler() {
                        private Map<String, Value> routingTable;

                        @Override
                        public void onRecord(List<Value> fields) {
                            observation.onRecord();
                            if (routingTable == null) {
                                var keys = runFuture.join().keys();
                                routingTable = new HashMap<>(keys.size());
                                for (var i = 0; i < keys.size(); i++) {
                                    routingTable.put(keys.get(i), fields.get(i));
                                }
                                routingTable = Collections.unmodifiableMap(routingTable);
                            }
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            pullFuture.completeExceptionally(throwable);
                        }

                        @Override
                        public void onSummary(PullSummary success) {
                            observation.onSummary(pullMessage.name());
                            pullFuture.complete(routingTable);
                        }
                    },
                    valueFactory);
            return connection
                    .write(pullMessage, pullHandler)
                    .thenAccept(message -> observation.onWrite(pullMessage.name()));
        });
    }

    @Override
    public CompletionStage<Void> beginTransaction(
            Connection connection,
            DatabaseName databaseName,
            AccessMode accessMode,
            String impersonatedUser,
            Set<String> bookmarks,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            String txType,
            NotificationConfig notificationConfig,
            MessageHandler<BeginSummary> handler,
            LoggingProvider logging,
            ValueFactory valueFactory,
            BoltExchangeObservation observation) {
        var exception = verifyNotificationConfigSupported(notificationConfig);
        if (exception != null) {
            return CompletableFuture.failedStage(exception);
        }
        try {
            verifyDatabaseNameBeforeTransaction(databaseName);
        } catch (Exception error) {
            return CompletableFuture.failedFuture(error);
        }

        var beginTxFuture = new CompletableFuture<BeginSummary>();
        var beginMessage = new BeginMessage(
                bookmarks,
                txTimeout,
                txMetadata,
                databaseName,
                accessMode,
                impersonatedUser,
                txType,
                notificationConfig,
                useLegacyNotifications(),
                logging,
                valueFactory);
        beginTxFuture.whenComplete((summary, throwable) -> {
            if (throwable != null) {
                handler.onError(throwable);
            } else {
                observation.onSummary(beginMessage.name());
                handler.onSummary(summary);
            }
        });
        return connection
                .write(beginMessage, new BeginTxResponseHandler(beginTxFuture))
                .thenAccept(message -> observation.onWrite(beginMessage.name()));
    }

    @Override
    public CompletionStage<Void> commitTransaction(
            Connection connection, MessageHandler<String> handler, BoltExchangeObservation observation) {
        var commitFuture = new CompletableFuture<String>();
        commitFuture.whenComplete((bookmark, throwable) -> {
            if (throwable != null) {
                handler.onError(throwable);
            } else {
                observation.onSummary(COMMIT.name());
                handler.onSummary(bookmark);
            }
        });
        return connection
                .write(COMMIT, new CommitTxResponseHandler(commitFuture))
                .thenAccept(message -> observation.onWrite(COMMIT.name()));
    }

    @Override
    public CompletionStage<Void> rollbackTransaction(
            Connection connection, MessageHandler<Void> handler, BoltExchangeObservation observation) {
        var rollbackFuture = new CompletableFuture<Void>();
        rollbackFuture.whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                handler.onError(throwable);
            } else {
                observation.onSummary(ROLLBACK.name());
                handler.onSummary(null);
            }
        });
        return connection
                .write(ROLLBACK, new RollbackTxResponseHandler(rollbackFuture))
                .thenAccept(message -> observation.onWrite(ROLLBACK.name()));
    }

    @Override
    public CompletionStage<Void> reset(
            Connection connection, MessageHandler<Void> handler, BoltExchangeObservation observation) {
        var resetFuture = new CompletableFuture<Void>();
        resetFuture.whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                handler.onError(throwable);
            } else {
                observation.onSummary(ResetMessage.RESET.name());
                handler.onSummary(null);
            }
        });
        var resetHandler = new ResetResponseHandler(resetFuture);
        return connection
                .write(ResetMessage.RESET, resetHandler)
                .thenAccept(message -> observation.onWrite(ResetMessage.RESET.name()));
    }

    @Override
    public CompletionStage<Void> telemetry(
            Connection connection, Integer api, MessageHandler<Void> handler, BoltExchangeObservation observation) {
        return CompletableFuture.failedStage(new BoltUnsupportedFeatureException("telemetry not supported"));
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public CompletionStage<Void> runAuto(
            Connection connection,
            DatabaseName databaseName,
            AccessMode accessMode,
            String impersonatedUser,
            String query,
            Map<String, Value> parameters,
            Set<String> bookmarks,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            NotificationConfig notificationConfig,
            MessageHandler<RunSummary> handler,
            LoggingProvider logging,
            ValueFactory valueFactory,
            BoltExchangeObservation observation) {
        try {
            verifyDatabaseNameBeforeTransaction(databaseName);
        } catch (Exception error) {
            return CompletableFuture.failedFuture(error);
        }

        var runMessage = RunWithMetadataMessage.autoCommitTxRunMessage(
                query,
                parameters,
                txTimeout,
                txMetadata,
                databaseName,
                accessMode,
                bookmarks,
                impersonatedUser,
                notificationConfig,
                useLegacyNotifications(),
                logging,
                valueFactory);
        var runFuture = new CompletableFuture<RunSummary>();
        runFuture.whenComplete((summary, throwable) -> {
            if (throwable != null) {
                handler.onError(throwable);
            } else {
                observation.onSummary(runMessage.name());
                handler.onSummary(summary);
            }
        });
        var runHandler = new RunResponseHandler(runFuture, METADATA_EXTRACTOR);
        return connection.write(runMessage, runHandler).thenAccept(message -> observation.onWrite(runMessage.name()));
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public CompletionStage<Void> run(
            Connection connection,
            String query,
            Map<String, Value> parameters,
            MessageHandler<RunSummary> handler,
            BoltExchangeObservation observation) {
        var runMessage = RunWithMetadataMessage.unmanagedTxRunMessage(query, parameters);
        var runFuture = new CompletableFuture<RunSummary>();
        runFuture.whenComplete((summary, throwable) -> {
            if (throwable != null) {
                handler.onError(throwable);
            } else {
                observation.onSummary(runMessage.name());
                handler.onSummary(summary);
            }
        });
        var runHandler = new RunResponseHandler(runFuture, METADATA_EXTRACTOR);
        return connection.write(runMessage, runHandler).thenAccept(message -> observation.onWrite(runMessage.name()));
    }

    @Override
    public CompletionStage<Void> pull(
            Connection connection,
            long qid,
            long request,
            PullMessageHandler handler,
            ValueFactory valueFactory,
            BoltExchangeObservation observation) {
        var pullMessage = PullAllMessage.PULL_ALL;
        var pullHandler = new PullResponseHandlerImpl(
                new PullMessageHandler() {
                    @Override
                    public void onRecord(List<Value> fields) {
                        observation.onRecord();
                        handler.onRecord(fields);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        handler.onError(throwable);
                    }

                    @Override
                    public void onSummary(PullSummary summary) {
                        observation.onSummary(pullMessage.name());
                        handler.onSummary(summary);
                    }
                },
                valueFactory);
        return connection
                .write(pullMessage, pullHandler)
                .thenAccept(message -> observation.onWrite(pullMessage.name()));
    }

    @Override
    public CompletionStage<Void> discard(
            Connection connection,
            long qid,
            long number,
            MessageHandler<DiscardSummary> handler,
            ValueFactory valueFactory,
            BoltExchangeObservation observation) {
        var discardMessage = new DiscardMessage(number, qid, valueFactory);
        var discardFuture = new CompletableFuture<DiscardSummary>();
        discardFuture.whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                handler.onError(throwable);
            } else {
                observation.onSummary(discardMessage.name());
                handler.onSummary(ignored);
            }
        });
        var discardHandler = new DiscardResponseHandler(discardFuture);
        return connection
                .write(discardMessage, discardHandler)
                .thenAccept(message -> observation.onWrite(discardMessage.name()));
    }

    protected void verifyDatabaseNameBeforeTransaction(DatabaseName databaseName) {
        MultiDatabaseUtil.assertEmptyDatabaseName(databaseName, version());
    }

    @Override
    public BoltProtocolVersion version() {
        return VERSION;
    }

    protected boolean includeDateTimeUtcPatchInHello() {
        return false;
    }

    protected BoltException verifyNotificationConfigSupported(NotificationConfig notificationConfig) {
        BoltException exception = null;
        if (notificationConfig != null && !notificationConfig.equals(NotificationConfig.defaultConfig())) {
            exception = new BoltUnsupportedFeatureException(String.format(
                    "Notification configuration is not supported on Bolt %s",
                    version().toString()));
        }
        return exception;
    }

    protected boolean useLegacyNotifications() {
        return true;
    }

    private record RouteSummaryImpl(ClusterComposition clusterComposition) implements RouteSummary {}

    public record Query(String query, Map<String, Value> parameters) {}
}
