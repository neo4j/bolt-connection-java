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
package org.neo4j.bolt.connection.netty.impl;

import io.netty.channel.EventLoop;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.EncoderException;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.neo4j.bolt.connection.AuthInfo;
import org.neo4j.bolt.connection.AuthToken;
import org.neo4j.bolt.connection.BoltConnection;
import org.neo4j.bolt.connection.BoltConnectionState;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.DatabaseName;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.ResponseHandler;
import org.neo4j.bolt.connection.exception.BoltClientException;
import org.neo4j.bolt.connection.exception.BoltConnectionReadTimeoutException;
import org.neo4j.bolt.connection.exception.BoltException;
import org.neo4j.bolt.connection.exception.BoltFailureException;
import org.neo4j.bolt.connection.exception.BoltProtocolException;
import org.neo4j.bolt.connection.exception.BoltServiceUnavailableException;
import org.neo4j.bolt.connection.exception.BoltUnsupportedFeatureException;
import org.neo4j.bolt.connection.message.BeginMessage;
import org.neo4j.bolt.connection.message.CommitMessage;
import org.neo4j.bolt.connection.message.DiscardMessage;
import org.neo4j.bolt.connection.message.LogoffMessage;
import org.neo4j.bolt.connection.message.LogonMessage;
import org.neo4j.bolt.connection.message.Message;
import org.neo4j.bolt.connection.message.PullMessage;
import org.neo4j.bolt.connection.message.ResetMessage;
import org.neo4j.bolt.connection.message.RollbackMessage;
import org.neo4j.bolt.connection.message.RouteMessage;
import org.neo4j.bolt.connection.message.RunMessage;
import org.neo4j.bolt.connection.message.TelemetryMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.BoltProtocol;
import org.neo4j.bolt.connection.netty.impl.messaging.MessageHandler;
import org.neo4j.bolt.connection.netty.impl.messaging.PullMessageHandler;
import org.neo4j.bolt.connection.netty.impl.spi.Connection;
import org.neo4j.bolt.connection.netty.impl.util.FutureUtil;
import org.neo4j.bolt.connection.observation.BoltExchangeObservation;
import org.neo4j.bolt.connection.observation.ImmutableObservation;
import org.neo4j.bolt.connection.observation.Observation;
import org.neo4j.bolt.connection.observation.ObservationProvider;
import org.neo4j.bolt.connection.summary.BeginSummary;
import org.neo4j.bolt.connection.summary.CommitSummary;
import org.neo4j.bolt.connection.summary.DiscardSummary;
import org.neo4j.bolt.connection.summary.LogoffSummary;
import org.neo4j.bolt.connection.summary.LogonSummary;
import org.neo4j.bolt.connection.summary.PullSummary;
import org.neo4j.bolt.connection.summary.ResetSummary;
import org.neo4j.bolt.connection.summary.RollbackSummary;
import org.neo4j.bolt.connection.summary.RouteSummary;
import org.neo4j.bolt.connection.summary.RunSummary;
import org.neo4j.bolt.connection.summary.TelemetrySummary;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.bolt.connection.values.ValueFactory;

public final class BoltConnectionImpl implements BoltConnection {
    private final LoggingProvider logging;
    private final System.Logger log;
    private final BoltProtocol protocol;
    private final Connection connection;
    private final EventLoop eventLoop;
    private final String serverAgent;
    private final BoltServerAddress serverAddress;
    private final BoltProtocolVersion protocolVersion;
    private final boolean telemetrySupported;
    private final boolean serverSideRouting;
    private final AtomicReference<BoltConnectionState> stateRef = new AtomicReference<>(BoltConnectionState.OPEN);
    private final AtomicReference<CompletableFuture<AuthInfo>> authDataRef;
    private final Map<String, Value> routingContext;
    private final Queue<Message> messages;
    private final Clock clock;
    private final ValueFactory valueFactory;
    private final ObservationProvider observationProvider;

    public BoltConnectionImpl(
            BoltProtocol protocol,
            Connection connection,
            EventLoop eventLoop,
            AuthToken authToken,
            CompletableFuture<Long> latestAuthMillisFuture,
            RoutingContext routingContext,
            Clock clock,
            LoggingProvider logging,
            ValueFactory valueFactory,
            ObservationProvider observationProvider) {
        this.protocol = Objects.requireNonNull(protocol);
        this.connection = Objects.requireNonNull(connection);
        this.eventLoop = Objects.requireNonNull(eventLoop);
        this.serverAgent = Objects.requireNonNull(connection.serverAgent());
        this.serverAddress = Objects.requireNonNull(connection.serverAddress());
        this.protocolVersion = Objects.requireNonNull(connection.protocol().version());
        this.telemetrySupported = connection.isTelemetryEnabled();
        this.serverSideRouting = routingContext.isServerRoutingEnabled() && connection.isSsrEnabled();
        this.authDataRef = new AtomicReference<>(
                CompletableFuture.completedFuture(new AuthInfoImpl(authToken, latestAuthMillisFuture.join())));
        this.valueFactory = Objects.requireNonNull(valueFactory);
        this.routingContext = routingContext.toMap().entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(
                        Map.Entry::getKey, entry -> valueFactory.value(entry.getValue()), (a, b) -> b));
        this.messages = new ArrayDeque<>();
        this.clock = Objects.requireNonNull(clock);
        this.logging = Objects.requireNonNull(logging);
        this.log = this.logging.getLog(getClass());
        this.observationProvider = Objects.requireNonNull(observationProvider);
    }

    @Override
    public CompletionStage<Void> writeAndFlush(
            ResponseHandler handler, List<Message> messages, ImmutableObservation parentObservation) {
        var flushFuture = new CompletableFuture<Void>();
        return executeInEventLoop(() -> {
                    this.messages.addAll(messages);
                    flush(handler, flushFuture, parentObservation);
                })
                .thenCompose(ignored -> flushFuture)
                .handle((ignored, throwable) -> {
                    if (throwable != null) {
                        throwable = FutureUtil.completionExceptionCause(throwable);
                        updateState(throwable);
                        if (throwable instanceof BoltException boltException) {
                            throw boltException;
                        } else if (throwable.getCause() instanceof BoltException boltException) {
                            throw boltException;
                        } else {
                            throw new BoltClientException("Failed to write messages", throwable);
                        }
                    } else {
                        return null;
                    }
                });
    }

    @Override
    public CompletionStage<Void> write(List<Message> messages) {
        return executeInEventLoop(() -> this.messages.addAll(messages)).thenApply(ignored -> null);
    }

    private void flush(
            ResponseHandler handler, CompletableFuture<Void> flushFuture, ImmutableObservation parentObservation) {
        if (connection.isOpen()) {
            var flushStage = CompletableFuture.<Void>completedStage(null);
            var observation = observationProvider.boltExchange(
                    parentObservation,
                    serverAddress.connectionHost(),
                    serverAddress.port(),
                    protocolVersion,
                    (key, value) -> {});
            var responseHandler = new ResponseHandleImpl(handler, messages.size(), observation);

            for (var message : messages) {
                flushStage = flushStage.thenCompose(ignored -> writeMessage(responseHandler, message, observation));
            }
            messages.clear();

            flushStage.thenCompose(ignored -> connection.flush()).whenComplete((ignored, throwable) -> {
                if (throwable != null) {
                    throwable = FutureUtil.completionExceptionCause(throwable);
                    if (throwable instanceof CodecException && throwable.getCause() instanceof IOException) {
                        var serviceError = new BoltServiceUnavailableException(
                                "Connection to the database failed", throwable.getCause());
                        forceClose("Connection has been closed due to encoding error")
                                .whenComplete((ignored1, ignored2) -> flushFuture.completeExceptionally(serviceError));
                    } else {
                        flushFuture.completeExceptionally(throwable);
                    }
                    observation.error(throwable);
                    observation.stop();
                } else {
                    flushFuture.complete(null);
                    log.log(System.Logger.Level.DEBUG, "flushed");
                }
            });
        } else {
            throw new BoltServiceUnavailableException("Connection is closed");
        }
    }

    private CompletionStage<Void> writeMessage(
            ResponseHandler handler, Message message, BoltExchangeObservation observation) {
        CompletionStage<Void> result;
        if (message instanceof RouteMessage routeMessage) {
            result = writeMessage(handler, routeMessage, observation);
        } else if (message instanceof BeginMessage beginMessage) {
            result = writeMessage(handler, beginMessage, observation);
        } else if (message instanceof RunMessage runMessage) {
            result = writeMessage(handler, runMessage, observation);
        } else if (message instanceof PullMessage pullMessage) {
            result = writeMessage(handler, pullMessage, observation);
        } else if (message instanceof DiscardMessage discardMessage) {
            result = writeMessage(handler, discardMessage, observation);
        } else if (message instanceof CommitMessage commitMessage) {
            result = writeMessage(handler, commitMessage, observation);
        } else if (message instanceof RollbackMessage rollbackMessage) {
            result = writeMessage(handler, rollbackMessage, observation);
        } else if (message instanceof ResetMessage resetMessage) {
            result = writeMessage(handler, resetMessage, observation);
        } else if (message instanceof LogoffMessage logoffMessage) {
            result = writeMessage(handler, logoffMessage, observation);
        } else if (message instanceof LogonMessage logonMessage) {
            result = writeMessage(handler, logonMessage, observation);
        } else if (message instanceof TelemetryMessage telemetryMessage) {
            result = writeMessage(handler, telemetryMessage, observation);
        } else {
            result = CompletableFuture.failedStage(new BoltException("Unknown message type: " + message.getClass()));
        }
        return result;
    }

    private CompletionStage<Void> writeMessage(
            ResponseHandler handler, RouteMessage routeMessage, BoltExchangeObservation observation) {
        return protocol.route(
                this.connection,
                this.routingContext,
                routeMessage.bookmarks(),
                routeMessage.databaseName().orElse(null),
                routeMessage.impersonatedUser().orElse(null),
                new MessageHandler<>() {
                    @Override
                    public void onError(Throwable throwable) {
                        updateState(throwable);
                        handler.onError(throwable);
                    }

                    @Override
                    public void onSummary(RouteSummary summary) {
                        handler.onRouteSummary(summary);
                    }
                },
                clock,
                logging,
                valueFactory,
                observation);
    }

    private CompletionStage<Void> writeMessage(
            ResponseHandler handler, BeginMessage beginMessage, BoltExchangeObservation observation) {
        return protocol.beginTransaction(
                this.connection,
                DatabaseName.database(beginMessage.databaseName().orElse(null)),
                beginMessage.accessMode(),
                beginMessage.impersonatedUser().orElse(null),
                beginMessage.bookmarks(),
                beginMessage.txTimeout().orElse(null),
                beginMessage.txMetadata(),
                switch (beginMessage.transactionType()) {
                    case DEFAULT -> null;
                    case UNCONSTRAINED -> "IMPLICIT";
                },
                beginMessage.notificationConfig(),
                new MessageHandler<>() {
                    @Override
                    public void onError(Throwable throwable) {
                        updateState(throwable);
                        handler.onError(throwable);
                    }

                    @Override
                    public void onSummary(BeginSummary summary) {
                        handler.onBeginSummary(summary);
                    }
                },
                logging,
                valueFactory,
                observation);
    }

    private CompletionStage<Void> writeMessage(
            ResponseHandler handler, RunMessage runMessage, BoltExchangeObservation observation) {
        if (runMessage.extra().isEmpty()) {
            return protocol.run(
                    connection,
                    runMessage.query(),
                    runMessage.parameters(),
                    new MessageHandler<>() {
                        @Override
                        public void onError(Throwable throwable) {
                            updateState(throwable);
                            handler.onError(throwable);
                        }

                        @Override
                        public void onSummary(RunSummary summary) {
                            handler.onRunSummary(summary);
                        }
                    },
                    observation);
        } else {
            var extra = runMessage.extra().get();
            return protocol.runAuto(
                    connection,
                    DatabaseName.database(extra.databaseName().orElse(null)),
                    extra.accessMode(),
                    extra.impersonatedUser().orElse(null),
                    runMessage.query(),
                    runMessage.parameters(),
                    extra.bookmarks(),
                    extra.txTimeout().orElse(null),
                    extra.txMetadata(),
                    extra.notificationConfig(),
                    new MessageHandler<>() {
                        @Override
                        public void onError(Throwable throwable) {
                            updateState(throwable);
                            handler.onError(throwable);
                        }

                        @Override
                        public void onSummary(RunSummary summary) {
                            handler.onRunSummary(summary);
                        }
                    },
                    logging,
                    valueFactory,
                    observation);
        }
    }

    private CompletionStage<Void> writeMessage(
            ResponseHandler handler, PullMessage pullMessage, BoltExchangeObservation observation) {
        return protocol.pull(
                connection,
                pullMessage.qid(),
                pullMessage.request(),
                new PullMessageHandler() {
                    @Override
                    public void onRecord(List<Value> fields) {
                        handler.onRecord(fields);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        updateState(throwable);
                        handler.onError(throwable);
                    }

                    @Override
                    public void onSummary(PullSummary success) {
                        handler.onPullSummary(success);
                    }
                },
                valueFactory,
                observation);
    }

    private CompletionStage<Void> writeMessage(
            ResponseHandler handler, DiscardMessage discardMessage, BoltExchangeObservation observation) {
        return protocol.discard(
                this.connection,
                discardMessage.qid(),
                discardMessage.number(),
                new MessageHandler<>() {
                    @Override
                    public void onError(Throwable throwable) {
                        updateState(throwable);
                        handler.onError(throwable);
                    }

                    @Override
                    public void onSummary(DiscardSummary summary) {
                        handler.onDiscardSummary(summary);
                    }
                },
                valueFactory,
                observation);
    }

    private CompletionStage<Void> writeMessage(
            ResponseHandler handler, CommitMessage commitMessage, BoltExchangeObservation observation) {
        return protocol.commitTransaction(
                connection,
                new MessageHandler<>() {
                    @Override
                    public void onError(Throwable throwable) {
                        updateState(throwable);
                        handler.onError(throwable);
                    }

                    @Override
                    public void onSummary(String bookmark) {
                        handler.onCommitSummary(() -> Optional.ofNullable(bookmark));
                    }
                },
                observation);
    }

    private CompletionStage<Void> writeMessage(
            ResponseHandler handler, RollbackMessage rollbackMessage, BoltExchangeObservation observation) {
        return protocol.rollbackTransaction(
                connection,
                new MessageHandler<>() {
                    @Override
                    public void onError(Throwable throwable) {
                        updateState(throwable);
                        handler.onError(throwable);
                    }

                    @Override
                    public void onSummary(Void summary) {
                        handler.onRollbackSummary(RollbackSummaryImpl.INSTANCE);
                    }
                },
                observation);
    }

    private CompletionStage<Void> writeMessage(
            ResponseHandler handler, ResetMessage resetMessage, BoltExchangeObservation observation) {
        return protocol.reset(
                connection,
                new MessageHandler<>() {
                    @Override
                    public void onError(Throwable throwable) {
                        updateState(throwable);
                        handler.onError(throwable);
                    }

                    @Override
                    public void onSummary(Void summary) {
                        stateRef.set(BoltConnectionState.OPEN);
                        handler.onResetSummary(null);
                    }
                },
                observation);
    }

    private CompletionStage<Void> writeMessage(
            ResponseHandler handler, LogoffMessage logoffMessage, BoltExchangeObservation observation) {
        return protocol.logoff(
                connection,
                new MessageHandler<>() {
                    @Override
                    public void onError(Throwable throwable) {
                        updateState(throwable);
                        handler.onError(throwable);
                    }

                    @Override
                    public void onSummary(Void summary) {
                        authDataRef.set(new CompletableFuture<>());
                        handler.onLogoffSummary(null);
                    }
                },
                observation);
    }

    private CompletionStage<Void> writeMessage(
            ResponseHandler handler, LogonMessage logonMessage, BoltExchangeObservation observation) {
        return protocol.logon(
                connection,
                logonMessage.authToken().asMap(),
                clock,
                new MessageHandler<>() {
                    @Override
                    public void onError(Throwable throwable) {
                        updateState(throwable);
                        handler.onError(throwable);
                    }

                    @Override
                    public void onSummary(Void summary) {
                        authDataRef.get().complete(new AuthInfoImpl(logonMessage.authToken(), clock.millis()));
                        handler.onLogonSummary(null);
                    }
                },
                valueFactory,
                observation);
    }

    private CompletionStage<Void> writeMessage(
            ResponseHandler handler, TelemetryMessage telemetryMessage, BoltExchangeObservation observation) {
        if (!telemetrySupported()) {
            return CompletableFuture.failedStage(new BoltUnsupportedFeatureException("telemetry not supported"));
        } else {
            return protocol.telemetry(
                    connection,
                    telemetryMessage.api().getValue(),
                    new MessageHandler<>() {
                        @Override
                        public void onError(Throwable throwable) {
                            updateState(throwable);
                            handler.onError(throwable);
                        }

                        @Override
                        public void onSummary(Void summary) {
                            handler.onTelemetrySummary(TelemetrySummaryImpl.INSTANCE);
                        }
                    },
                    observation);
        }
    }

    @Override
    public CompletionStage<Void> forceClose(String reason) {
        if (stateRef.getAndSet(BoltConnectionState.CLOSED) != BoltConnectionState.CLOSED) {
            try {
                return connection.forceClose(reason).exceptionally(ignored -> null);
            } catch (Throwable throwable) {
                return CompletableFuture.completedStage(null);
            }
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public CompletionStage<Void> close() {
        CompletionStage<Void> close;
        try {
            close = switch (stateRef.getAndSet(BoltConnectionState.CLOSED)) {
                case OPEN -> connection.close();
                case ERROR -> connection.forceClose("Closing connection after error");
                case FAILURE -> connection.forceClose("Closing connection after failure");
                case CLOSED -> CompletableFuture.completedStage(null);};
        } catch (Throwable throwable) {
            close = CompletableFuture.completedStage(null);
        }
        return close.exceptionally(ignored -> null);
    }

    @Override
    public CompletionStage<Void> setReadTimeout(Duration duration) {
        return executeInEventLoop(() -> connection.setReadTimeout(duration));
    }

    @Override
    public BoltConnectionState state() {
        var state = stateRef.get();
        if (state == BoltConnectionState.OPEN) {
            if (!connection.isOpen()) {
                state = BoltConnectionState.CLOSED;
            }
        }
        return state;
    }

    @Override
    public CompletionStage<AuthInfo> authInfo() {
        return authDataRef.get();
    }

    @Override
    public String serverAgent() {
        return serverAgent;
    }

    @Override
    public BoltServerAddress serverAddress() {
        return serverAddress;
    }

    @Override
    public BoltProtocolVersion protocolVersion() {
        return protocolVersion;
    }

    @Override
    public boolean telemetrySupported() {
        return telemetrySupported;
    }

    @Override
    public boolean serverSideRoutingEnabled() {
        return serverSideRouting;
    }

    @Override
    public Optional<Duration> defaultReadTimeout() {
        return connection.defaultReadTimeoutMillis();
    }

    private <T> CompletionStage<T> executeInEventLoop(Runnable runnable) {
        return executeInEventLoop(() -> {
            runnable.run();
            return null;
        });
    }

    private <T> CompletionStage<T> executeInEventLoop(Supplier<T> supplier) {
        var executeFuture = new CompletableFuture<T>();
        Runnable stageCompletingRunnable = () -> {
            try {
                executeFuture.complete(supplier.get());
            } catch (Throwable throwable) {
                executeFuture.completeExceptionally(throwable);
            }
        };
        if (eventLoop.inEventLoop()) {
            stageCompletingRunnable.run();
        } else {
            try {
                eventLoop.execute(stageCompletingRunnable);
            } catch (Throwable throwable) {
                executeFuture.completeExceptionally(throwable);
            }
        }
        return executeFuture;
    }

    private void updateState(Throwable throwable) {
        if (throwable instanceof BoltServiceUnavailableException) {
            if (throwable instanceof BoltConnectionReadTimeoutException) {
                stateRef.compareAndExchange(BoltConnectionState.OPEN, BoltConnectionState.ERROR);
            } else {
                stateRef.set(BoltConnectionState.CLOSED);
            }
        } else if (throwable instanceof BoltFailureException boltFailureException) {
            if ("Neo.ClientError.Security.AuthorizationExpired".equals(boltFailureException.code())) {
                stateRef.compareAndExchange(BoltConnectionState.OPEN, BoltConnectionState.ERROR);
            } else {
                stateRef.compareAndExchange(BoltConnectionState.OPEN, BoltConnectionState.FAILURE);
            }
        } else if (throwable instanceof MessageIgnoredException) {
            stateRef.compareAndExchange(BoltConnectionState.OPEN, BoltConnectionState.FAILURE);
        } else if (throwable instanceof EncoderException) {
            stateRef.compareAndExchange(BoltConnectionState.OPEN, BoltConnectionState.ERROR);
        } else {
            stateRef.updateAndGet(state -> switch (state) {
                case OPEN, FAILURE, ERROR -> BoltConnectionState.ERROR;
                case CLOSED -> BoltConnectionState.CLOSED;
            });
        }
    }

    private record AuthInfoImpl(AuthToken authToken, long authAckMillis) implements AuthInfo {}

    private static class ResponseHandleImpl implements ResponseHandler {
        private final ResponseHandler delegate;
        private final CompletableFuture<Void> summariesFuture = new CompletableFuture<>();
        private int expectedSummaries;
        private final Observation observation;

        private ResponseHandleImpl(ResponseHandler delegate, int expectedSummaries, Observation observation) {
            this.delegate = Objects.requireNonNull(delegate);
            this.expectedSummaries = expectedSummaries;
            this.observation = Objects.requireNonNull(observation);

            summariesFuture.whenComplete((ignored1, ignored2) -> onComplete());
        }

        @Override
        public void onError(Throwable throwable) {
            if (!(throwable instanceof MessageIgnoredException)) {
                if (!summariesFuture.isDone()) {
                    observation.error(throwable);
                    runIgnoringError(() -> delegate.onError(throwable));
                    if (!(throwable instanceof BoltException)
                            || throwable instanceof BoltServiceUnavailableException
                            || throwable instanceof BoltProtocolException) {
                        // assume unrecoverable error, ensure onComplete
                        expectedSummaries = 1;
                    }
                    handleSummary();
                }
            } else {
                onIgnored();
            }
        }

        @Override
        public void onBeginSummary(BeginSummary summary) {
            if (!summariesFuture.isDone()) {
                runIgnoringError(() -> delegate.onBeginSummary(summary));
                handleSummary();
            }
        }

        @Override
        public void onRunSummary(RunSummary summary) {
            if (!summariesFuture.isDone()) {
                runIgnoringError(() -> delegate.onRunSummary(summary));
                handleSummary();
            }
        }

        @Override
        public void onRecord(List<Value> fields) {
            if (!summariesFuture.isDone()) {
                runIgnoringError(() -> delegate.onRecord(fields));
            }
        }

        @Override
        public void onPullSummary(PullSummary summary) {
            if (!summariesFuture.isDone()) {
                runIgnoringError(() -> delegate.onPullSummary(summary));
                handleSummary();
            }
        }

        @Override
        public void onDiscardSummary(DiscardSummary summary) {
            if (!summariesFuture.isDone()) {
                runIgnoringError(() -> delegate.onDiscardSummary(summary));
                handleSummary();
            }
        }

        @Override
        public void onCommitSummary(CommitSummary summary) {
            if (!summariesFuture.isDone()) {
                runIgnoringError(() -> delegate.onCommitSummary(summary));
                handleSummary();
            }
        }

        @Override
        public void onRollbackSummary(RollbackSummary summary) {
            if (!summariesFuture.isDone()) {
                runIgnoringError(() -> delegate.onRollbackSummary(summary));
                handleSummary();
            }
        }

        @Override
        public void onResetSummary(ResetSummary summary) {
            if (!summariesFuture.isDone()) {
                runIgnoringError(() -> delegate.onResetSummary(summary));
                handleSummary();
            }
        }

        @Override
        public void onRouteSummary(RouteSummary summary) {
            if (!summariesFuture.isDone()) {
                runIgnoringError(() -> delegate.onRouteSummary(summary));
                handleSummary();
            }
        }

        @Override
        public void onLogoffSummary(LogoffSummary summary) {
            if (!summariesFuture.isDone()) {
                runIgnoringError(() -> delegate.onLogoffSummary(summary));
                handleSummary();
            }
        }

        @Override
        public void onLogonSummary(LogonSummary summary) {
            if (!summariesFuture.isDone()) {
                runIgnoringError(() -> delegate.onLogonSummary(summary));
                handleSummary();
            }
        }

        @Override
        public void onTelemetrySummary(TelemetrySummary summary) {
            if (!summariesFuture.isDone()) {
                runIgnoringError(() -> delegate.onTelemetrySummary(summary));
                handleSummary();
            }
        }

        @Override
        public void onIgnored() {
            if (!summariesFuture.isDone()) {
                runIgnoringError(delegate::onIgnored);
                handleSummary();
            }
        }

        @Override
        public void onComplete() {
            observation.stop();
            runIgnoringError(delegate::onComplete);
        }

        private void handleSummary() {
            expectedSummaries--;
            if (expectedSummaries == 0) {
                summariesFuture.complete(null);
            }
        }

        private void runIgnoringError(Runnable runnable) {
            try {
                runnable.run();
            } catch (Throwable ignored) {
            }
        }
    }

    private static class TelemetrySummaryImpl implements TelemetrySummary {
        private static final TelemetrySummary INSTANCE = new TelemetrySummaryImpl();
    }

    private static class RollbackSummaryImpl implements RollbackSummary {
        private static final RollbackSummary INSTANCE = new RollbackSummaryImpl();
    }
}
