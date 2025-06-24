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
package org.neo4j.bolt.connection.pooled.impl;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.neo4j.bolt.connection.AuthInfo;
import org.neo4j.bolt.connection.BasicResponseHandler;
import org.neo4j.bolt.connection.BoltConnection;
import org.neo4j.bolt.connection.BoltConnectionState;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.ResponseHandler;
import org.neo4j.bolt.connection.exception.BoltFailureException;
import org.neo4j.bolt.connection.message.Message;
import org.neo4j.bolt.connection.message.Messages;
import org.neo4j.bolt.connection.observation.ImmutableObservation;
import org.neo4j.bolt.connection.observation.Observation;
import org.neo4j.bolt.connection.pooled.PooledBoltConnectionSource;
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

public class PooledBoltConnection implements BoltConnection {
    private final BoltConnection delegate;
    private final PooledBoltConnectionSource source;
    private final Runnable releaseRunnable;
    private final Runnable purgeRunnable;
    private final Function<ImmutableObservation, Observation> inUseObservationFunction;
    private volatile Observation inUseObservation;
    private CompletableFuture<Void> closeFuture;

    public PooledBoltConnection(
            BoltConnection delegate,
            PooledBoltConnectionSource source,
            Runnable releaseRunnable,
            Runnable purgeRunnable,
            Function<ImmutableObservation, Observation> inUseObservationFunction) {
        this.delegate = Objects.requireNonNull(delegate);
        this.source = Objects.requireNonNull(source);
        this.releaseRunnable = Objects.requireNonNull(releaseRunnable);
        this.purgeRunnable = Objects.requireNonNull(purgeRunnable);
        this.inUseObservationFunction = Objects.requireNonNull(inUseObservationFunction);
    }

    @Override
    public CompletionStage<Void> writeAndFlush(
            ResponseHandler handler, List<Message> messages, ImmutableObservation parentObservation) {
        return delegate.writeAndFlush(new PooledResponseHandler(source, handler), messages, parentObservation)
                .whenComplete((ignored, throwable) -> {
                    if (throwable != null) {
                        if (delegate.state() == BoltConnectionState.CLOSED) {
                            purgeRunnable.run();
                        }
                    }
                });
    }

    @Override
    public CompletionStage<Void> write(List<Message> messages) {
        return delegate.write(messages);
    }

    @Override
    public CompletionStage<Void> forceClose(String reason) {
        return delegate.forceClose(reason).whenComplete((closeResult, closeThrowable) -> purgeRunnable.run());
    }

    @Override
    public CompletionStage<Void> close() {
        CompletableFuture<Void> closeFuture;
        var close = false;
        synchronized (this) {
            if (this.closeFuture == null) {
                this.closeFuture = new CompletableFuture<>();
                close = true;
            }
            closeFuture = this.closeFuture;
        }

        if (close) {
            var inUseObservation = this.inUseObservation;
            if (delegate.state() == BoltConnectionState.CLOSED) {
                inUseObservation.stop();
                purgeRunnable.run();
                closeFuture.complete(null);
                return closeFuture;
            } else if (delegate.state() == BoltConnectionState.ERROR) {
                inUseObservation.stop();
                purgeRunnable.run();
                closeFuture.complete(null);
                return closeFuture;
            }

            var resetHandler = new BasicResponseHandler();
            delegate.writeAndFlush(resetHandler, Messages.reset(), inUseObservation)
                    .thenCompose(ignored -> resetHandler.summaries())
                    .whenComplete((ignored, throwable) -> {
                        inUseObservation.stop();
                        if (throwable != null) {
                            purgeRunnable.run();
                        } else {
                            releaseRunnable.run();
                        }
                        closeFuture.complete(null);
                    });
        }

        return closeFuture;
    }

    @Override
    public CompletionStage<Void> setReadTimeout(Duration duration) {
        return delegate.setReadTimeout(duration);
    }

    @Override
    public BoltConnectionState state() {
        return delegate.state();
    }

    @Override
    public CompletionStage<AuthInfo> authInfo() {
        return delegate.authInfo();
    }

    @Override
    public String serverAgent() {
        return delegate.serverAgent();
    }

    @Override
    public BoltServerAddress serverAddress() {
        return delegate.serverAddress();
    }

    @Override
    public BoltProtocolVersion protocolVersion() {
        return delegate.protocolVersion();
    }

    @Override
    public boolean telemetrySupported() {
        return delegate.telemetrySupported();
    }

    @Override
    public boolean serverSideRoutingEnabled() {
        return delegate.serverSideRoutingEnabled();
    }

    @Override
    public Optional<Duration> defaultReadTimeout() {
        return delegate.defaultReadTimeout();
    }

    // internal use only
    public BoltConnection delegate() {
        return delegate;
    }

    public void onUsageStart(ImmutableObservation observationParent) {
        inUseObservation = inUseObservationFunction.apply(observationParent);
    }

    private record PooledResponseHandler(PooledBoltConnectionSource provider, ResponseHandler handler)
            implements ResponseHandler {

        @Override
        public void onError(Throwable throwable) {
            if (throwable instanceof BoltFailureException boltFailureException) {
                if ("Neo.ClientError.Security.AuthorizationExpired".equals(boltFailureException.code())) {
                    provider.onExpired();
                }
            }
            handler.onError(throwable);
        }

        @Override
        public void onBeginSummary(BeginSummary summary) {
            handler.onBeginSummary(summary);
        }

        @Override
        public void onRunSummary(RunSummary summary) {
            handler.onRunSummary(summary);
        }

        @Override
        public void onRecord(List<Value> fields) {
            handler.onRecord(fields);
        }

        @Override
        public void onPullSummary(PullSummary summary) {
            handler.onPullSummary(summary);
        }

        @Override
        public void onDiscardSummary(DiscardSummary summary) {
            handler.onDiscardSummary(summary);
        }

        @Override
        public void onCommitSummary(CommitSummary summary) {
            handler.onCommitSummary(summary);
        }

        @Override
        public void onRollbackSummary(RollbackSummary summary) {
            handler.onRollbackSummary(summary);
        }

        @Override
        public void onResetSummary(ResetSummary summary) {
            handler.onResetSummary(summary);
        }

        @Override
        public void onRouteSummary(RouteSummary summary) {
            handler.onRouteSummary(summary);
        }

        @Override
        public void onLogoffSummary(LogoffSummary summary) {
            handler.onLogoffSummary(summary);
        }

        @Override
        public void onLogonSummary(LogonSummary summary) {
            handler.onLogonSummary(summary);
        }

        @Override
        public void onTelemetrySummary(TelemetrySummary summary) {
            handler.onTelemetrySummary(summary);
        }

        @Override
        public void onIgnored() {
            handler.onIgnored();
        }

        @Override
        public void onComplete() {
            handler.onComplete();
        }
    }
}
