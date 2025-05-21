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
package org.neo4j.bolt.connection.routed.impl;

import static java.lang.String.format;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.neo4j.bolt.connection.AccessMode;
import org.neo4j.bolt.connection.AuthInfo;
import org.neo4j.bolt.connection.BoltConnection;
import org.neo4j.bolt.connection.BoltConnectionState;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.ResponseHandler;
import org.neo4j.bolt.connection.exception.BoltFailureException;
import org.neo4j.bolt.connection.exception.BoltServiceUnavailableException;
import org.neo4j.bolt.connection.message.Message;
import org.neo4j.bolt.connection.routed.RoutedBoltConnectionSource;
import org.neo4j.bolt.connection.routed.impl.cluster.RoutingTableHandler;
import org.neo4j.bolt.connection.routed.impl.util.FutureUtil;
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

public class RoutedBoltConnection implements BoltConnection {
    private final BoltConnection delegate;
    private final RoutingTableHandler routingTableHandler;
    private final AccessMode accessMode;
    private final RoutedBoltConnectionSource source;

    public RoutedBoltConnection(
            BoltConnection delegate,
            RoutingTableHandler routingTableHandler,
            AccessMode accessMode,
            RoutedBoltConnectionSource source) {
        this.delegate = Objects.requireNonNull(delegate);
        this.routingTableHandler = Objects.requireNonNull(routingTableHandler);
        this.accessMode = Objects.requireNonNull(accessMode);
        this.source = Objects.requireNonNull(source);
    }

    @Override
    public CompletionStage<Void> writeAndFlush(ResponseHandler handler, List<Message> messages) {
        return delegate.writeAndFlush(
                new RoutedResponseHandler(routingTableHandler, handler, accessMode, serverAddress()), messages);
    }

    @Override
    public CompletionStage<Void> write(List<Message> messages) {
        return delegate.write(messages);
    }

    @Override
    public CompletionStage<Void> forceClose(String reason) {
        return delegate.forceClose(reason);
    }

    @Override
    public CompletionStage<Void> close() {
        source.decrementInUseCount(serverAddress());
        return delegate.close();
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

    private static class RoutedResponseHandler implements ResponseHandler {
        private final RoutingTableHandler routingTableHandler;
        private final ResponseHandler handler;
        private final AccessMode accessMode;
        private final BoltServerAddress serverAddress;
        private boolean notifyHandler = true;

        private RoutedResponseHandler(
                RoutingTableHandler routingTableHandler,
                ResponseHandler handler,
                AccessMode accessMode,
                BoltServerAddress serverAddress) {
            this.routingTableHandler = routingTableHandler;
            this.handler = handler;
            this.accessMode = accessMode;
            this.serverAddress = serverAddress;
        }

        @Override
        public void onError(Throwable throwable) {
            handler.onError(handledError(throwable, notifyHandler));
            notifyHandler = false;
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
        public void onRecord(Value[] fields) {
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

        private Throwable handledError(Throwable receivedError, boolean notifyHandler) {
            var error = FutureUtil.completionExceptionCause(receivedError);

            if (error instanceof BoltServiceUnavailableException boltServiceUnavailableException) {
                return handledServiceUnavailableException(boltServiceUnavailableException, notifyHandler);
            } else if (error instanceof BoltFailureException boltFailureException) {
                return handledBoltFailureException(boltFailureException, notifyHandler);
            } else {
                return error;
            }
        }

        private Throwable handledServiceUnavailableException(BoltServiceUnavailableException e, boolean notifyHandler) {
            if (notifyHandler) {
                routingTableHandler.onConnectionFailure(serverAddress);
            }
            return new BoltServiceUnavailableException(format("Server at %s is no longer available", serverAddress), e);
        }

        private Throwable handledBoltFailureException(BoltFailureException e, boolean notifyHandler) {
            var errorCode = e.code();
            if (Objects.equals(errorCode, "Neo.TransientError.General.DatabaseUnavailable")) {
                if (notifyHandler) {
                    routingTableHandler.onConnectionFailure(serverAddress);
                }
            } else if (isFailureToWrite(errorCode)) {
                // The server is unaware of the session mode, so we have to implement this logic in the driver.
                // In the future, we might be able to move this logic to the server.
                switch (accessMode) {
                    case READ -> {}
                    case WRITE -> {
                        if (notifyHandler) {
                            routingTableHandler.onWriteFailure(serverAddress);
                        }
                    }
                }
            }
            return e;
        }

        private static boolean isFailureToWrite(String errorCode) {
            return Objects.equals(errorCode, "Neo.ClientError.Cluster.NotALeader")
                    || Objects.equals(errorCode, "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase");
        }
    }
}
