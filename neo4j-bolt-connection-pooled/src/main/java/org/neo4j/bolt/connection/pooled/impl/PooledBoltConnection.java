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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.bolt.connection.AccessMode;
import org.neo4j.bolt.connection.AuthInfo;
import org.neo4j.bolt.connection.AuthToken;
import org.neo4j.bolt.connection.BasicResponseHandler;
import org.neo4j.bolt.connection.BoltConnection;
import org.neo4j.bolt.connection.BoltConnectionState;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.DatabaseName;
import org.neo4j.bolt.connection.NotificationConfig;
import org.neo4j.bolt.connection.ResponseHandler;
import org.neo4j.bolt.connection.TelemetryApi;
import org.neo4j.bolt.connection.TransactionType;
import org.neo4j.bolt.connection.exception.BoltFailureException;
import org.neo4j.bolt.connection.pooled.PooledBoltConnectionProvider;
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
    private final PooledBoltConnectionProvider provider;
    private final Runnable releaseRunnable;
    private final Runnable purgeRunnable;
    private CompletableFuture<Void> closeFuture;

    public PooledBoltConnection(
            BoltConnection delegate,
            PooledBoltConnectionProvider provider,
            Runnable releaseRunnable,
            Runnable purgeRunnable) {
        this.delegate = Objects.requireNonNull(delegate);
        this.provider = Objects.requireNonNull(provider);
        this.releaseRunnable = Objects.requireNonNull(releaseRunnable);
        this.purgeRunnable = Objects.requireNonNull(purgeRunnable);
    }

    @Override
    public CompletionStage<BoltConnection> onLoop() {
        return delegate.onLoop();
    }

    @Override
    public CompletionStage<BoltConnection> route(
            DatabaseName databaseName, String impersonatedUser, Set<String> bookmarks) {
        return delegate.route(databaseName, impersonatedUser, bookmarks).thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> beginTransaction(
            DatabaseName databaseName,
            AccessMode accessMode,
            String impersonatedUser,
            Set<String> bookmarks,
            TransactionType transactionType,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            String txType,
            NotificationConfig notificationConfig) {
        return delegate.beginTransaction(
                        databaseName,
                        accessMode,
                        impersonatedUser,
                        bookmarks,
                        transactionType,
                        txTimeout,
                        txMetadata,
                        txType,
                        notificationConfig)
                .thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> runInAutoCommitTransaction(
            DatabaseName databaseName,
            AccessMode accessMode,
            String impersonatedUser,
            Set<String> bookmarks,
            String query,
            Map<String, Value> parameters,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            NotificationConfig notificationConfig) {
        return delegate.runInAutoCommitTransaction(
                        databaseName,
                        accessMode,
                        impersonatedUser,
                        bookmarks,
                        query,
                        parameters,
                        txTimeout,
                        txMetadata,
                        notificationConfig)
                .thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> run(String query, Map<String, Value> parameters) {
        return delegate.run(query, parameters).thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> pull(long qid, long request) {
        return delegate.pull(qid, request).thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> discard(long qid, long number) {
        return delegate.discard(qid, number).thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> commit() {
        return delegate.commit().thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> rollback() {
        return delegate.rollback().thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> reset() {
        return delegate.reset().thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> logoff() {
        return delegate.logoff().thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> logon(AuthToken authToken) {
        return delegate.logon(authToken).thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> telemetry(TelemetryApi telemetryApi) {
        return delegate.telemetry(telemetryApi).thenApply(ignored -> this);
    }

    @Override
    public CompletionStage<BoltConnection> clear() {
        return delegate.clear();
    }

    @Override
    public CompletionStage<Void> flush(ResponseHandler handler) {
        return delegate.flush(new ResponseHandler() {

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
                })
                .whenComplete((ignored, throwable) -> {
                    if (throwable != null) {
                        if (delegate.state() == BoltConnectionState.CLOSED) {
                            purgeRunnable.run();
                        }
                    }
                });
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
            if (delegate.state() == BoltConnectionState.CLOSED) {
                purgeRunnable.run();
                closeFuture.complete(null);
                return closeFuture;
            } else if (delegate.state() == BoltConnectionState.ERROR) {
                purgeRunnable.run();
                closeFuture.complete(null);
                return closeFuture;
            }

            var resetHandler = new BasicResponseHandler();
            delegate.reset()
                    .thenCompose(boltConnection -> boltConnection.flush(resetHandler))
                    .thenCompose(ignored -> resetHandler.summaries())
                    .whenComplete((ignored, throwable) -> {
                        if (throwable != null) {
                            delegate.close().whenComplete((closeResult, closeThrowable) -> purgeRunnable.run());
                        } else {
                            CompletableFuture.<Void>completedStage(null)
                                    .whenComplete((ignoredResult, nothing) -> releaseRunnable.run());
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
}
