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
package org.neo4j.bolt.connection.query.api.impl;

import static org.neo4j.bolt.connection.query.api.impl.FutureUtil.completionExceptionCause;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;
import org.neo4j.bolt.connection.AuthInfo;
import org.neo4j.bolt.connection.AuthToken;
import org.neo4j.bolt.connection.BoltConnection;
import org.neo4j.bolt.connection.BoltConnectionState;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.ResponseHandler;
import org.neo4j.bolt.connection.exception.BoltClientException;
import org.neo4j.bolt.connection.exception.BoltConnectionReadTimeoutException;
import org.neo4j.bolt.connection.exception.BoltException;
import org.neo4j.bolt.connection.exception.BoltFailureException;
import org.neo4j.bolt.connection.exception.BoltServiceUnavailableException;
import org.neo4j.bolt.connection.message.BeginMessage;
import org.neo4j.bolt.connection.message.CommitMessage;
import org.neo4j.bolt.connection.message.DiscardMessage;
import org.neo4j.bolt.connection.message.Message;
import org.neo4j.bolt.connection.message.PullMessage;
import org.neo4j.bolt.connection.message.RollbackMessage;
import org.neo4j.bolt.connection.message.RunMessage;
import org.neo4j.bolt.connection.values.ValueFactory;

public final class QueryApiBoltConnection implements BoltConnection {
    private static final Gson GSON =
            new GsonBuilder().disableHtmlEscaping().serializeNulls().create();
    private final LoggingProvider logging;
    private final System.Logger log;
    private final ValueFactory valueFactory;
    private final HttpClient httpClient;
    private final URI baseUri;
    private final String authHeader;
    private final AuthInfo authInfo;
    private final String serverAgent;

    // synchronized
    private final List<Message> messages = new ArrayList<>();
    private final Map<Long, Query> qidToQuery = new HashMap<>();
    private TransactionInfo transactionInfo;
    private CompletionStage<Void> execTail = CompletableFuture.completedFuture(null);
    private BoltConnectionState state = BoltConnectionState.OPEN;

    public QueryApiBoltConnection(
            ValueFactory valueFactory,
            HttpClient httpClient,
            URI baseUri,
            AuthToken authToken,
            String serverAgent,
            LoggingProvider logging) {
        this.logging = logging;
        this.log = logging.getLog(getClass());
        this.valueFactory = Objects.requireNonNull(valueFactory);
        this.httpClient = Objects.requireNonNull(httpClient);
        this.baseUri = Objects.requireNonNull(baseUri);
        this.authInfo = new AuthInfoImpl(authToken, Clock.systemUTC().millis());
        var authMap = authToken.asMap();
        var username = authMap.get("principal").asString();
        var password = authMap.get("credentials").asString();
        this.authHeader = "Basic "
                + Base64.getEncoder()
                        .encodeToString("%s:%s".formatted(username, password).getBytes());
        this.serverAgent = Objects.requireNonNull(serverAgent);
    }

    @Override
    public CompletionStage<Void> writeAndFlush(ResponseHandler handler, List<Message> messages) {
        try {
            synchronized (this) {
                List<Message> messagesToWrite = new ArrayList<>(this.messages);
                this.messages.clear();
                messagesToWrite.addAll(messages);

                var messageHandlers = initMessageHandlers(handler, messagesToWrite);
                var messageHandling = setupMessageHandling(handler, messageHandlers, execTail);
                execTail = execTail.thenCompose(ignored -> messageHandling);
            }
            return CompletableFuture.completedStage(null);
        } catch (Throwable t) {
            return CompletableFuture.failedStage(t);
        }
    }

    @Override
    public CompletionStage<Void> write(List<Message> messages) {
        try {
            synchronized (this) {
                this.messages.addAll(messages);
            }
            return CompletableFuture.completedFuture(null);
        } catch (Throwable throwable) {
            return CompletableFuture.failedStage(throwable);
        }
    }

    @Override
    public CompletionStage<Void> forceClose(String reason) {
        return CompletableFuture.completedStage(null);
    }

    @Override
    public CompletionStage<Void> close() {
        return CompletableFuture.completedStage(null);
    }

    @Override
    public CompletionStage<Void> setReadTimeout(Duration duration) {
        // todo update this
        return CompletableFuture.completedStage(null);
    }

    @Override
    public BoltConnectionState state() {
        synchronized (this) {
            return state;
        }
    }

    @Override
    public CompletionStage<AuthInfo> authInfo() {
        return CompletableFuture.completedStage(authInfo);
    }

    @Override
    public String serverAgent() {
        return serverAgent;
    }

    @Override
    public BoltServerAddress serverAddress() {
        return new BoltServerAddress(baseUri.getHost(), getPort());
    }

    @Override
    public BoltProtocolVersion protocolVersion() {
        // Higher versions of Bolt require GQL support that it not available in Query API.
        return new BoltProtocolVersion(5, 4);
    }

    @Override
    public boolean telemetrySupported() {
        return false;
    }

    @Override
    public boolean serverSideRoutingEnabled() {
        return false;
    }

    @Override
    public Optional<Duration> defaultReadTimeout() {
        return Optional.empty();
    }

    private synchronized void setTransactionInfo(TransactionInfo transactionInfo) {
        this.transactionInfo = transactionInfo;
    }

    private synchronized TransactionInfo getTransactionInfo() {
        return transactionInfo;
    }

    private synchronized Query findById(long id) {
        return qidToQuery.get(id);
    }

    private synchronized void deleteById(long id) {
        qidToQuery.remove(id);
    }

    private synchronized List<MessageHandler<?>> initMessageHandlers(ResponseHandler handler, List<Message> messages) {
        var messageHandlers = new ArrayList<MessageHandler<?>>(messages.size());
        log.log(System.Logger.Level.DEBUG, "Initializing message handlers %s".formatted(messages));
        for (var message : messages) {
            if (message instanceof BeginMessage beginMessage) {
                var httpContext = new HttpContext(httpClient, baseUri, GSON, authHeader);
                messageHandlers.add(new BeginMessageHandler(handler, httpContext, beginMessage, valueFactory, logging));
            } else if (message instanceof RunMessage runMessage) {
                var httpContext = new HttpContext(httpClient, baseUri, GSON, authHeader);
                messageHandlers.add(new RunMessageHandler(
                        handler, httpContext, valueFactory, runMessage, this::getTransactionInfo, logging));
            } else if (message instanceof PullMessage pullMessage) {
                if (pullMessage.qid() != -1 && !qidToQuery.containsKey(pullMessage.qid())) {
                    throw new BoltClientException("Pull query does not contain query id: " + pullMessage.qid());
                }
                messageHandlers.add(
                        new PullMessageHandler(handler, pullMessage, this::findById, this::deleteById, logging));
            } else if (message instanceof DiscardMessage discardMessage) {
                if (discardMessage.qid() != -1 && !qidToQuery.containsKey(discardMessage.qid())) {
                    throw new BoltClientException("Discard query does not contain query id: " + discardMessage.qid());
                }
                messageHandlers.add(
                        new DiscardMessageHandler(handler, discardMessage, this::findById, this::deleteById, logging));
            } else if (message instanceof CommitMessage) {
                var httpContext = new HttpContext(httpClient, baseUri, GSON, authHeader);
                messageHandlers.add(new CommitMessageHandler(
                        handler, httpContext, valueFactory, this::getTransactionInfo, logging));
            } else if (message instanceof RollbackMessage) {
                var httpContext = new HttpContext(httpClient, baseUri, GSON, authHeader);
                messageHandlers.add(new RollbackMessageHandler(
                        handler, httpContext, this::getTransactionInfo, valueFactory, logging));
            } else {
                throw new BoltException(
                        String.format("%s not supported", message.getClass().getCanonicalName()));
            }
        }
        return messageHandlers;
    }

    private synchronized CompletionStage<Void> setupMessageHandling(
            ResponseHandler handler, List<MessageHandler<?>> messageHandlers, CompletionStage<Void> previousExecution) {
        var messageHandlingFuture = new CompletableFuture<Void>();
        var exchange = previousExecution.whenComplete((result, throwable) -> {});
        for (var messageHandler : messageHandlers) {
            CompletionStage<Void> handlerStage;
            if (messageHandler instanceof BeginMessageHandler beginMessageHandler) {
                exchange = appendMessageHandler(
                        handler, exchange, () -> beginMessageHandler.exchange().thenApply(transactionInfo -> {
                            setTransactionInfo(transactionInfo);
                            return null;
                        }));
            } else if (messageHandler instanceof RunMessageHandler runMessageHandler) {
                exchange = appendMessageHandler(
                        handler, exchange, () -> runMessageHandler.exchange().thenApply(query -> {
                            synchronized (this) {
                                qidToQuery.put(query.id(), query);
                                qidToQuery.put(-1L, query);
                            }
                            return null;
                        }));
            } else if (messageHandler instanceof CommitMessageHandler commitMessageHandler) {
                exchange = appendMessageHandler(
                        handler, exchange, () -> commitMessageHandler.exchange().thenApply(ignored0 -> {
                            synchronized (this) {
                                setTransactionInfo(null);
                                qidToQuery.clear();
                            }
                            return null;
                        }));
            } else if (messageHandler instanceof RollbackMessageHandler rollbackMessageHandler) {
                exchange = appendMessageHandler(handler, exchange, () -> rollbackMessageHandler
                        .exchange()
                        .thenApply(transactionInfo -> {
                            synchronized (this) {
                                setTransactionInfo(null);
                                qidToQuery.clear();
                            }
                            return null;
                        }));
            } else {
                exchange = appendMessageHandler(
                        handler, exchange, () -> messageHandler.exchange().thenApply(ignored0 -> null));
            }
        }
        exchange.whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                throwable = completionExceptionCause(throwable);
                synchronized (this) {
                    BoltConnectionState newState;
                    if (throwable instanceof BoltServiceUnavailableException) {
                        if (throwable instanceof BoltConnectionReadTimeoutException) {
                            newState = BoltConnectionState.ERROR;
                        } else {
                            newState = BoltConnectionState.CLOSED;
                        }
                    } else if (throwable instanceof BoltFailureException boltFailureException) {
                        if ("Neo.ClientError.Security.AuthorizationExpired".equals(boltFailureException.code())) {
                            newState = BoltConnectionState.ERROR;
                        } else {
                            newState = BoltConnectionState.FAILURE;
                        }
                    } else {
                        newState = switch (state) {
                            case OPEN, FAILURE, ERROR -> BoltConnectionState.ERROR;
                            case CLOSED -> BoltConnectionState.CLOSED;};
                    }
                    state = newState;
                }
            }
            handler.onComplete();
            messageHandlingFuture.complete(null);
        });
        return messageHandlingFuture;
    }

    private int getPort() {
        if (baseUri.getPort() == -1) {
            if (baseUri.getScheme().equals("https")) {
                return 443;
            } else {
                return 80;
            }
        } else {
            return baseUri.getPort();
        }
    }

    private static CompletionStage<Void> appendMessageHandler(
            ResponseHandler handler, CompletionStage<Void> previous, Supplier<CompletionStage<Void>> messageSupplier) {
        return previous.handle((ignored, throwable) -> {
                    if (throwable != null) {
                        handler.onIgnored();
                        throwable = completionExceptionCause(throwable);
                        return CompletableFuture.<Void>failedStage(throwable);
                    } else {
                        return messageSupplier.get().whenComplete((handlerResult, handlerError) -> {
                            if (handlerError != null) {
                                handlerError = completionExceptionCause(handlerError);
                                handler.onError(handlerError);
                            }
                        });
                    }
                })
                .thenCompose(Function.identity());
    }
}
