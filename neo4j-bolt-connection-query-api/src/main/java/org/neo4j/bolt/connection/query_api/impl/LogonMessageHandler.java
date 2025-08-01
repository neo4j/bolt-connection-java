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
package org.neo4j.bolt.connection.query_api.impl;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import org.neo4j.bolt.connection.AuthToken;
import org.neo4j.bolt.connection.ResponseHandler;
import org.neo4j.bolt.connection.message.LogonMessage;

final class LogonMessageHandler implements MessageHandler<Void> {
    private final ResponseHandler handler;
    private final LogonMessage message;
    private final Consumer<AuthToken> authTokenHandler;

    LogonMessageHandler(ResponseHandler handler, LogonMessage message, Consumer<AuthToken> authTokenHandler) {
        this.handler = Objects.requireNonNull(handler);
        this.message = Objects.requireNonNull(message);
        this.authTokenHandler = Objects.requireNonNull(authTokenHandler);
    }

    @Override
    public CompletionStage<Void> exchange() {
        return CompletableFuture.completedStage(null).thenApply(ignored -> {
            authTokenHandler.accept(message.authToken());
            handler.onLogonSummary(LogonSummaryImpl.INSTANCE);
            return null;
        });
    }
}
