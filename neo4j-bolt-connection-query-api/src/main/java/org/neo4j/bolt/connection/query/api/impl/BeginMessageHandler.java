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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.Objects;
import org.neo4j.bolt.connection.AccessMode;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.ResponseHandler;
import org.neo4j.bolt.connection.TransactionType;
import org.neo4j.bolt.connection.exception.BoltClientException;
import org.neo4j.bolt.connection.message.BeginMessage;
import org.neo4j.bolt.connection.values.ValueFactory;

final class BeginMessageHandler extends AbstractMessageHandler<TransactionInfo> {
    private final System.Logger log;
    private final ResponseHandler handler;
    private final HttpContext httpContext;
    private final HttpRequest.BodyPublisher bodyPublisher;
    private final String databaseName;

    BeginMessageHandler(
            ResponseHandler handler,
            HttpContext httpContext,
            BeginMessage message,
            ValueFactory valueFactory,
            LoggingProvider logging) {
        super(httpContext, handler, valueFactory, logging);
        this.log = logging.getLog(getClass());
        this.handler = Objects.requireNonNull(handler);
        this.httpContext = Objects.requireNonNull(httpContext);

        if (message.databaseName().isPresent()) {
            this.databaseName = message.databaseName().get();
        } else {
            throw new BoltClientException("Database name must be specified");
        }

        if (message.transactionType() != TransactionType.DEFAULT) {
            throw new BoltClientException("Only TransactionType.DEFAULT is supported");
        }
        this.bodyPublisher = newHttpRequestBodyPublisher(httpContext, message, this.databaseName);
    }

    @Override
    protected HttpRequest newHttpRequest() {
        var runUri = httpContext.baseUri().resolve("db/%s/query/v2/tx".formatted(databaseName));
        return HttpRequest.newBuilder(runUri)
                .header("Content-Type", "application/vnd.neo4j.query")
                .header("Accept", "application/vnd.neo4j.query")
                .header("Authorization", httpContext.authHeader())
                .POST(bodyPublisher)
                .build();
    }

    @Override
    protected TransactionInfo handleResponse(HttpResponse<String> response) {
        var transactionEntry = httpContext.gson().fromJson(response.body(), TransactionEntry.class);
        var affinity = response.headers().firstValue("neo4j-cluster-affinity").orElse(null);
        var info = new TransactionInfo(
                databaseName,
                transactionEntry.transaction().id(),
                Instant.parse(transactionEntry.transaction().expires()),
                affinity);
        handler.onBeginSummary(new BeginSummaryImpl(databaseName));
        return info;
    }

    private static HttpRequest.BodyPublisher newHttpRequestBodyPublisher(
            HttpContext httpContext, BeginMessage message, String databaseName) {
        var jsonObject = new JsonObject();

        if (message.accessMode() == AccessMode.READ) {
            jsonObject.addProperty("accessMode", "Read");
        }
        message.impersonatedUser()
                .ifPresent(impersonatedUser -> jsonObject.addProperty("impersonatedUser", impersonatedUser));
        if (!message.bookmarks().isEmpty()) {
            var jsonArray = new JsonArray();
            message.bookmarks().forEach(jsonArray::add);
            jsonObject.add("bookmarks", jsonArray);
        }

        var jsonBody = httpContext.gson().toJson(jsonObject);
        return HttpRequest.BodyPublishers.ofString(jsonBody);
    }

    record TransactionEntry(Transaction transaction) {}

    record Transaction(String id, String expires) {}
}
