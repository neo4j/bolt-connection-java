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

import java.io.IOException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.neo4j.bolt.connection.AccessMode;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.ResponseHandler;
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
        } else if (httpContext.defaultDatabase() != null) {
            this.databaseName = httpContext.defaultDatabase();
        } else {
            throw new BoltClientException("Database name must be specified");
        }

        try {
            this.bodyPublisher = newHttpRequestBodyPublisher(httpContext, message, this.databaseName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected HttpRequest newHttpRequest() {
        return HttpRequest.newBuilder(httpContext.txUrl(databaseName))
                .headers(httpContext.headers())
                .POST(bodyPublisher)
                .build();
    }

    @Override
    protected TransactionInfo handleResponse(HttpResponse<String> response) {
        try {
            var transactionEntry = httpContext.json().beanFrom(TransactionEntry.class, response.body());
            var affinity =
                    response.headers().firstValue("neo4j-cluster-affinity").orElse(null);
            var info = new TransactionInfo(
                    databaseName,
                    transactionEntry.transaction().id(),
                    Instant.parse(transactionEntry.transaction().expires()),
                    affinity);
            handler.onBeginSummary(new BeginSummaryImpl(databaseName));
            return info;
        } catch (IOException e) {
            throw new BoltClientException("Cannot parse %s to TransactionEntry".formatted(response.body()), e);
        }
    }

    private static HttpRequest.BodyPublisher newHttpRequestBodyPublisher(
            HttpContext httpContext, BeginMessage message, String databaseName) throws IOException {

        var accessMode = message.accessMode() == AccessMode.READ ? "Read" : null;
        var impersonatedUser = message.impersonatedUser().orElse(null);
        List<String> bookmarks = null;

        if (!message.bookmarks().isEmpty()) {
            bookmarks = new ArrayList<>(message.bookmarks());
        }

        var txType = message.transactionType() != null
                ? switch (message.transactionType()) {
                    case UNCONSTRAINED -> "IMPLICIT";
                    case DEFAULT -> null;
                }
                : null;

        var jsonBody =
                httpContext.json().asString(new BeginMessageWrapper(accessMode, impersonatedUser, bookmarks, txType));
        return HttpRequest.BodyPublishers.ofString(jsonBody);
    }

    record TransactionEntry(Transaction transaction) {}

    record Transaction(String id, String expires) {}

    record BeginMessageWrapper(String accessMode, String impersonatedUser, List<String> bookmarks, String txType) {}
}
