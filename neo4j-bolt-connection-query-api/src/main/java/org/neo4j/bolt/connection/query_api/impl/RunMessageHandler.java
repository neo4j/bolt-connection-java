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

import com.fasterxml.jackson.jr.ob.JSON;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.neo4j.bolt.connection.AccessMode;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.ResponseHandler;
import org.neo4j.bolt.connection.exception.BoltClientException;
import org.neo4j.bolt.connection.message.RunMessage;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.bolt.connection.values.ValueFactory;

final class RunMessageHandler extends AbstractMessageHandler<Query> {
    private final System.Logger log;
    private final ResponseHandler handler;
    private final HttpContext httpContext;
    private final Supplier<String> authHeaderSupplier;
    private final HttpRequest.BodyPublisher bodyPublisher;
    private final ValueFactory valueFactory;
    private final RunMessage message;
    private final Supplier<TransactionInfo> transactionInfoSupplier;
    private final AtomicReference<String> databaseName = new AtomicReference<>();
    private final String defaultDatabase;

    RunMessageHandler(
            ResponseHandler handler,
            HttpContext httpContext,
            Supplier<String> authHeaderSupplier,
            ValueFactory valueFactory,
            RunMessage message,
            Supplier<TransactionInfo> transactionInfoSupplier,
            LoggingProvider logging) {
        super(httpContext, handler, valueFactory, logging);
        this.log = logging.getLog(getClass());
        this.handler = Objects.requireNonNull(handler);
        this.httpContext = Objects.requireNonNull(httpContext);
        this.authHeaderSupplier = Objects.requireNonNull(authHeaderSupplier);
        this.valueFactory = Objects.requireNonNull(valueFactory);
        this.message = Objects.requireNonNull(message);
        this.transactionInfoSupplier = Objects.requireNonNull(transactionInfoSupplier);

        if (message.extra().isPresent()) {
            var extra = message.extra().get();
            if (extra.databaseName().isEmpty() && httpContext.defaultDatabase() == null) {
                throw new BoltClientException("Database name must be specified");
            }
        }
        this.bodyPublisher = newHttpRequestBodyPublisher(httpContext.json(), message);
        this.defaultDatabase = httpContext.defaultDatabase();
    }

    @Override
    protected HttpRequest newHttpRequest() {
        var transactionInfo = transactionInfoSupplier.get();
        String databaseName;
        URI uri;
        var headers = httpContext.headers(authHeaderSupplier.get());
        if (transactionInfo != null) {
            databaseName = transactionInfo.databaseName();
            uri = httpContext.txUrl(transactionInfo);
            if (transactionInfo.affinity() != null) {
                headers = Arrays.copyOf(headers, headers.length + 2);
                headers[headers.length - 2] = "neo4j-cluster-affinity";
                headers[headers.length - 1] = transactionInfo.affinity();
            }
        } else {
            databaseName =
                    message.extra().flatMap(extra -> extra.databaseName()).orElse(defaultDatabase);
            if (databaseName == null) {
                throw new BoltClientException("Database not specified");
            }
            uri = httpContext.queryUrl(databaseName);
        }
        this.databaseName.set(databaseName);
        return HttpRequest.newBuilder(uri).headers(headers).POST(bodyPublisher).build();
    }

    @Override
    protected Query handleResponse(HttpResponse<String> response) {
        QueryResult queryResult = null;
        String body = response.body();
        try {
            log.log(System.Logger.Level.DEBUG, "received body: " + body);
            queryResult = httpContext.json().beanFrom(QueryResult.class, body);
        } catch (IOException e) {
            throw new BoltClientException("Cannot parse response %s to QueryResult".formatted(body), e);
        }
        var id = new Random().nextLong();
        var counters = queryResult.counters();
        var statsMap = Map.ofEntries(
                Map.entry("nodes-created", valueFactory.value(counters.nodesCreated())),
                Map.entry("nodes-deleted", valueFactory.value(counters.nodesDeleted())),
                Map.entry("relationships-created", valueFactory.value(counters.relationshipsCreated())),
                Map.entry("relationships-deleted", valueFactory.value(counters.relationshipsDeleted())),
                Map.entry("properties-set", valueFactory.value(counters.propertiesSet())),
                Map.entry("labels-added", valueFactory.value(counters.labelsAdded())),
                Map.entry("labels-removed", valueFactory.value(counters.labelsRemoved())),
                Map.entry("indexes-added", valueFactory.value(counters.indexesAdded())),
                Map.entry("indexes-removed", valueFactory.value(counters.indexesRemoved())),
                Map.entry("constraints-added", valueFactory.value(counters.constraintsAdded())),
                Map.entry("constraints-removed", valueFactory.value(counters.constraintsRemoved())),
                Map.entry("system-updates", valueFactory.value(counters.systemUpdates())));
        String bookmark = null;
        if (queryResult.bookmarks() != null && !queryResult.bookmarks().isEmpty()) {
            if (queryResult.bookmarks().size() > 1) {
                log.log(System.Logger.Level.WARNING, "Found multiple bookmarks in run request");
                bookmark = queryResult.bookmarks().get(0);
            } else {
                bookmark = queryResult.bookmarks().get(0);
            }
        }
        var databaseName = this.databaseName.get();
        var metadata = new HashMap<String, Value>();
        metadata.put("stats", valueFactory.value(statsMap));
        metadata.put("db", valueFactory.value(databaseName));
        if (bookmark != null) {
            metadata.put("bookmark", valueFactory.value(bookmark));
        }
        var notifications = queryResult.notifications();
        if (notifications != null && !notifications.isEmpty()) {
            metadata.put("notifications", valueFactory.value(notifications));
        }
        var query = new Query(
                id, queryResult.data().fields(), queryResult.data().values(), Collections.unmodifiableMap(metadata));
        handler.onRunSummary(new RunSummaryImpl(query.id(), query.fields(), -1, databaseName));
        return query;
    }

    private HttpRequest.BodyPublisher newHttpRequestBodyPublisher(JSON json, RunMessage message) {
        String statement = message.query();
        Map<String, Value> parameters = null;
        if (!message.parameters().isEmpty()) {
            parameters = message.parameters();
        }

        String accessMode = null;
        String impersonatedUser = null;
        List<String> bookmarks = null;
        if (message.extra().isPresent()) {
            var extra = message.extra().get();
            if (extra.accessMode() == AccessMode.READ) {
                accessMode = "Read";
            }
            impersonatedUser = extra.impersonatedUser().orElseGet(() -> null);
            if (!extra.bookmarks().isEmpty()) {
                bookmarks = new ArrayList<>(extra.bookmarks());
            }
        }

        QueryAPIRequestPayload payload =
                new QueryAPIRequestPayload(statement, parameters, bookmarks, impersonatedUser, accessMode);
        try {
            var jsonBody = json.asString(payload);
            return HttpRequest.BodyPublishers.ofString(jsonBody);
        } catch (IOException e) {
            throw new BoltClientException("Cannot serialize payload %s".formatted(payload), e);
        }
    }

    private static class QueryAPIRequestPayload {

        private final String statement;
        private final Map<String, Value> parameters;
        private final List<String> bookmarks;
        private final String impersonatedUser;
        private final String accessMode;
        private final Boolean includeCounters = true;

        public QueryAPIRequestPayload(
                String statement,
                Map<String, Value> parameters,
                List<String> bookmarks,
                String impersonatedUser,
                String accessMode) {
            this.statement = statement;
            this.parameters = parameters;
            this.bookmarks = bookmarks;
            this.impersonatedUser = impersonatedUser;
            this.accessMode = accessMode;
        }

        public Map<String, Value> getParameters() {
            return parameters;
        }

        public String getStatement() {
            return statement;
        }

        public List<String> getBookmarks() {
            return bookmarks;
        }

        public String getAccessMode() {
            return accessMode;
        }

        public Boolean getIncludeCounters() {
            return includeCounters;
        }

        public String getImpersonatedUser() {
            return impersonatedUser;
        }
    }
}
