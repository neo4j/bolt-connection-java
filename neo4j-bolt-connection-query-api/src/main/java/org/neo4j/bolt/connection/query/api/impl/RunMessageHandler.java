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

import static org.neo4j.bolt.connection.query.api.impl.ValueUtil.asJsonObject;
import static org.neo4j.bolt.connection.query.api.impl.ValueUtil.asValue;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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
    private final HttpRequest.BodyPublisher bodyPublisher;
    private final ValueFactory valueFactory;
    private final RunMessage message;
    private final Supplier<TransactionInfo> transactionInfoSupplier;
    private final AtomicReference<String> databaseName = new AtomicReference<>();

    RunMessageHandler(
            ResponseHandler handler,
            HttpContext httpContext,
            ValueFactory valueFactory,
            RunMessage message,
            Supplier<TransactionInfo> transactionInfoSupplier,
            LoggingProvider logging) {
        super(httpContext, handler, valueFactory, logging);
        this.log = logging.getLog(getClass());
        this.handler = Objects.requireNonNull(handler);
        this.httpContext = Objects.requireNonNull(httpContext);
        this.valueFactory = Objects.requireNonNull(valueFactory);
        this.message = Objects.requireNonNull(message);
        this.transactionInfoSupplier = Objects.requireNonNull(transactionInfoSupplier);

        if (message.extra().isPresent()) {
            var extra = message.extra().get();
            if (extra.databaseName().isEmpty()) {
                throw new BoltClientException("Database name must be specified");
            }
        }
        this.bodyPublisher = newHttpRequestBodyPublisher(httpContext.gson(), message);
    }

    @Override
    protected HttpRequest newHttpRequest() {
        var transactionInfo = transactionInfoSupplier.get();
        String databaseName;
        URI uri;
        var headers = this.headers;
        if (transactionInfo != null) {
            databaseName = transactionInfo.databaseName();
            uri = URI.create("%s/db/%s/query/v2/tx/%s"
                    .formatted(httpContext.baseUri().toString(), databaseName, transactionInfo.id()));
            if (transactionInfo.affinity() != null) {
                headers = Arrays.copyOf(headers, headers.length + 2);
                headers[headers.length - 2] = "neo4j-cluster-affinity";
                headers[headers.length - 1] = transactionInfo.affinity();
            }
        } else {
            databaseName =
                    message.extra().flatMap(extra -> extra.databaseName()).orElse(null);
            if (databaseName == null) {
                throw new BoltClientException("Database not specified");
            }
            uri = URI.create("%s/db/%s/query/v2".formatted(httpContext.baseUri().toString(), databaseName));
        }
        this.databaseName.set(databaseName);
        return HttpRequest.newBuilder(uri).headers(headers).POST(bodyPublisher).build();
    }

    @Override
    protected Query handleResponse(HttpResponse<String> response) {
        var queryResult = httpContext.gson().fromJson(response.body(), QueryResult.class);
        var records = queryResult.data().values().stream()
                .map(record -> record.stream()
                        .map(value -> asValue(value, valueFactory))
                        .toArray(Value[]::new))
                .collect(Collectors.toList());
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
        var query = new Query(id, queryResult.data().fields(), records, Collections.unmodifiableMap(metadata));
        handler.onRunSummary(new RunSummaryImpl(query.id(), query.fields(), -1, databaseName));
        return query;
    }

    private HttpRequest.BodyPublisher newHttpRequestBodyPublisher(Gson gson, RunMessage message) {
        var jsonObject = new JsonObject();

        jsonObject.addProperty("statement", message.query());
        if (!message.parameters().isEmpty()) {
            jsonObject.add("parameters", params(message.parameters()));
        }

        if (message.extra().isPresent()) {
            var extra = message.extra().get();
            if (extra.accessMode() == AccessMode.READ) {
                jsonObject.addProperty("accessMode", "Read");
            }
            extra.impersonatedUser()
                    .ifPresent(impersonatedUser -> jsonObject.addProperty("impersonatedUser", impersonatedUser));
            if (!extra.bookmarks().isEmpty()) {
                var jsonArray = new JsonArray();
                extra.bookmarks().forEach(jsonArray::add);
                jsonObject.add("bookmarks", jsonArray);
            }
        }

        jsonObject.addProperty("includeCounters", true);
        var jsonBody = gson.toJson(jsonObject);
        log.log(System.Logger.Level.DEBUG, "json body: " + jsonBody);
        return HttpRequest.BodyPublishers.ofString(jsonBody);
    }

    private static JsonObject params(Map<String, Value> parameters) {
        var parametersObject = new JsonObject();
        for (var entry : parameters.entrySet()) {
            var key = entry.getKey();
            var value = entry.getValue();
            parametersObject.add(key, asJsonObject(value));
        }
        return parametersObject;
    }
}
