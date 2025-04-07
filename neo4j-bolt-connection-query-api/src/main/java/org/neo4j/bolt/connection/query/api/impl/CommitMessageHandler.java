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

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.ResponseHandler;
import org.neo4j.bolt.connection.exception.BoltClientException;
import org.neo4j.bolt.connection.values.ValueFactory;

final class CommitMessageHandler extends AbstractMessageHandler<Void> {
    private final System.Logger log;
    private final ResponseHandler handler;
    private final HttpContext httpContext;
    private final Supplier<TransactionInfo> transactionInfoSupplier;

    CommitMessageHandler(
            ResponseHandler handler,
            HttpContext httpContext,
            ValueFactory valueFactory,
            Supplier<TransactionInfo> transactionInfoSupplier,
            LoggingProvider logging) {
        super(httpContext, handler, valueFactory, logging);
        this.log = logging.getLog(getClass());
        this.handler = Objects.requireNonNull(handler);
        this.httpContext = Objects.requireNonNull(httpContext);
        this.transactionInfoSupplier = Objects.requireNonNull(transactionInfoSupplier);
    }

    @Override
    protected HttpRequest newHttpRequest() {
        var transactionInfo = transactionInfoSupplier.get();
        if (transactionInfo == null) {
            throw new BoltClientException("No transaction found");
        }
        var headers = this.headers;
        if (transactionInfo.affinity() != null) {
            headers = Arrays.copyOf(headers, headers.length + 2);
            headers[headers.length - 2] = "neo4j-cluster-affinity";
            headers[headers.length - 1] = transactionInfo.affinity();
        }
        var uri = URI.create("%s/db/%s/query/v2/tx/%s/commit"
                .formatted(httpContext.baseUri().toString(), transactionInfo.databaseName(), transactionInfo.id()));
        return HttpRequest.newBuilder(uri)
                .headers(headers)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
    }

    @Override
    protected Void handleResponse(HttpResponse<String> response) {
        var bookmarksWrapper = httpContext.gson().fromJson(response.body(), BookmarksWrapper.class);
        var bookmarks = bookmarksWrapper.bookmarks();
        String bookmark = null;
        if (bookmarks == null) {
            log.log(System.Logger.Level.INFO, "No bookmarks found");
        } else if (bookmarks.isEmpty()) {
            log.log(System.Logger.Level.INFO, "No bookmarks found");
        } else if (bookmarks.size() > 1) {
            log.log(System.Logger.Level.INFO, "Multiple bookmarks found");
            bookmark = bookmarks.get(0);
        } else {
            bookmark = bookmarks.get(0);
        }
        handler.onCommitSummary(new CommitSummaryImpl(bookmark));
        return null;
    }

    record BookmarksWrapper(List<String> bookmarks) {}
}
