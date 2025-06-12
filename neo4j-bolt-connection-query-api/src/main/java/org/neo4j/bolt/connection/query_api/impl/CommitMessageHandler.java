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
import java.time.Duration;
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
    private final Supplier<String> authHeaderSupplier;
    private final Supplier<TransactionInfo> transactionInfoSupplier;
    private final Duration readTimeout;

    CommitMessageHandler(
            ResponseHandler handler,
            HttpContext httpContext,
            Supplier<String> authHeaderSupplier,
            Supplier<TransactionInfo> transactionInfoSupplier,
            Duration readTimeout,
            ValueFactory valueFactory,
            LoggingProvider logging) {
        super(httpContext, handler, valueFactory, logging);
        this.log = logging.getLog(getClass());
        this.handler = Objects.requireNonNull(handler);
        this.httpContext = Objects.requireNonNull(httpContext);
        this.authHeaderSupplier = Objects.requireNonNull(authHeaderSupplier);
        this.transactionInfoSupplier = Objects.requireNonNull(transactionInfoSupplier);
        this.readTimeout = readTimeout;
    }

    @Override
    protected HttpRequest newHttpRequest() {
        var transactionInfo = transactionInfoSupplier.get();
        if (transactionInfo == null) {
            throw new BoltClientException("No transaction found");
        }
        var headers = httpContext.headers(authHeaderSupplier.get());
        if (transactionInfo.affinity() != null) {
            headers = Arrays.copyOf(headers, headers.length + 2);
            headers[headers.length - 2] = "neo4j-cluster-affinity";
            headers[headers.length - 1] = transactionInfo.affinity();
        }
        var uri = httpContext.commitUrl(transactionInfo);
        var builder = HttpRequest.newBuilder(uri).headers(headers).POST(HttpRequest.BodyPublishers.noBody());
        if (readTimeout != null) {
            builder = builder.timeout(readTimeout);
        }
        return builder.build();
    }

    @Override
    protected Void handleResponse(HttpResponse<String> response) {
        try {
            var bookmarksWrapper = httpContext.json().beanFrom(BookmarksWrapper.class, response.body());
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
        } catch (IOException e) {
            throw new BoltClientException("Cannot parse %s to BookmarksWrapper".formatted(response.body()), e);
        }
    }

    record BookmarksWrapper(List<String> bookmarks) {}
}
