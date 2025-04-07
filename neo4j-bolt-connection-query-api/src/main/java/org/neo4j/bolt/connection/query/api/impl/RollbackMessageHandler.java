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
import java.util.Objects;
import java.util.function.Supplier;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.ResponseHandler;
import org.neo4j.bolt.connection.exception.BoltClientException;
import org.neo4j.bolt.connection.summary.RollbackSummary;
import org.neo4j.bolt.connection.values.ValueFactory;

final class RollbackMessageHandler extends AbstractMessageHandler<Void> {
    private final System.Logger log;
    private final ResponseHandler handler;
    private final HttpContext httpContext;
    private final Supplier<TransactionInfo> transactionInfoSupplier;

    RollbackMessageHandler(
            ResponseHandler handler,
            HttpContext httpContext,
            Supplier<TransactionInfo> transactionInfoSupplier,
            ValueFactory valueFactory,
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
        var uri = URI.create("%s/db/%s/query/v2/tx/%s"
                .formatted(httpContext.baseUri().toString(), transactionInfo.databaseName(), transactionInfo.id()));
        var headers = this.headers;
        if (transactionInfo.affinity() != null) {
            headers = Arrays.copyOf(headers, headers.length + 2);
            headers[headers.length - 2] = "neo4j-cluster-affinity";
            headers[headers.length - 1] = transactionInfo.affinity();
        }
        return HttpRequest.newBuilder(uri).headers(headers).DELETE().build();
    }

    @Override
    protected Void handleResponse(HttpResponse<String> response) {
        handler.onRollbackSummary(new RollbackSummary() {});
        return null;
    }
}
