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
import static org.neo4j.bolt.connection.query.api.impl.HttpUtil.mapToString;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import org.neo4j.bolt.connection.GqlStatusError;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.ResponseHandler;
import org.neo4j.bolt.connection.exception.BoltException;
import org.neo4j.bolt.connection.exception.BoltFailureException;
import org.neo4j.bolt.connection.exception.BoltServiceUnavailableException;
import org.neo4j.bolt.connection.values.ValueFactory;

abstract class AbstractMessageHandler<T> implements MessageHandler<T> {
    private final System.Logger log;
    private final HttpClient httpClient;
    private final Gson gson;
    protected final String[] headers;
    protected final ResponseHandler handler;
    protected final ValueFactory valueFactory;

    AbstractMessageHandler(
            HttpContext httpContext, ResponseHandler handler, ValueFactory valueFactory, LoggingProvider logging) {
        this.log = logging.getLog(getClass());
        this.httpClient = Objects.requireNonNull(httpContext.httpClient());
        this.gson = Objects.requireNonNull(httpContext.gson());
        this.headers = new String[] {
            "Content-Type", "application/vnd.neo4j.query",
            "Accept", "application/vnd.neo4j.query",
            "Authorization", Objects.requireNonNull(httpContext.authHeader())
        };
        this.handler = Objects.requireNonNull(handler);
        this.valueFactory = Objects.requireNonNull(valueFactory);
    }

    @Override
    public CompletionStage<T> exchange() {
        var request = newHttpRequest();
        log.log(System.Logger.Level.DEBUG, "Sending request %s".formatted(mapToString(request)));
        return httpClient
                .sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .handle((response, throwable) -> {
                    if (throwable != null) {
                        log.log(
                                System.Logger.Level.DEBUG,
                                "An error occurred while sending request %s".formatted(throwable.getMessage()));
                        throwable = completionExceptionCause(throwable);
                        if (throwable instanceof IOException) {
                            // todo defunct
                            throw new BoltServiceUnavailableException(
                                    "An error occurred while sending request", throwable);
                        } else {
                            throw new BoltException("An error occurred while sending request", throwable);
                        }
                    } else {
                        log.log(System.Logger.Level.DEBUG, "Received response %s".formatted(mapToString(response)));
                        return switch (response.statusCode()) {
                            case 200, 202 -> {
                                // Query API may return an error
                                var jsonObject = gson.fromJson(response.body(), JsonObject.class);
                                if (jsonObject != null && jsonObject.get("errors") != null) {
                                    yield handleFailureResponse(response);
                                } else {
                                    yield handleResponse(response);
                                }
                            }
                            case 400, 401, 404, 500 -> handleFailureResponse(response);
                            default -> throw new BoltException(
                                    "An unexpected response code: " + response.statusCode(), null);
                        };
                    }
                });
    }

    protected abstract HttpRequest newHttpRequest();

    protected abstract T handleResponse(HttpResponse<String> response);

    protected T handleFailureResponse(HttpResponse<String> response) {
        var errorsData = gson.fromJson(response.body(), ErrorsData.class);
        var error = errorsData.errors().get(0);
        var diagnosticRecord = Map.ofEntries(
                Map.entry("CURRENT_SCHEMA", valueFactory.value("/")),
                Map.entry("OPERATION", valueFactory.value("")),
                Map.entry("OPERATION_CODE", valueFactory.value("0")));
        throw new BoltFailureException(
                error.code(),
                error.message(),
                GqlStatusError.UNKNOWN.getStatus(),
                GqlStatusError.UNKNOWN.getStatusDescription(error.message()),
                diagnosticRecord,
                null);
    }
}
