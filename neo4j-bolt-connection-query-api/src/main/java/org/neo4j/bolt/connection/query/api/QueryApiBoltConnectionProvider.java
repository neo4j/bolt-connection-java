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
package org.neo4j.bolt.connection.query.api;

import com.fasterxml.jackson.jr.ob.JSON;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.neo4j.bolt.connection.AccessMode;
import org.neo4j.bolt.connection.AuthToken;
import org.neo4j.bolt.connection.BoltAgent;
import org.neo4j.bolt.connection.BoltConnection;
import org.neo4j.bolt.connection.BoltConnectionProvider;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.DatabaseName;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.NotificationConfig;
import org.neo4j.bolt.connection.RoutingContext;
import org.neo4j.bolt.connection.SecurityPlan;
import org.neo4j.bolt.connection.exception.BoltClientException;
import org.neo4j.bolt.connection.query.api.impl.QueryApiBoltConnection;
import org.neo4j.bolt.connection.values.ValueFactory;

public class QueryApiBoltConnectionProvider implements BoltConnectionProvider {
    private final HttpClient httpClient;
    private final URI baseUri;
    private final LoggingProvider logging;
    private final ValueFactory valueFactory;
    private boolean closed;

    public QueryApiBoltConnectionProvider(URI baseUri, LoggingProvider logging, ValueFactory valueFactory) {
        this.httpClient = HttpClient.newBuilder().build();
        this.baseUri = Objects.requireNonNull(baseUri);
        this.logging = Objects.requireNonNull(logging);
        this.valueFactory = Objects.requireNonNull(valueFactory);
    }

    @Override
    public CompletionStage<BoltConnection> connect(
            BoltServerAddress address,
            RoutingContext routingContext,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            SecurityPlan securityPlan,
            DatabaseName databaseName,
            Supplier<CompletionStage<AuthToken>> authTokenStageSupplier,
            AccessMode mode,
            Set<String> bookmarks,
            String impersonatedUser,
            BoltProtocolVersion minVersion,
            NotificationConfig notificationConfig,
            Consumer<DatabaseName> databaseNameConsumer,
            Map<String, Object> additionalParameters) {
        if (closed) {
            return CompletableFuture.failedFuture(new IllegalStateException("Connection provider is closed."));
        }
        var request = HttpRequest.newBuilder(baseUri).build();
        return httpClient
                .sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenCompose(response -> {
                    if (response.statusCode() == 200) {
                        DiscoveryResponse discoveryResponse = null;
                        try {
                            discoveryResponse = JSON.std.beanFrom(DiscoveryResponse.class, response.body());
                        } catch (IOException e) {
                            throw new BoltClientException(
                                    "Cannot parse %s to DiscoveryResponse".formatted(response.body()), e);
                        }
                        var serverAgent = "Neo4j/%s".formatted(discoveryResponse.neo4j_version());
                        return authTokenStageSupplier
                                .get()
                                .thenApply(authToken -> new QueryApiBoltConnection(
                                        valueFactory, httpClient, baseUri, authToken, serverAgent, logging));
                    } else {
                        return CompletableFuture.failedStage(
                                new BoltClientException("Unexpected response code: " + response.statusCode()));
                    }
                });
    }

    @Override
    public CompletionStage<Void> verifyConnectivity(
            BoltServerAddress address,
            RoutingContext routingContext,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            SecurityPlan securityPlan,
            AuthToken authToken) {
        if (closed) {
            return CompletableFuture.failedFuture(new IllegalStateException("Connection provider is closed."));
        }
        var request = HttpRequest.newBuilder(baseUri).build();
        return httpClient
                .sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(response -> {
                    if (response.statusCode() == 200) {
                        try {
                            var discoveryResponse =
                                    JSON.builder().build().beanFrom(DiscoveryResponse.class, response.body());
                            var serverAgent = "Neo4j/%s".formatted(discoveryResponse.neo4j_version());
                            return null;
                        } catch (IOException e) {
                            throw new BoltClientException(
                                    "Cannot parse %s to DiscoveryResponse".formatted(response.body()), e);
                        }
                    } else {
                        throw new BoltClientException("Unexpected response code: " + response.statusCode());
                    }
                });
    }

    @Override
    public CompletionStage<Boolean> supportsMultiDb(
            BoltServerAddress address,
            RoutingContext routingContext,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            SecurityPlan securityPlan,
            AuthToken authToken) {
        return null;
    }

    @Override
    public CompletionStage<Boolean> supportsSessionAuth(
            BoltServerAddress address,
            RoutingContext routingContext,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            SecurityPlan securityPlan,
            AuthToken authToken) {
        return null;
    }

    @Override
    public CompletionStage<Void> close() {
        // hard hack to align with general driver behaviour
        this.closed = true;
        return CompletableFuture.completedStage(null);
    }
}
