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

import com.fasterxml.jackson.jr.ob.JSON;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.bolt.connection.AuthToken;
import org.neo4j.bolt.connection.BoltAgent;
import org.neo4j.bolt.connection.BoltConnection;
import org.neo4j.bolt.connection.BoltConnectionProvider;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.NotificationConfig;
import org.neo4j.bolt.connection.RoutingContext;
import org.neo4j.bolt.connection.SecurityPlan;
import org.neo4j.bolt.connection.exception.BoltClientException;
import org.neo4j.bolt.connection.values.ValueFactory;

public class QueryApiBoltConnectionProvider implements BoltConnectionProvider {
    private final HttpClient httpClient;
    private final LoggingProvider logging;
    private final ValueFactory valueFactory;

    public QueryApiBoltConnectionProvider(LoggingProvider logging, ValueFactory valueFactory) {
        this.httpClient = HttpClient.newBuilder().build();
        this.logging = Objects.requireNonNull(logging);
        this.valueFactory = Objects.requireNonNull(valueFactory);
    }

    @Override
    public CompletionStage<BoltConnection> connect(
            URI uri,
            RoutingContext routingContext,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            SecurityPlan securityPlan,
            AuthToken authToken,
            BoltProtocolVersion minVersion,
            NotificationConfig notificationConfig) {

        var request = HttpRequest.newBuilder(uri).build();
        return httpClient
                .sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(response -> {
                    if (response.statusCode() == 200) {
                        DiscoveryResponse discoveryResponse = null;
                        try {
                            discoveryResponse = JSON.std.beanFrom(DiscoveryResponse.class, response.body());
                        } catch (IOException e) {
                            throw new BoltClientException(
                                    "Cannot parse %s to DiscoveryResponse".formatted(response.body()), e);
                        }
                        var serverAgent = "Neo4j/%s".formatted(discoveryResponse.neo4j_version());
                        return new QueryApiBoltConnection(
                                valueFactory, httpClient, uri, authToken, serverAgent, logging);
                    } else {
                        throw new BoltClientException("Unexpected response code: " + response.statusCode());
                    }
                });
    }

    @Override
    public CompletionStage<Void> close() {
        return CompletableFuture.completedStage(null);
    }
}
