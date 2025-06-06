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
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import javax.net.ssl.SSLParameters;
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
import org.neo4j.bolt.connection.exception.MinVersionAcquisitionException;
import org.neo4j.bolt.connection.values.ValueFactory;

public class QueryApiBoltConnectionProvider implements BoltConnectionProvider {
    private final LoggingProvider logging;
    private final System.Logger logger;
    private final ValueFactory valueFactory;
    // Higher versions of Bolt require GQL support that it not available in Query API.
    private final BoltProtocolVersion BOLT_PROTOCOL_VERSION = new BoltProtocolVersion(5, 4);
    private final Executor httpExecutor;

    public QueryApiBoltConnectionProvider(LoggingProvider logging, ValueFactory valueFactory) {
        this.logging = Objects.requireNonNull(logging);
        this.logger = logging.getLog(getClass());
        this.valueFactory = Objects.requireNonNull(valueFactory);
        this.httpExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    @SuppressWarnings("resource") // not AutoCloseable in Java 17
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
        if (minVersion != null && minVersion.compareTo(BOLT_PROTOCOL_VERSION) > 0) {
            return CompletableFuture.failedStage(
                    new MinVersionAcquisitionException("lower version", BOLT_PROTOCOL_VERSION));
        }
        if (notificationConfig != null && !notificationConfig.equals(NotificationConfig.defaultConfig())) {
            logger.log(
                    System.Logger.Level.WARNING,
                    "Setting notification config is not supported, server default will be used instead");
        }
        if ("http".equals(uri.getScheme()) && securityPlan != null) {
            logger.log(
                    System.Logger.Level.WARNING,
                    "Setting security plan when using http scheme is not supported, it will be ignored");
        }
        HttpClient httpClientWithTimeout;
        try {
            var builder = newHttpClientBuilder(securityPlan);
            if (connectTimeoutMillis > 0) {
                builder.connectTimeout(Duration.ofMillis(connectTimeoutMillis));
            }
            httpClientWithTimeout = builder.build();
        } catch (Exception ex) {
            return CompletableFuture.failedStage(ex);
        }
        var requestBuilder = HttpRequest.newBuilder(uri);
        if (userAgent != null) {
            requestBuilder.header("User-Agent", userAgent);
        }
        return httpClientWithTimeout
                .sendAsync(requestBuilder.build(), HttpResponse.BodyHandlers.ofString())
                .thenApply(response -> {
                    if (response.statusCode() == 200) {
                        try {
                            DiscoveryResponse discoveryResponse =
                                    JSON.std.beanFrom(DiscoveryResponse.class, response.body());

                            var serverAgent = "Neo4j/%s".formatted(discoveryResponse.neo4j_version());
                            var httpClient = newHttpClientBuilder(securityPlan).build();
                            return new QueryApiBoltConnection(
                                    valueFactory,
                                    httpClient,
                                    uri,
                                    authToken,
                                    userAgent,
                                    serverAgent,
                                    BOLT_PROTOCOL_VERSION,
                                    logging);
                        } catch (IOException e) {
                            throw new BoltClientException(
                                    "Cannot parse %s to DiscoveryResponse".formatted(response.body()), e);
                        }
                    } else {
                        throw new BoltClientException("Unexpected response code: " + response.statusCode());
                    }
                });
    }

    private HttpClient.Builder newHttpClientBuilder(SecurityPlan securityPlan) {
        var httpClientBuilder = HttpClient.newBuilder().executor(httpExecutor);
        if (securityPlan != null) {
            httpClientBuilder = httpClientBuilder.sslContext(securityPlan.sslContext());
            String endpointIdentificationAlgorithm;
            if (securityPlan.verifyHostname()) {
                if (securityPlan.expectedHostname() != null) {
                    // not needed at the moment
                    throw new BoltClientException("SecurityPlan with expectedHostname is not supported");
                }
                endpointIdentificationAlgorithm = "HTTPS";
            } else {
                endpointIdentificationAlgorithm = null;
            }
            var sslParameters = new SSLParameters();
            sslParameters.setEndpointIdentificationAlgorithm(endpointIdentificationAlgorithm);
            httpClientBuilder = httpClientBuilder.sslParameters(sslParameters);
        }
        return httpClientBuilder;
    }

    @Override
    public CompletionStage<Void> close() {
        return CompletableFuture.completedStage(null);
    }
}
