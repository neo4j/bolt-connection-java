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
package org.neo4j.bolt.connection.netty.impl;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.netty.handler.ssl.SslHandshakeTimeoutException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.connection.AuthToken;
import org.neo4j.bolt.connection.AuthTokens;
import org.neo4j.bolt.connection.BoltAgent;
import org.neo4j.bolt.connection.BoltConnectionProvider;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.SecurityPlans;
import org.neo4j.bolt.connection.exception.BoltConnectionInitialisationTimeoutException;
import org.neo4j.bolt.connection.netty.NettyBoltConnectionProviderFactory;
import org.neo4j.bolt.connection.observation.BoltExchangeObservation;
import org.neo4j.bolt.connection.observation.HttpExchangeObservation;
import org.neo4j.bolt.connection.observation.ImmutableObservation;
import org.neo4j.bolt.connection.observation.ObservationProvider;
import org.neo4j.bolt.connection.test.values.TestValueFactory;
import org.neo4j.bolt.connection.values.ValueFactory;

class NettyBoltConnectionProviderIT {
    static NettyBoltConnectionProviderFactory factory = new NettyBoltConnectionProviderFactory();
    static LoggingProvider loggingProvider = new SystemLoggingProvider();
    static ValueFactory valueFactory = TestValueFactory.INSTANCE;
    static ObservationProvider observationProvider = new NoopObservationProvider();
    static BoltAgent boltAgent = new BoltAgent("product", "platform", "language", "languageDetails");
    static String userAgent = "userAgent";
    static AuthToken authToken = AuthTokens.none(valueFactory);

    BoltConnectionProvider provider;

    @BeforeEach
    void beforeEach() {
        provider = factory.create(loggingProvider, valueFactory, observationProvider, Map.of());
    }

    @Test
    void shouldTimeoutSslHandshake() throws GeneralSecurityException, IOException {
        try (var server = new ServerSocket(0)) {
            // given
            var uri = URI.create("bolt://localhost:%d".formatted(server.getLocalPort()));

            // when
            var connectionFuture = provider.connect(
                            uri,
                            null,
                            boltAgent,
                            userAgent,
                            0,
                            100,
                            SecurityPlans.encryptedForAnyCertificate(),
                            authToken,
                            null,
                            null,
                            NoopObservationProvider.NOOP_OBSERVATION)
                    .toCompletableFuture();

            // then
            Throwable exception = assertThrows(CompletionException.class, connectionFuture::join);
            exception = exception.getCause();
            assertInstanceOf(BoltConnectionInitialisationTimeoutException.class, exception);
            exception = exception.getCause();
            assertInstanceOf(SslHandshakeTimeoutException.class, exception);
        }
    }

    static class SystemLoggingProvider implements LoggingProvider {
        @Override
        public System.Logger getLog(Class<?> cls) {
            return System.getLogger(cls.getName());
        }

        @Override
        public System.Logger getLog(String name) {
            return System.getLogger(name);
        }
    }

    static class NoopObservationProvider implements ObservationProvider {
        static final NoopObservation NOOP_OBSERVATION = new NoopObservation();

        @Override
        public BoltExchangeObservation boltExchange(
                ImmutableObservation observationParent,
                String host,
                int port,
                BoltProtocolVersion boltVersion,
                BiConsumer<String, String> setter) {
            return NOOP_OBSERVATION;
        }

        @Override
        public HttpExchangeObservation httpExchange(
                ImmutableObservation observationParent,
                URI uri,
                String method,
                String uriTemplate,
                BiConsumer<String, String> setter) {
            return null;
        }

        @Override
        public ImmutableObservation scopedObservation() {
            return null;
        }

        @Override
        public <T> T supplyInScope(ImmutableObservation observation, Supplier<T> supplier) {
            return supplier.get();
        }
    }

    static class NoopObservation implements BoltExchangeObservation {
        @Override
        public BoltExchangeObservation onWrite(String messageName) {
            return this;
        }

        @Override
        public BoltExchangeObservation onRecord() {
            return this;
        }

        @Override
        public BoltExchangeObservation onSummary(String messageName) {
            return this;
        }

        @Override
        public BoltExchangeObservation error(Throwable error) {
            return this;
        }

        @Override
        public void stop() {}
    }
}
