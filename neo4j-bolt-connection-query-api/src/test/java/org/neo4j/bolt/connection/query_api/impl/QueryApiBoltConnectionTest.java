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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.neo4j.bolt.connection.AccessMode;
import org.neo4j.bolt.connection.AuthTokens;
import org.neo4j.bolt.connection.BoltConnectionState;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.NotificationConfig;
import org.neo4j.bolt.connection.ResponseHandler;
import org.neo4j.bolt.connection.TransactionType;
import org.neo4j.bolt.connection.exception.BoltClientException;
import org.neo4j.bolt.connection.exception.BoltFailureException;
import org.neo4j.bolt.connection.message.BeginMessage;
import org.neo4j.bolt.connection.message.Messages;
import org.neo4j.bolt.connection.message.RunMessage;
import org.neo4j.bolt.connection.test.values.TestValueFactory;

final class QueryApiBoltConnectionTest {
    QueryApiBoltConnection boltConnection;

    @Mock
    HttpClient httpClient;

    @Mock
    LoggingProvider loggingProvider;

    @Mock
    System.Logger logger;

    @Mock
    ResponseHandler handler;

    @Mock
    HttpResponse<String> response;

    @SuppressWarnings("resource")
    @BeforeEach
    void beforeEach() {
        MockitoAnnotations.openMocks(this);
        given(loggingProvider.getLog(any(Class.class))).willReturn(logger);
        given(response.headers()).willReturn(HttpHeaders.of(Map.of(), (k, v) -> true));
        given(response.statusCode()).willReturn(202);
        given(httpClient.sendAsync(any(), eq(HttpResponse.BodyHandlers.ofString())))
                .willReturn(CompletableFuture.completedFuture(response));
        boltConnection = new QueryApiBoltConnection(
                TestValueFactory.INSTANCE,
                httpClient,
                URI.create("http://localhost"),
                AuthTokens.basic("user", "password", "realm", TestValueFactory.INSTANCE),
                "userAgent",
                "serverAgent",
                new BoltProtocolVersion(5, 6),
                Clock.systemUTC(),
                loggingProvider);
    }

    @Test
    void shouldBeOpenWhenNew() {
        assertEquals(BoltConnectionState.OPEN, boltConnection.state());
    }

    @Test
    void shouldBeFailedAfterFailure() {
        // given
        given(response.body())
                .willReturn(
                        """
                {
                    "errors": [{"code": "code", "message": "message"}]
                }
                """);

        // when
        boltConnection
                .writeAndFlush(handler, newRunMessage())
                .toCompletableFuture()
                .join();

        // then
        then(handler).should().onError(any(BoltFailureException.class));
        then(handler).should().onComplete();
        assertEquals(BoltConnectionState.FAILURE, boltConnection.state());
    }

    @Test
    void shouldIgnoreMessagesWhenFailed() {
        // given
        boltConnection.updateState(BoltConnectionState.FAILURE);
        var messages = List.of(newRunMessage(), Messages.pull(-1, -1));

        // when
        boltConnection.writeAndFlush(handler, messages).toCompletableFuture().join();

        // then
        then(handler).should(times(2)).onIgnored();
        then(handler).should().onComplete();
    }

    @Test
    void shouldResetStateWhenFailed() {
        // given
        given(response.body())
                .willReturn(
                        """
                {
                    "transaction": {"id": "id", "expires": "2024-10-22T15:48:29Z"}
                }
                """);
        boltConnection.updateState(BoltConnectionState.FAILURE);
        var messages = List.of(Messages.reset(), newBeginMessage());

        // when
        boltConnection.writeAndFlush(handler, messages).toCompletableFuture().join();

        // then
        then(handler).should().onResetSummary(any());
        then(handler).should().onBeginSummary(any());
        then(handler).should().onComplete();
        assertEquals(BoltConnectionState.OPEN, boltConnection.state());
    }

    @ParameterizedTest
    @MethodSource("shouldFailToWriteArgs")
    void shouldFailToWrite(BoltConnectionState state) {
        // given
        boltConnection.updateState(state);

        // when
        var result = boltConnection.writeAndFlush(handler, newRunMessage()).toCompletableFuture();

        // then
        var ex = assertThrows(CompletionException.class, result::join);
        assertInstanceOf(BoltClientException.class, ex.getCause());
    }

    @Test
    void shouldReleaseResourcesOnRelease() {
        // given
        boltConnection.setTransactionInfo(mock(TransactionInfo.class));
        boltConnection.addQuery(-1, mock(Query.class));

        // when
        boltConnection
                .writeAndFlush(handler, Messages.reset())
                .toCompletableFuture()
                .join();

        // then
        then(handler).should().onResetSummary(any());
        then(handler).should().onComplete();
        assertEquals(BoltConnectionState.OPEN, boltConnection.state());
        assertNull(boltConnection.getTransactionInfo());
        assertNull(boltConnection.findById(-1));
    }

    static Stream<Arguments> shouldFailToWriteArgs() {
        return Stream.of(Arguments.of(BoltConnectionState.ERROR), Arguments.of(BoltConnectionState.CLOSED));
    }

    private static BeginMessage newBeginMessage() {
        return Messages.beginTransaction(
                "neo4j",
                AccessMode.WRITE,
                null,
                Set.of(),
                TransactionType.DEFAULT,
                null,
                null,
                NotificationConfig.defaultConfig());
    }

    private static RunMessage newRunMessage() {
        return Messages.run(
                "neo4j",
                AccessMode.WRITE,
                null,
                Set.of(),
                "not query",
                Map.of(),
                null,
                null,
                NotificationConfig.defaultConfig());
    }
}
