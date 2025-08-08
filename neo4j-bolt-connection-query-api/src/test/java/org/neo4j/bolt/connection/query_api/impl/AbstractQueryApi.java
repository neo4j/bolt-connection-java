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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

import java.io.IOException;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;
import org.neo4j.bolt.connection.AccessMode;
import org.neo4j.bolt.connection.AuthTokens;
import org.neo4j.bolt.connection.BoltConnectionState;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.NotificationConfig;
import org.neo4j.bolt.connection.ResponseHandler;
import org.neo4j.bolt.connection.SecurityPlans;
import org.neo4j.bolt.connection.TransactionType;
import org.neo4j.bolt.connection.exception.BoltClientException;
import org.neo4j.bolt.connection.exception.BoltConnectionReadTimeoutException;
import org.neo4j.bolt.connection.exception.BoltFailureException;
import org.neo4j.bolt.connection.message.Messages;
import org.neo4j.bolt.connection.observation.HttpExchangeObservation;
import org.neo4j.bolt.connection.observation.ImmutableObservation;
import org.neo4j.bolt.connection.observation.Observation;
import org.neo4j.bolt.connection.observation.ObservationProvider;
import org.neo4j.bolt.connection.summary.BeginSummary;
import org.neo4j.bolt.connection.summary.CommitSummary;
import org.neo4j.bolt.connection.summary.DiscardSummary;
import org.neo4j.bolt.connection.summary.PullSummary;
import org.neo4j.bolt.connection.summary.RollbackSummary;
import org.neo4j.bolt.connection.summary.RunSummary;
import org.neo4j.bolt.connection.test.values.TestValueFactory;
import org.neo4j.bolt.connection.values.Value;

abstract class AbstractQueryApi {
    @Mock
    LoggingProvider logging;

    @Mock
    ResponseHandler responseHandler;

    @Mock
    ObservationProvider observationProvider;

    @Mock
    HttpExchangeObservation observation;

    QueryApiBoltConnection connection;

    @SuppressWarnings("resource")
    @BeforeEach
    void beforeEach() throws GeneralSecurityException, IOException {
        MockitoAnnotations.openMocks(this);
        var valueFactory = TestValueFactory.INSTANCE;
        given(logging.getLog(any(Class.class))).willAnswer((Answer<System.Logger>) invocation ->
                System.getLogger(invocation.getArgument(0).getClass().getCanonicalName()));
        given(observationProvider.httpExchange(any(), any(), any(), any(), any()))
                .willReturn(observation);
        var provider =
                new QueryApiBoltConnectionProvider(logging, valueFactory, Clock.systemUTC(), observationProvider);
        connection = (QueryApiBoltConnection) provider.connect(
                        uri(),
                        null,
                        null,
                        null,
                        0,
                        SecurityPlans.encryptedForSystemCASignedCertificates(),
                        AuthTokens.basic(username(), password(), "basic", valueFactory),
                        null,
                        null,
                        mock(ImmutableObservation.class))
                .toCompletableFuture()
                .join();
    }

    @Test
    void shouldRunAutocommit() {
        // given
        var responseFuture = new CompletableFuture<>();
        willAnswer(invocation -> {
                    responseFuture.complete(null);
                    return null;
                })
                .given(responseHandler)
                .onComplete();
        var message = Messages.run(
                database(),
                AccessMode.WRITE,
                null,
                Set.of(),
                "RETURN 1",
                Map.of(),
                null,
                Map.of(),
                NotificationConfig.defaultConfig());

        // when
        connection
                .writeAndFlush(responseHandler, message, mock(Observation.class))
                .thenCompose(ignored -> responseFuture)
                .toCompletableFuture()
                .join();

        // then
        var runSummaryCaptor = ArgumentCaptor.forClass(RunSummary.class);
        var responseHandlerInOrder = inOrder(responseHandler);
        responseHandlerInOrder.verify(responseHandler).onRunSummary(runSummaryCaptor.capture());
        responseHandlerInOrder.verify(responseHandler).onComplete();
        then(responseHandler).shouldHaveNoMoreInteractions();

        var runSummary = runSummaryCaptor.getValue();
        assertNotNull(runSummary);
        assertTrue(runSummary.queryId() != 0);
        assertEquals(runSummary.keys(), List.of("1"));
        assertEquals(-1, runSummary.resultAvailableAfter());
        assertEquals(database(), runSummary.databaseName().orElse(null));
    }

    @Test
    void shoulRunAutocommitWithParameters() {
        // given
        var responseFuture = new CompletableFuture<>();
        willAnswer(invocation -> {
                    responseFuture.complete(null);
                    return null;
                })
                .given(responseHandler)
                .onComplete();
        var message = Messages.run(
                database(),
                AccessMode.WRITE,
                null,
                Set.of(),
                "RETURN $param as test",
                Map.of("param", TestValueFactory.INSTANCE.value(1)),
                null,
                Map.of(),
                NotificationConfig.defaultConfig());

        // when
        connection
                .writeAndFlush(responseHandler, message, mock(Observation.class))
                .thenCompose(ignored -> responseFuture)
                .toCompletableFuture()
                .join();

        // then
        var runSummaryCaptor = ArgumentCaptor.forClass(RunSummary.class);
        var responseHandlerInOrder = inOrder(responseHandler);
        responseHandlerInOrder.verify(responseHandler).onRunSummary(runSummaryCaptor.capture());
        responseHandlerInOrder.verify(responseHandler).onComplete();
        then(responseHandler).shouldHaveNoMoreInteractions();

        var runSummary = runSummaryCaptor.getValue();
        assertNotNull(runSummary);
        assertTrue(runSummary.queryId() != 0);
        assertEquals(runSummary.keys(), List.of("test"));
        assertEquals(-1, runSummary.resultAvailableAfter());
        assertEquals(database(), runSummary.databaseName().orElse(null));
    }

    @Test
    void shouldFailRunAutocommitWhenDatabaseNameIsNull() {
        // given
        var message = Messages.run(
                null,
                AccessMode.WRITE,
                null,
                Set.of(),
                "RETURN 1",
                Map.of(),
                null,
                Map.of(),
                NotificationConfig.defaultConfig());

        // when & then
        var exception = assertThrows(CompletionException.class, () -> connection
                .writeAndFlush(responseHandler, message, mock(Observation.class))
                .toCompletableFuture()
                .join());
        var boltException = assertInstanceOf(BoltClientException.class, exception.getCause());
        assertEquals("Database name must be specified", boltException.getMessage());
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldRunAutocommitAndPullAll() {
        // given
        var responseFuture = new CompletableFuture<>();
        willAnswer(invocation -> {
                    responseFuture.complete(null);
                    return null;
                })
                .given(responseHandler)
                .onComplete();
        var messages = List.of(
                Messages.run(
                        database(),
                        AccessMode.WRITE,
                        null,
                        Set.of(),
                        "RETURN 1",
                        Map.of(),
                        null,
                        Map.of(),
                        NotificationConfig.defaultConfig()),
                Messages.pull(-1, -1));

        // when
        connection
                .writeAndFlush(responseHandler, messages, mock(Observation.class))
                .thenCompose(ignored -> responseFuture)
                .toCompletableFuture()
                .join();

        // then
        var runSummaryCaptor = ArgumentCaptor.forClass(RunSummary.class);
        var valuesCaptor = ArgumentCaptor.forClass(List.class);
        var pullSummaryCaptor = ArgumentCaptor.forClass(PullSummary.class);
        var responseHandlerInOrder = inOrder(responseHandler);
        responseHandlerInOrder.verify(responseHandler).onRunSummary(runSummaryCaptor.capture());
        responseHandlerInOrder.verify(responseHandler).onRecord(valuesCaptor.capture());
        responseHandlerInOrder.verify(responseHandler).onPullSummary(pullSummaryCaptor.capture());
        responseHandlerInOrder.verify(responseHandler).onComplete();
        then(responseHandler).shouldHaveNoMoreInteractions();

        var runSummary = runSummaryCaptor.getValue();
        assertNotNull(runSummary);
        assertTrue(runSummary.queryId() != 0);
        assertEquals(runSummary.keys(), List.of("1"));
        assertEquals(-1, runSummary.resultAvailableAfter());
        assertEquals(database(), runSummary.databaseName().orElse(null));

        var values = (List<Value>) valuesCaptor.getValue();
        assertEquals(List.of(TestValueFactory.INSTANCE.value(1L)), values);

        var pullSummary = pullSummaryCaptor.getValue();
        assertNotNull(pullSummary);
        assertFalse(pullSummary.hasMore());
        assertTrue(pullSummary.metadata().containsKey("stats"));
        assertTrue(pullSummary.metadata().containsKey("bookmark"));
        assertFalse(pullSummary.metadata().get("bookmark").isEmpty());
    }

    @Test
    void shouldRunAutocommitAndDiscardAll() {
        // given
        var responseFuture = new CompletableFuture<>();
        willAnswer(invocation -> {
                    responseFuture.complete(null);
                    return null;
                })
                .given(responseHandler)
                .onComplete();
        var messages = List.of(
                Messages.run(
                        database(),
                        AccessMode.WRITE,
                        null,
                        Set.of(),
                        "RETURN 1",
                        Map.of(),
                        null,
                        Map.of(),
                        NotificationConfig.defaultConfig()),
                Messages.discard(-1, -1));

        // when
        connection
                .writeAndFlush(responseHandler, messages, mock(Observation.class))
                .thenCompose(ignored -> responseFuture)
                .toCompletableFuture()
                .join();

        // then
        var runSummaryCaptor = ArgumentCaptor.forClass(RunSummary.class);
        var discardSummaryCaptor = ArgumentCaptor.forClass(DiscardSummary.class);
        var responseHandlerInOrder = inOrder(responseHandler);
        responseHandlerInOrder.verify(responseHandler).onRunSummary(runSummaryCaptor.capture());
        responseHandlerInOrder.verify(responseHandler).onDiscardSummary(discardSummaryCaptor.capture());
        responseHandlerInOrder.verify(responseHandler).onComplete();
        then(responseHandler).shouldHaveNoMoreInteractions();

        var runSummary = runSummaryCaptor.getValue();
        assertNotNull(runSummary);
        assertTrue(runSummary.queryId() != 0);
        assertEquals(runSummary.keys(), List.of("1"));
        assertEquals(-1, runSummary.resultAvailableAfter());
        assertEquals(database(), runSummary.databaseName().orElse(null));

        var discardSummary = discardSummaryCaptor.getValue();
        assertNotNull(discardSummary);
        assertTrue(discardSummary.metadata().containsKey("stats"));
        assertTrue(discardSummary.metadata().containsKey("bookmark"));
        assertFalse(discardSummary.metadata().get("bookmark").isEmpty());
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldRunAutocommitAndPullAllLater() {
        // given
        var runResponseHandler = mock(ResponseHandler.class);
        var runResponseFuture = new CompletableFuture<>();
        willAnswer(invocation -> {
                    runResponseFuture.complete(null);
                    return null;
                })
                .given(runResponseHandler)
                .onComplete();
        var runMessage = Messages.run(
                database(),
                AccessMode.WRITE,
                null,
                Set.of(),
                "RETURN 1",
                Map.of(),
                null,
                Map.of(),
                NotificationConfig.defaultConfig());
        connection
                .writeAndFlush(runResponseHandler, runMessage, mock(Observation.class))
                .thenCompose(ignored -> runResponseFuture)
                .toCompletableFuture()
                .join();
        var runSummaryCaptor = ArgumentCaptor.forClass(RunSummary.class);
        then(runResponseHandler).should().onRunSummary(runSummaryCaptor.capture());
        var runSummary = runSummaryCaptor.getValue();

        var pullResponseFuture = new CompletableFuture<>();
        willAnswer(invocation -> {
                    pullResponseFuture.complete(null);
                    return null;
                })
                .given(responseHandler)
                .onComplete();

        // when
        connection
                .writeAndFlush(responseHandler, Messages.pull(runSummary.queryId(), -1), mock(Observation.class))
                .thenCompose(ignored -> pullResponseFuture)
                .toCompletableFuture()
                .join();

        // then
        var valuesCaptor = ArgumentCaptor.forClass(List.class);
        var pullSummaryCaptor = ArgumentCaptor.forClass(PullSummary.class);
        var responseHandlerInOrder = inOrder(responseHandler);
        responseHandlerInOrder.verify(responseHandler).onRecord(valuesCaptor.capture());
        responseHandlerInOrder.verify(responseHandler).onPullSummary(pullSummaryCaptor.capture());
        responseHandlerInOrder.verify(responseHandler).onComplete();
        then(responseHandler).shouldHaveNoMoreInteractions();

        var values = valuesCaptor.getValue();
        assertEquals(List.of(TestValueFactory.INSTANCE.value(1L)), values);

        var pullSummary = pullSummaryCaptor.getValue();
        assertNotNull(pullSummary);
        assertFalse(pullSummary.hasMore());
        assertTrue(pullSummary.metadata().containsKey("stats"));
        assertTrue(pullSummary.metadata().containsKey("bookmark"));
        assertFalse(pullSummary.metadata().get("bookmark").isEmpty());
    }

    @Test
    void shouldBeginTransaction() {
        // given
        var responseFuture = new CompletableFuture<>();
        willAnswer(invocation -> {
                    responseFuture.complete(null);
                    return null;
                })
                .given(responseHandler)
                .onComplete();
        var message = Messages.beginTransaction(
                database(),
                AccessMode.WRITE,
                null,
                Set.of(),
                TransactionType.DEFAULT,
                null,
                Map.of(),
                NotificationConfig.defaultConfig());

        // when
        connection
                .writeAndFlush(responseHandler, message, mock(Observation.class))
                .thenCompose(ignored -> responseFuture)
                .toCompletableFuture()
                .join();

        // then
        var beginSummaryCaptor = ArgumentCaptor.forClass(BeginSummary.class);
        var responseHandlerInOrder = inOrder(responseHandler);
        responseHandlerInOrder.verify(responseHandler).onBeginSummary(beginSummaryCaptor.capture());
        responseHandlerInOrder.verify(responseHandler).onComplete();
        then(responseHandler).shouldHaveNoMoreInteractions();

        var beginSummary = beginSummaryCaptor.getValue();
        assertNotNull(beginSummary);
        assertEquals(database(), beginSummary.databaseName().orElse(null));
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldBeginTransactionAndRunAndPull() {
        // given
        var responseFuture = new CompletableFuture<>();
        willAnswer(invocation -> {
                    responseFuture.complete(null);
                    return null;
                })
                .given(responseHandler)
                .onComplete();
        var messages = List.of(
                Messages.beginTransaction(
                        database(),
                        AccessMode.WRITE,
                        null,
                        Set.of(),
                        TransactionType.DEFAULT,
                        null,
                        Map.of(),
                        NotificationConfig.defaultConfig()),
                Messages.run("RETURN 1", Map.of()),
                Messages.pull(-1, -1));

        // when
        connection
                .writeAndFlush(responseHandler, messages, mock(Observation.class))
                .thenCompose(ignored -> responseFuture)
                .toCompletableFuture()
                .join();

        // then
        var beginSummaryCaptor = ArgumentCaptor.forClass(BeginSummary.class);
        var runSummaryCaptor = ArgumentCaptor.forClass(RunSummary.class);
        var valuesCaptor = ArgumentCaptor.forClass(List.class);
        var pullSummaryCaptor = ArgumentCaptor.forClass(PullSummary.class);
        var responseHandlerInOrder = inOrder(responseHandler);
        responseHandlerInOrder.verify(responseHandler).onBeginSummary(beginSummaryCaptor.capture());
        responseHandlerInOrder.verify(responseHandler).onRunSummary(runSummaryCaptor.capture());
        responseHandlerInOrder.verify(responseHandler).onRecord(valuesCaptor.capture());
        responseHandlerInOrder.verify(responseHandler).onPullSummary(pullSummaryCaptor.capture());
        responseHandlerInOrder.verify(responseHandler).onComplete();
        then(responseHandler).shouldHaveNoMoreInteractions();

        var beginSummary = beginSummaryCaptor.getValue();
        assertNotNull(beginSummary);
        assertEquals(database(), beginSummary.databaseName().orElse(null));

        var runSummary = runSummaryCaptor.getValue();
        assertNotNull(runSummary);
        assertTrue(runSummary.queryId() != 0);
        assertEquals(runSummary.keys(), List.of("1"));
        assertEquals(-1, runSummary.resultAvailableAfter());
        assertEquals(database(), runSummary.databaseName().orElse(null));

        var values = valuesCaptor.getValue();
        assertEquals(List.of(TestValueFactory.INSTANCE.value(1L)), values);

        var pullSummary = pullSummaryCaptor.getValue();
        assertNotNull(pullSummary);
        assertFalse(pullSummary.hasMore());
        assertTrue(pullSummary.metadata().containsKey("stats"));
        assertFalse(pullSummary.metadata().containsKey("bookmark"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldBeginTransactionAndRunAndPullAndCommit() {
        // given
        var responseFuture = new CompletableFuture<>();
        willAnswer(invocation -> {
                    responseFuture.complete(null);
                    return null;
                })
                .given(responseHandler)
                .onComplete();
        var messages = List.of(
                Messages.beginTransaction(
                        database(),
                        AccessMode.WRITE,
                        null,
                        Set.of(),
                        TransactionType.DEFAULT,
                        null,
                        Map.of(),
                        NotificationConfig.defaultConfig()),
                Messages.run("RETURN 1", Map.of()),
                Messages.pull(-1, -1),
                Messages.commit());

        // when
        connection
                .writeAndFlush(responseHandler, messages, mock(Observation.class))
                .thenCompose(ignored -> responseFuture)
                .toCompletableFuture()
                .join();

        // then
        var beginSummaryCaptor = ArgumentCaptor.forClass(BeginSummary.class);
        var runSummaryCaptor = ArgumentCaptor.forClass(RunSummary.class);
        var valuesCaptor = ArgumentCaptor.forClass(List.class);
        var pullSummaryCaptor = ArgumentCaptor.forClass(PullSummary.class);
        var commitSummaryCaptor = ArgumentCaptor.forClass(CommitSummary.class);
        var responseHandlerInOrder = inOrder(responseHandler);
        responseHandlerInOrder.verify(responseHandler).onBeginSummary(beginSummaryCaptor.capture());
        responseHandlerInOrder.verify(responseHandler).onRunSummary(runSummaryCaptor.capture());
        responseHandlerInOrder.verify(responseHandler).onRecord(valuesCaptor.capture());
        responseHandlerInOrder.verify(responseHandler).onPullSummary(pullSummaryCaptor.capture());
        responseHandlerInOrder.verify(responseHandler).onCommitSummary(commitSummaryCaptor.capture());
        responseHandlerInOrder.verify(responseHandler).onComplete();
        then(responseHandler).shouldHaveNoMoreInteractions();

        var beginSummary = beginSummaryCaptor.getValue();
        assertNotNull(beginSummary);
        assertEquals(database(), beginSummary.databaseName().orElse(null));

        var runSummary = runSummaryCaptor.getValue();
        assertNotNull(runSummary);
        assertTrue(runSummary.queryId() != 0);
        assertEquals(runSummary.keys(), List.of("1"));
        assertEquals(-1, runSummary.resultAvailableAfter());
        assertEquals(database(), runSummary.databaseName().orElse(null));

        var values = valuesCaptor.getValue();
        assertEquals(List.of(TestValueFactory.INSTANCE.value(1L)), values);

        var pullSummary = pullSummaryCaptor.getValue();
        assertNotNull(pullSummary);
        assertFalse(pullSummary.hasMore());
        assertTrue(pullSummary.metadata().containsKey("stats"));
        assertFalse(pullSummary.metadata().containsKey("bookmark"));

        var commitSummary = commitSummaryCaptor.getValue();
        assertNotNull(commitSummary);
        assertTrue(commitSummary.bookmark().isPresent());
        assertFalse(commitSummary.bookmark().get().isEmpty());
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldBeginTransactionAndRunAndPullAndRollback() {
        // given
        var responseFuture = new CompletableFuture<>();
        willAnswer(invocation -> {
                    responseFuture.complete(null);
                    return null;
                })
                .given(responseHandler)
                .onComplete();
        var messages = List.of(
                Messages.beginTransaction(
                        database(),
                        AccessMode.WRITE,
                        null,
                        Set.of(),
                        TransactionType.DEFAULT,
                        null,
                        Map.of(),
                        NotificationConfig.defaultConfig()),
                Messages.run("RETURN 1", Map.of()),
                Messages.pull(-1, -1),
                Messages.rollback());

        // when
        connection
                .writeAndFlush(responseHandler, messages, mock(Observation.class))
                .thenCompose(ignored -> responseFuture)
                .toCompletableFuture()
                .join();

        // then
        var beginSummaryCaptor = ArgumentCaptor.forClass(BeginSummary.class);
        var runSummaryCaptor = ArgumentCaptor.forClass(RunSummary.class);
        var valuesCaptor = ArgumentCaptor.forClass(List.class);
        var pullSummaryCaptor = ArgumentCaptor.forClass(PullSummary.class);
        var rollbackSummaryCaptor = ArgumentCaptor.forClass(RollbackSummary.class);
        var responseHandlerInOrder = inOrder(responseHandler);
        responseHandlerInOrder.verify(responseHandler).onBeginSummary(beginSummaryCaptor.capture());
        responseHandlerInOrder.verify(responseHandler).onRunSummary(runSummaryCaptor.capture());
        responseHandlerInOrder.verify(responseHandler).onRecord(valuesCaptor.capture());
        responseHandlerInOrder.verify(responseHandler).onPullSummary(pullSummaryCaptor.capture());
        responseHandlerInOrder.verify(responseHandler).onRollbackSummary(rollbackSummaryCaptor.capture());
        responseHandlerInOrder.verify(responseHandler).onComplete();
        then(responseHandler).shouldHaveNoMoreInteractions();

        var beginSummary = beginSummaryCaptor.getValue();
        assertNotNull(beginSummary);
        assertEquals(database(), beginSummary.databaseName().orElse(null));

        var runSummary = runSummaryCaptor.getValue();
        assertNotNull(runSummary);
        assertTrue(runSummary.queryId() != 0);
        assertEquals(runSummary.keys(), List.of("1"));
        assertEquals(-1, runSummary.resultAvailableAfter());
        assertEquals(database(), runSummary.databaseName().orElse(null));

        var values = valuesCaptor.getValue();
        assertEquals(List.of(TestValueFactory.INSTANCE.value(1L)), values);

        var pullSummary = pullSummaryCaptor.getValue();
        assertNotNull(pullSummary);
        assertFalse(pullSummary.hasMore());
        assertTrue(pullSummary.metadata().containsKey("stats"));
        assertFalse(pullSummary.metadata().containsKey("bookmark"));

        var rollbackSummary = rollbackSummaryCaptor.getValue();
        assertNotNull(rollbackSummary);
    }

    @Test
    void shouldRunAutocommitAndPullAndDiscardAllWithFailure() {
        // given
        var responseFuture = new CompletableFuture<>();
        willAnswer(invocation -> {
                    responseFuture.complete(null);
                    return null;
                })
                .given(responseHandler)
                .onComplete();
        var messages = List.of(
                Messages.run(
                        database(),
                        AccessMode.WRITE,
                        null,
                        Set.of(),
                        "RETURN",
                        Map.of(),
                        null,
                        Map.of(),
                        NotificationConfig.defaultConfig()),
                Messages.pull(-1, 100),
                Messages.discard(-1, -1));

        // when
        connection
                .writeAndFlush(responseHandler, messages, mock(Observation.class))
                .thenCompose(ignored -> responseFuture)
                .toCompletableFuture()
                .join();

        // then
        var throwableCaptor = ArgumentCaptor.forClass(Throwable.class);
        var valuesCaptor = ArgumentCaptor.forClass(Value[].class);
        var pullSummaryCaptor = ArgumentCaptor.forClass(PullSummary.class);
        var responseHandlerInOrder = inOrder(responseHandler);
        responseHandlerInOrder.verify(responseHandler).onError(throwableCaptor.capture());
        responseHandlerInOrder.verify(responseHandler, times(2)).onIgnored();
        responseHandlerInOrder.verify(responseHandler).onComplete();
        then(responseHandler).shouldHaveNoMoreInteractions();

        var exception = throwableCaptor.getValue();
        assertNotNull(exception);

        assertEquals(BoltConnectionState.FAILURE, connection.state());
    }

    @Test
    void shouldResetAfterFailure() {
        // given
        var responseFuture = new CompletableFuture<>();
        willAnswer(invocation -> {
                    responseFuture.complete(null);
                    return null;
                })
                .given(responseHandler)
                .onComplete();
        var message = Messages.run(
                database(),
                AccessMode.WRITE,
                null,
                Set.of(),
                "not query",
                Map.of(),
                null,
                Map.of(),
                NotificationConfig.defaultConfig());
        connection
                .writeAndFlush(responseHandler, message, mock(Observation.class))
                .thenCompose(ignored -> responseFuture)
                .toCompletableFuture()
                .join();
        then(responseHandler).should().onError(any(BoltFailureException.class));
        then(responseHandler).should().onComplete();
        assertEquals(BoltConnectionState.FAILURE, connection.state());
        Mockito.reset(responseHandler);
        var resetResponseFuture = new CompletableFuture<>();
        willAnswer(invocation -> {
                    resetResponseFuture.complete(null);
                    return null;
                })
                .given(responseHandler)
                .onComplete();

        // when
        connection
                .writeAndFlush(responseHandler, Messages.reset(), mock(Observation.class))
                .thenCompose(ignored -> resetResponseFuture)
                .toCompletableFuture()
                .join();

        // then
        then(responseHandler).should().onComplete();
        assertEquals(BoltConnectionState.OPEN, connection.state());
    }

    @Test
    void shouldUpdateAuthToken() {
        // given
        var responseFuture = new CompletableFuture<>();
        willAnswer(invocation -> {
                    responseFuture.complete(null);
                    return null;
                })
                .given(responseHandler)
                .onComplete();
        var messages = List.of(
                Messages.logoff(),
                Messages.logon(AuthTokens.basic(username(), password() + "typo", "basic", TestValueFactory.INSTANCE)),
                Messages.run(
                        database(),
                        AccessMode.WRITE,
                        null,
                        Set.of(),
                        "RETURN 1",
                        Map.of(),
                        null,
                        Map.of(),
                        NotificationConfig.defaultConfig()));

        // when
        connection
                .writeAndFlush(responseHandler, messages, mock(Observation.class))
                .thenCompose(ignored -> responseFuture)
                .toCompletableFuture()
                .join();

        // then
        var boltFailureExceptionArgumentCaptor = ArgumentCaptor.forClass(BoltFailureException.class);
        var responseHandlerInOrder = inOrder(responseHandler);
        responseHandlerInOrder.verify(responseHandler).onLogoffSummary(any());
        responseHandlerInOrder.verify(responseHandler).onLogonSummary(any());
        responseHandlerInOrder.verify(responseHandler).onError(boltFailureExceptionArgumentCaptor.capture());
        responseHandlerInOrder.verify(responseHandler).onComplete();
        then(responseHandler).shouldHaveNoMoreInteractions();
        var error = boltFailureExceptionArgumentCaptor.getValue();
        assertEquals("Neo.ClientError.Security.Unauthorized", error.code());
        assertEquals(BoltConnectionState.FAILURE, connection.state());
    }

    @Test
    void shouldTimeout() {
        // given
        var timeoutMillis = 100;
        var responseFuture = new CompletableFuture<>();
        willAnswer(invocation -> {
                    responseFuture.complete(null);
                    return null;
                })
                .given(responseHandler)
                .onComplete();
        var message = Messages.run(
                database(),
                AccessMode.WRITE,
                null,
                Set.of(),
                "CALL apoc.util.sleep(%d)".formatted(timeoutMillis * 2),
                Map.of(),
                null,
                Map.of(),
                NotificationConfig.defaultConfig());
        connection.setReadTimeout(Duration.ofMillis(timeoutMillis));

        // when
        connection
                .writeAndFlush(responseHandler, message, mock(Observation.class))
                .thenCompose(ignored -> responseFuture)
                .toCompletableFuture()
                .join();

        // then
        var responseHandlerInOrder = inOrder(responseHandler);
        responseHandlerInOrder.verify(responseHandler).onError(any(BoltConnectionReadTimeoutException.class));
        responseHandlerInOrder.verify(responseHandler).onComplete();
        then(responseHandler).shouldHaveNoMoreInteractions();
    }

    abstract URI uri();

    abstract String database();

    abstract String username();

    abstract String password();
}
