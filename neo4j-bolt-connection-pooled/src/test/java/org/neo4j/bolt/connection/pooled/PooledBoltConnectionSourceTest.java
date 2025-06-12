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
package org.neo4j.bolt.connection.pooled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.MockitoAnnotations.openMocks;

import java.net.URI;
import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.BDDMockito;
import org.mockito.Mock;
import org.mockito.stubbing.Answer;
import org.neo4j.bolt.connection.AccessMode;
import org.neo4j.bolt.connection.AuthInfo;
import org.neo4j.bolt.connection.AuthToken;
import org.neo4j.bolt.connection.AuthTokens;
import org.neo4j.bolt.connection.BoltAgent;
import org.neo4j.bolt.connection.BoltConnection;
import org.neo4j.bolt.connection.BoltConnectionParameters;
import org.neo4j.bolt.connection.BoltConnectionProvider;
import org.neo4j.bolt.connection.BoltConnectionState;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.DatabaseName;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.MetricsListener;
import org.neo4j.bolt.connection.NotificationConfig;
import org.neo4j.bolt.connection.ResponseHandler;
import org.neo4j.bolt.connection.SecurityPlan;
import org.neo4j.bolt.connection.exception.MinVersionAcquisitionException;
import org.neo4j.bolt.connection.message.Messages;
import org.neo4j.bolt.connection.pooled.impl.PooledBoltConnection;
import org.neo4j.bolt.connection.summary.ResetSummary;
import org.neo4j.bolt.connection.values.Value;

class PooledBoltConnectionSourceTest {
    PooledBoltConnectionSource boltConnectionSource;

    @Mock
    BoltConnectionProvider upstreamProvider;

    @Mock
    LoggingProvider loggingProvider;

    @Mock
    Clock clock;

    @Mock
    MetricsListener metricsListener;

    @Mock
    Consumer<DatabaseName> databaseNameConsumer;

    @Mock
    BoltConnection connection;

    @Mock
    AuthTokenManager authTokenManager;

    @Mock
    SecurityPlanSupplier securityPlanSupplier;

    final int maxSize = 2;
    final long acquisitionTimeout = 5000;
    final long maxLifetime = 60000;
    final long idleBeforeTest = 30000;
    final URI uri = URI.create("bolt://localhost:7687");
    final String routingContextAddress = "%s:%d".formatted(uri.getHost(), uri.getPort());
    final BoltAgent boltAgent = new BoltAgent("agent", null, null, null);
    final String userAgent = "agent";
    final int timeout = 1000;

    final SecurityPlan securityPlan = null;
    final DatabaseName databaseName = DatabaseName.defaultDatabase();
    final AccessMode mode = AccessMode.WRITE;
    final Set<String> bookmarks = Set.of("bookmark1", "bookmark2");
    final BoltProtocolVersion minVersion = new BoltProtocolVersion(5, 6);
    final NotificationConfig notificationConfig = NotificationConfig.defaultConfig();
    final AuthToken authToken = AuthTokens.custom(Collections.emptyMap());

    @BeforeEach
    @SuppressWarnings("resource")
    void beforeEach() {
        openMocks(this);
        given(loggingProvider.getLog(any(Class.class))).willReturn(mock(System.Logger.class));
        given(authTokenManager.getToken()).willReturn(CompletableFuture.completedStage(authToken));
        given(securityPlanSupplier.getPlan()).willReturn(CompletableFuture.completedStage(securityPlan));
        boltConnectionSource = new PooledBoltConnectionSource(
                loggingProvider,
                clock,
                uri,
                upstreamProvider,
                authTokenManager,
                securityPlanSupplier,
                maxSize,
                acquisitionTimeout,
                maxLifetime,
                idleBeforeTest,
                metricsListener,
                routingContextAddress,
                boltAgent,
                userAgent,
                timeout,
                notificationConfig);
        given(upstreamProvider.connect(
                        eq(uri),
                        eq(routingContextAddress),
                        eq(boltAgent),
                        eq(userAgent),
                        eq(timeout),
                        eq(securityPlan),
                        eq(authToken),
                        eq(null),
                        eq(notificationConfig)))
                .willReturn(CompletableFuture.completedStage(connection));
    }

    @Test
    void shouldCreateNewConnection() {
        // when
        var connection =
                boltConnectionSource.getConnection().toCompletableFuture().join();

        // then
        var pooledConnection = assertInstanceOf(PooledBoltConnection.class, connection);
        assertEquals(this.connection, pooledConnection.delegate());
        then(upstreamProvider)
                .should()
                .connect(
                        eq(uri),
                        eq(routingContextAddress),
                        eq(boltAgent),
                        eq(userAgent),
                        eq(timeout),
                        eq(securityPlan),
                        eq(authToken),
                        eq(null),
                        eq(notificationConfig));
        assertEquals(1, boltConnectionSource.inUse());
        assertEquals(1, boltConnectionSource.size());
    }

    @Test
    void shouldTimeout() {
        // given
        var acquisitionTimeout = TimeUnit.SECONDS.toMillis(1);
        boltConnectionSource = new PooledBoltConnectionSource(
                loggingProvider,
                clock,
                uri,
                upstreamProvider,
                authTokenManager,
                securityPlanSupplier,
                1,
                acquisitionTimeout,
                maxLifetime,
                idleBeforeTest,
                metricsListener,
                routingContextAddress,
                boltAgent,
                userAgent,
                timeout,
                notificationConfig);
        boltConnectionSource.getConnection().toCompletableFuture().join();

        // when
        var connectionStage = boltConnectionSource.getConnection();

        // then
        var completionException = assertThrows(
                CompletionException.class,
                () -> connectionStage.toCompletableFuture().join());
        assertInstanceOf(TimeoutException.class, completionException.getCause());
    }

    @Test
    void shouldReturnConnectionToPool() {
        // given
        given(connection.writeAndFlush(any(), eq(Messages.reset())))
                .willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
                    var handler = (ResponseHandler) invocationOnMock.getArgument(0);
                    handler.onResetSummary(mock(ResetSummary.class));
                    handler.onComplete();
                    return CompletableFuture.completedStage(null);
                });
        var connection =
                boltConnectionSource.getConnection().toCompletableFuture().join();

        // when
        connection.close().toCompletableFuture().join();

        // then
        assertEquals(0, boltConnectionSource.inUse());
        assertEquals(1, boltConnectionSource.size());
    }

    @Test
    void shouldUseExistingConnection() {
        // given
        given(connection.writeAndFlush(any(), eq(Messages.reset())))
                .willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
                    var handler = (ResponseHandler) invocationOnMock.getArgument(0);
                    handler.onResetSummary(mock(ResetSummary.class));
                    handler.onComplete();
                    return CompletableFuture.completedStage(null);
                });
        given(connection.state()).willReturn(BoltConnectionState.OPEN);
        given(connection.protocolVersion()).willReturn(minVersion);
        var authInfo = mock(AuthInfo.class);
        given(authInfo.authAckMillis()).willReturn(0L);
        given(authInfo.authToken()).willReturn(AuthTokens.custom(Collections.emptyMap()));
        given(connection.authInfo()).willReturn(CompletableFuture.completedStage(authInfo));
        boltConnectionSource
                .getConnection()
                .toCompletableFuture()
                .join()
                .close()
                .toCompletableFuture()
                .join();
        BDDMockito.reset(upstreamProvider);

        // when
        var connection =
                boltConnectionSource.getConnection().toCompletableFuture().join();

        // then
        var pooledConnection = assertInstanceOf(PooledBoltConnection.class, connection);
        assertEquals(this.connection, pooledConnection.delegate());
        then(upstreamProvider).shouldHaveNoInteractions();
        assertEquals(1, boltConnectionSource.inUse());
        assertEquals(1, boltConnectionSource.size());
    }

    @Test
    void shouldClose() {
        // given
        given(connection.writeAndFlush(any(), eq(Messages.reset())))
                .willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
                    var handler = (ResponseHandler) invocationOnMock.getArgument(0);
                    handler.onResetSummary(mock(ResetSummary.class));
                    handler.onComplete();
                    return CompletableFuture.completedStage(null);
                });
        given(connection.state()).willReturn(BoltConnectionState.OPEN);
        given(connection.close()).willReturn(CompletableFuture.completedStage(null));
        given(upstreamProvider.close()).willReturn(CompletableFuture.completedStage(null));
        boltConnectionSource
                .getConnection()
                .toCompletableFuture()
                .join()
                .close()
                .toCompletableFuture()
                .join();

        // when
        boltConnectionSource.close().toCompletableFuture().join();

        // then
        then(connection).should().close();
        then(upstreamProvider).should().close();
    }

    @Test
    void shouldVerifyConnectivity() {
        // given
        given(connection.writeAndFlush(any(), eq(Messages.reset())))
                .willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
                    var handler = (ResponseHandler) invocationOnMock.getArgument(0);
                    handler.onResetSummary(mock(ResetSummary.class));
                    handler.onComplete();
                    return CompletableFuture.completedStage(null);
                });

        // when
        boltConnectionSource.verifyConnectivity().toCompletableFuture().join();

        // then
        then(upstreamProvider)
                .should()
                .connect(
                        eq(uri),
                        eq(routingContextAddress),
                        eq(boltAgent),
                        eq(userAgent),
                        eq(timeout),
                        eq(securityPlan),
                        eq(authToken),
                        eq(null),
                        eq(notificationConfig));
        then(connection).should().writeAndFlush(any(), eq(Messages.reset()));
    }

    @ParameterizedTest
    @MethodSource("supportsMultiDbParams")
    void shouldSupportMultiDb(BoltProtocolVersion boltProtocolVersion, boolean expectedToSupport) {
        // given
        given(connection.protocolVersion()).willReturn(boltProtocolVersion);
        given(connection.writeAndFlush(any(), eq(Messages.reset())))
                .willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
                    var handler = (ResponseHandler) invocationOnMock.getArgument(0);
                    handler.onResetSummary(mock(ResetSummary.class));
                    handler.onComplete();
                    return CompletableFuture.completedStage(null);
                });

        // when
        var supports =
                boltConnectionSource.supportsMultiDb().toCompletableFuture().join();

        // then
        assertEquals(expectedToSupport, supports);
        then(upstreamProvider)
                .should()
                .connect(
                        eq(uri),
                        eq(routingContextAddress),
                        eq(boltAgent),
                        eq(userAgent),
                        eq(timeout),
                        eq(securityPlan),
                        eq(authToken),
                        eq(null),
                        eq(notificationConfig));
        then(connection).should().writeAndFlush(any(), eq(Messages.reset()));
    }

    private static Stream<Arguments> supportsMultiDbParams() {
        return Stream.of(
                Arguments.arguments(new BoltProtocolVersion(4, 0), true),
                Arguments.arguments(new BoltProtocolVersion(3, 5), false));
    }

    @ParameterizedTest
    @MethodSource("supportsSessionAuthParams")
    void shouldSupportsSessionAuth(BoltProtocolVersion boltProtocolVersion, boolean expectedToSupport) {
        // given
        given(connection.protocolVersion()).willReturn(boltProtocolVersion);
        given(connection.writeAndFlush(any(), eq(Messages.reset())))
                .willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
                    var handler = (ResponseHandler) invocationOnMock.getArgument(0);
                    handler.onResetSummary(mock(ResetSummary.class));
                    handler.onComplete();
                    return CompletableFuture.completedStage(null);
                });

        // when
        var supports =
                boltConnectionSource.supportsSessionAuth().toCompletableFuture().join();

        // then
        assertEquals(expectedToSupport, supports);
        then(upstreamProvider)
                .should()
                .connect(
                        eq(uri),
                        eq(routingContextAddress),
                        eq(boltAgent),
                        eq(userAgent),
                        eq(timeout),
                        eq(securityPlan),
                        eq(authToken),
                        eq(null),
                        eq(notificationConfig));
        then(connection).should().writeAndFlush(any(), eq(Messages.reset()));
    }

    private static Stream<Arguments> supportsSessionAuthParams() {
        return Stream.of(
                Arguments.arguments(new BoltProtocolVersion(5, 1), true),
                Arguments.arguments(new BoltProtocolVersion(5, 0), false));
    }

    @Test
    void shouldThrowOnLowerVersion() {
        // given
        given(connection.protocolVersion()).willReturn(new BoltProtocolVersion(5, 0));
        given(connection.state()).willReturn(BoltConnectionState.OPEN);
        given(connection.writeAndFlush(any(), eq(Messages.reset())))
                .willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
                    var handler = (ResponseHandler) invocationOnMock.getArgument(0);
                    handler.onResetSummary(mock(ResetSummary.class));
                    handler.onComplete();
                    return CompletableFuture.completedStage(null);
                });
        boltConnectionSource
                .getConnection()
                .toCompletableFuture()
                .join()
                .close()
                .toCompletableFuture()
                .join();

        // when
        var parameters = BoltConnectionParameters.builder()
                .withMinVersion(new BoltProtocolVersion(5, 5))
                .build();
        var future = boltConnectionSource.getConnection(parameters).toCompletableFuture();

        // then
        var exception = assertThrows(CompletionException.class, future::join);
        assertInstanceOf(MinVersionAcquisitionException.class, exception.getCause());
    }

    @Test
    void shouldTestMaxLifetime() {
        // given
        given(connection.protocolVersion()).willReturn(minVersion);
        given(connection.state()).willReturn(BoltConnectionState.OPEN);
        given(connection.writeAndFlush(any(), eq(Messages.reset())))
                .willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
                    var handler = (ResponseHandler) invocationOnMock.getArgument(0);
                    handler.onResetSummary(mock(ResetSummary.class));
                    handler.onComplete();
                    return CompletableFuture.completedStage(null);
                });
        var connection2 = mock(BoltConnection.class);
        given(upstreamProvider.connect(
                        eq(uri),
                        eq(routingContextAddress),
                        eq(boltAgent),
                        eq(userAgent),
                        eq(timeout),
                        eq(securityPlan),
                        eq(authToken),
                        eq(null),
                        eq(notificationConfig)))
                .willReturn(CompletableFuture.completedStage(connection))
                .willReturn(CompletableFuture.completedStage(connection2));
        boltConnectionSource
                .getConnection()
                .toCompletableFuture()
                .join()
                .close()
                .toCompletableFuture()
                .join();
        given(clock.millis()).willReturn(maxLifetime + 1);

        // when
        var anotherConnection =
                boltConnectionSource.getConnection().toCompletableFuture().join();

        // then
        assertEquals(1, boltConnectionSource.inUse());
        assertEquals(1, boltConnectionSource.size());
        assertEquals(connection2, ((PooledBoltConnection) anotherConnection).delegate());
        then(connection).should().close();
    }

    @Test
    void shouldTestLiveness() {
        // given
        given(connection.protocolVersion()).willReturn(minVersion);
        given(connection.state()).willReturn(BoltConnectionState.OPEN);
        given(connection.writeAndFlush(any(), eq(Messages.reset())))
                .willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
                    var handler = (ResponseHandler) invocationOnMock.getArgument(0);
                    handler.onResetSummary(mock(ResetSummary.class));
                    handler.onComplete();
                    return CompletableFuture.completedStage(null);
                });
        var authInfo = mock(AuthInfo.class);
        given(authInfo.authAckMillis()).willReturn(0L);
        given(authInfo.authToken()).willReturn(AuthTokens.custom(Collections.emptyMap()));
        given(connection.authInfo()).willReturn(CompletableFuture.completedStage(authInfo));
        boltConnectionSource
                .getConnection()
                .toCompletableFuture()
                .join()
                .close()
                .toCompletableFuture()
                .join();
        given(clock.millis()).willReturn(idleBeforeTest + 1);

        // when
        var actualConnection =
                boltConnectionSource.getConnection().toCompletableFuture().join();

        // then
        assertEquals(connection, ((PooledBoltConnection) actualConnection).delegate());
        then(connection).should(times(2)).writeAndFlush(any(), eq(Messages.reset()));
    }

    @Test
    void shouldPipelineReauth() {
        // given
        given(connection.protocolVersion()).willReturn(minVersion);
        given(connection.state()).willReturn(BoltConnectionState.OPEN);
        given(connection.writeAndFlush(any(), eq(Messages.reset())))
                .willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
                    var handler = (ResponseHandler) invocationOnMock.getArgument(0);
                    handler.onResetSummary(mock(ResetSummary.class));
                    handler.onComplete();
                    return CompletableFuture.completedStage(null);
                });
        var authMap = Map.of("key", mock(Value.class));
        var authToken = AuthTokens.custom(authMap);
        given(connection.write(List.of(Messages.logoff(), Messages.logon(authToken))))
                .willReturn(CompletableFuture.completedStage(null));
        var authInfo = mock(AuthInfo.class);
        given(authInfo.authAckMillis()).willReturn(0L);
        given(authInfo.authToken()).willReturn(AuthTokens.custom(Collections.emptyMap()));
        given(connection.authInfo()).willReturn(CompletableFuture.completedStage(authInfo));
        given(authTokenManager.getToken())
                .willReturn(CompletableFuture.completedStage(AuthTokens.custom(Collections.emptyMap())))
                .willReturn(CompletableFuture.completedStage(authToken));
        boltConnectionSource
                .getConnection()
                .toCompletableFuture()
                .join()
                .close()
                .toCompletableFuture()
                .join();

        // when
        var actualConnection =
                boltConnectionSource.getConnection().toCompletableFuture().join();

        // then
        assertEquals(connection, ((PooledBoltConnection) actualConnection).delegate());
        then(connection).should().write(List.of(Messages.logoff(), Messages.logon(authToken)));
    }
}
