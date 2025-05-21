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

class PooledBoltConnectionSourceTest {
    //    PooledBoltConnectionSource provider;
    //
    //    @Mock
    //    BoltConnectionProvider upstreamProvider;
    //
    //    @Mock
    //    LoggingProvider loggingProvider;
    //
    //    @Mock
    //    Clock clock;
    //
    //    @Mock
    //    MetricsListener metricsListener;
    //
    //    @Mock
    //    Consumer<DatabaseName> databaseNameConsumer;
    //
    //    @Mock
    //    BoltConnection connection;
    //
    //    @Mock
    //    Supplier<CompletionStage<AuthToken>> authTokenStageSupplier;
    //
    //    final int maxSize = 2;
    //    final long acquisitionTimeout = 5000;
    //    final long maxLifetime = 60000;
    //    final long idleBeforeTest = 30000;
    //    final BoltServerAddress address = BoltServerAddress.LOCAL_DEFAULT;
    //    final RoutingContext context = RoutingContext.EMPTY;
    //    final BoltAgent boltAgent = new BoltAgent("agent", null, null, null);
    //    final String userAgent = "agent";
    //    final int timeout = 1000;
    //
    //    final SecurityPlan securityPlan = SecurityPlans.unencrypted();
    //    final DatabaseName databaseName = DatabaseNameUtil.defaultDatabase();
    //    final AccessMode mode = AccessMode.WRITE;
    //    final Set<String> bookmarks = Set.of("bookmark1", "bookmark2");
    //    final BoltProtocolVersion minVersion = new BoltProtocolVersion(5, 6);
    //    final NotificationConfig notificationConfig = NotificationConfig.defaultConfig();
    //
    //    @BeforeEach
    //    @SuppressWarnings("resource")
    //    void beforeEach() {
    //        openMocks(this);
    //        given(loggingProvider.getLog(any(Class.class))).willReturn(mock(System.Logger.class));
    //        given(authTokenStageSupplier.get())
    //                .willReturn(CompletableFuture.completedStage(AuthTokens.custom(Collections.emptyMap())));
    //        provider = new PooledBoltConnectionSource(
    //                upstreamProvider,
    //                maxSize,
    //                acquisitionTimeout,
    //                maxLifetime,
    //                idleBeforeTest,
    //                clock,
    //                loggingProvider,
    //                metricsListener,
    //                address,
    //                context,
    //                boltAgent,
    //                userAgent,
    //                timeout);
    //    }
    //
    //    @Test
    //    void shouldCreateNewConnection() {
    //        // given
    //        given(upstreamProvider.connect(
    //                        eq(address),
    //                        eq(context),
    //                        eq(boltAgent),
    //                        eq(userAgent),
    //                        eq(timeout),
    //                        eq(securityPlan),
    //                        eq(databaseName),
    //                        any(),
    //                        eq(mode),
    //                        eq(bookmarks),
    //                        eq(null),
    //                        eq(minVersion),
    //                        eq(notificationConfig),
    //                        any(),
    //                        any()))
    //                .willReturn(CompletableFuture.completedStage(connection));
    //
    //        // when
    //        var connection = provider.connect(
    //                        address,
    //                        context,
    //                        boltAgent,
    //                        userAgent,
    //                        timeout,
    //                        securityPlan,
    //                        databaseName,
    //                        authTokenStageSupplier,
    //                        mode,
    //                        bookmarks,
    //                        null,
    //                        minVersion,
    //                        notificationConfig,
    //                        databaseNameConsumer,
    //                        Collections.emptyMap())
    //                .toCompletableFuture()
    //                .join();
    //
    //        // then
    //        var pooledConnection = assertInstanceOf(PooledBoltConnection.class, connection);
    //        assertEquals(this.connection, pooledConnection.delegate());
    //        then(upstreamProvider)
    //                .should()
    //                .connect(
    //                        eq(address),
    //                        eq(context),
    //                        eq(boltAgent),
    //                        eq(userAgent),
    //                        eq(timeout),
    //                        eq(securityPlan),
    //                        eq(databaseName),
    //                        any(),
    //                        eq(mode),
    //                        eq(bookmarks),
    //                        eq(null),
    //                        eq(minVersion),
    //                        eq(notificationConfig),
    //                        any(),
    //                        any());
    //        assertEquals(1, provider.inUse());
    //        assertEquals(1, provider.size());
    //    }
    //
    //    @Test
    //    void shouldTimeout() {
    //        // given
    //        var acquisitionTimeout = TimeUnit.SECONDS.toMillis(1);
    //        given(upstreamProvider.connect(
    //                        eq(address),
    //                        eq(context),
    //                        eq(boltAgent),
    //                        eq(userAgent),
    //                        eq(timeout),
    //                        eq(securityPlan),
    //                        eq(databaseName),
    //                        any(),
    //                        eq(mode),
    //                        eq(bookmarks),
    //                        eq(null),
    //                        eq(minVersion),
    //                        eq(notificationConfig),
    //                        any(),
    //                        any()))
    //                .willReturn(CompletableFuture.completedStage(connection));
    //        provider = new PooledBoltConnectionSource(
    //                upstreamProvider,
    //                1,
    //                acquisitionTimeout,
    //                maxLifetime,
    //                idleBeforeTest,
    //                clock,
    //                loggingProvider,
    //                metricsListener,
    //                address,
    //                context,
    //                boltAgent,
    //                userAgent,
    //                timeout);
    //        provider.connect(
    //                        address,
    //                        context,
    //                        boltAgent,
    //                        userAgent,
    //                        timeout,
    //                        securityPlan,
    //                        databaseName,
    //                        authTokenStageSupplier,
    //                        mode,
    //                        bookmarks,
    //                        null,
    //                        minVersion,
    //                        notificationConfig,
    //                        databaseNameConsumer,
    //                        Collections.emptyMap())
    //                .toCompletableFuture()
    //                .join();
    //
    //        // when
    //        var connectionStage = provider.connect(
    //                address,
    //                context,
    //                boltAgent,
    //                userAgent,
    //                timeout,
    //                securityPlan,
    //                databaseName,
    //                authTokenStageSupplier,
    //                mode,
    //                bookmarks,
    //                null,
    //                minVersion,
    //                notificationConfig,
    //                databaseNameConsumer,
    //                Collections.emptyMap());
    //
    //        // then
    //        var completionException = assertThrows(
    //                CompletionException.class,
    //                () -> connectionStage.toCompletableFuture().join());
    //        assertInstanceOf(TimeoutException.class, completionException.getCause());
    //    }
    //
    //    @Test
    //    void shouldReturnConnectionToPool() {
    //        // given
    //        given(connection.writeAndFlush(any(), eq(Messages.reset())))
    //                .willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
    //                    var handler = (ResponseHandler) invocationOnMock.getArgument(0);
    //                    handler.onResetSummary(mock(ResetSummary.class));
    //                    handler.onComplete();
    //                    return CompletableFuture.completedStage(null);
    //                });
    //        given(upstreamProvider.connect(
    //                        eq(address),
    //                        eq(context),
    //                        eq(boltAgent),
    //                        eq(userAgent),
    //                        eq(timeout),
    //                        eq(securityPlan),
    //                        eq(databaseName),
    //                        any(),
    //                        eq(mode),
    //                        eq(bookmarks),
    //                        eq(null),
    //                        eq(minVersion),
    //                        eq(notificationConfig),
    //                        any(),
    //                        any()))
    //                .willReturn(CompletableFuture.completedStage(connection));
    //        var connection = provider.connect(
    //                        address,
    //                        context,
    //                        boltAgent,
    //                        userAgent,
    //                        timeout,
    //                        securityPlan,
    //                        databaseName,
    //                        authTokenStageSupplier,
    //                        mode,
    //                        bookmarks,
    //                        null,
    //                        minVersion,
    //                        notificationConfig,
    //                        databaseNameConsumer,
    //                        Collections.emptyMap())
    //                .toCompletableFuture()
    //                .join();
    //
    //        // when
    //        connection.close().toCompletableFuture().join();
    //
    //        // then
    //        assertEquals(0, provider.inUse());
    //        assertEquals(1, provider.size());
    //    }
    //
    //    @Test
    //    void shouldUseExistingConnection() {
    //        // given
    //        given(connection.writeAndFlush(any(), eq(Messages.reset())))
    //                .willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
    //                    var handler = (ResponseHandler) invocationOnMock.getArgument(0);
    //                    handler.onResetSummary(mock(ResetSummary.class));
    //                    handler.onComplete();
    //                    return CompletableFuture.completedStage(null);
    //                });
    //        given(connection.state()).willReturn(BoltConnectionState.OPEN);
    //        given(connection.protocolVersion()).willReturn(minVersion);
    //        var authInfo = mock(AuthInfo.class);
    //        given(authInfo.authAckMillis()).willReturn(0L);
    //        given(authInfo.authToken()).willReturn(AuthTokens.custom(Collections.emptyMap()));
    //        given(connection.authInfo()).willReturn(CompletableFuture.completedStage(authInfo));
    //        given(upstreamProvider.connect(
    //                        eq(address),
    //                        eq(context),
    //                        eq(boltAgent),
    //                        eq(userAgent),
    //                        eq(timeout),
    //                        eq(securityPlan),
    //                        eq(databaseName),
    //                        any(),
    //                        eq(mode),
    //                        eq(bookmarks),
    //                        eq(null),
    //                        eq(minVersion),
    //                        eq(notificationConfig),
    //                        any(),
    //                        any()))
    //                .willReturn(CompletableFuture.completedStage(connection));
    //        provider.connect(
    //                        address,
    //                        context,
    //                        boltAgent,
    //                        userAgent,
    //                        timeout,
    //                        securityPlan,
    //                        databaseName,
    //                        authTokenStageSupplier,
    //                        mode,
    //                        bookmarks,
    //                        null,
    //                        minVersion,
    //                        notificationConfig,
    //                        databaseNameConsumer,
    //                        Collections.emptyMap())
    //                .toCompletableFuture()
    //                .join()
    //                .close()
    //                .toCompletableFuture()
    //                .join();
    //        BDDMockito.reset(upstreamProvider);
    //
    //        // when
    //        var connection = provider.connect(
    //                        address,
    //                        context,
    //                        boltAgent,
    //                        userAgent,
    //                        timeout,
    //                        securityPlan,
    //                        databaseName,
    //                        authTokenStageSupplier,
    //                        mode,
    //                        bookmarks,
    //                        null,
    //                        minVersion,
    //                        notificationConfig,
    //                        databaseNameConsumer,
    //                        Collections.emptyMap())
    //                .toCompletableFuture()
    //                .join();
    //
    //        // then
    //        var pooledConnection = assertInstanceOf(PooledBoltConnection.class, connection);
    //        assertEquals(this.connection, pooledConnection.delegate());
    //        then(upstreamProvider).shouldHaveNoInteractions();
    //        assertEquals(1, provider.inUse());
    //        assertEquals(1, provider.size());
    //    }
    //
    //    @Test
    //    void shouldClose() {
    //        // given
    //        given(connection.writeAndFlush(any(), eq(Messages.reset())))
    //                .willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
    //                    var handler = (ResponseHandler) invocationOnMock.getArgument(0);
    //                    handler.onResetSummary(mock(ResetSummary.class));
    //                    handler.onComplete();
    //                    return CompletableFuture.completedStage(null);
    //                });
    //        given(connection.state()).willReturn(BoltConnectionState.OPEN);
    //        given(connection.close()).willReturn(CompletableFuture.completedStage(null));
    //        given(upstreamProvider.close()).willReturn(CompletableFuture.completedStage(null));
    //        given(upstreamProvider.connect(
    //                        eq(address),
    //                        eq(context),
    //                        eq(boltAgent),
    //                        eq(userAgent),
    //                        eq(timeout),
    //                        eq(securityPlan),
    //                        eq(databaseName),
    //                        any(),
    //                        eq(mode),
    //                        eq(bookmarks),
    //                        eq(null),
    //                        eq(minVersion),
    //                        eq(notificationConfig),
    //                        any(),
    //                        any()))
    //                .willReturn(CompletableFuture.completedStage(connection));
    //        provider.connect(
    //                        address,
    //                        context,
    //                        boltAgent,
    //                        userAgent,
    //                        timeout,
    //                        securityPlan,
    //                        databaseName,
    //                        authTokenStageSupplier,
    //                        mode,
    //                        bookmarks,
    //                        null,
    //                        minVersion,
    //                        notificationConfig,
    //                        databaseNameConsumer,
    //                        Collections.emptyMap())
    //                .toCompletableFuture()
    //                .join()
    //                .close()
    //                .toCompletableFuture()
    //                .join();
    //
    //        // when
    //        provider.close().toCompletableFuture().join();
    //
    //        // then
    //        then(connection).should().close();
    //        then(upstreamProvider).should().close();
    //    }
    //
    //    @Test
    //    void shouldVerifyConnectivity() {
    //        // given
    //        given(connection.writeAndFlush(any(), eq(Messages.reset())))
    //                .willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
    //                    var handler = (ResponseHandler) invocationOnMock.getArgument(0);
    //                    handler.onResetSummary(mock(ResetSummary.class));
    //                    handler.onComplete();
    //                    return CompletableFuture.completedStage(null);
    //                });
    //        given(upstreamProvider.connect(
    //                        eq(address),
    //                        eq(context),
    //                        eq(boltAgent),
    //                        eq(userAgent),
    //                        eq(timeout),
    //                        eq(securityPlan),
    //                        eq(null),
    //                        any(),
    //                        eq(AccessMode.WRITE),
    //                        eq(Collections.emptySet()),
    //                        eq(null),
    //                        eq(null),
    //                        eq(null),
    //                        any(),
    //                        any()))
    //                .willReturn(CompletableFuture.completedStage(connection));
    //
    //        // when
    //        provider.verifyConnectivity(
    //                        address,
    //                        context,
    //                        boltAgent,
    //                        userAgent,
    //                        timeout,
    //                        SecurityPlans.unencrypted(),
    //                        AuthTokens.custom(Collections.emptyMap()))
    //                .toCompletableFuture()
    //                .join();
    //
    //        // then
    //        then(upstreamProvider)
    //                .should()
    //                .connect(
    //                        eq(address),
    //                        eq(context),
    //                        eq(boltAgent),
    //                        eq(userAgent),
    //                        eq(timeout),
    //                        eq(securityPlan),
    //                        eq(null),
    //                        any(),
    //                        eq(AccessMode.WRITE),
    //                        eq(Collections.emptySet()),
    //                        eq(null),
    //                        eq(null),
    //                        eq(null),
    //                        any(),
    //                        any());
    //        then(connection).should().writeAndFlush(any(), eq(Messages.reset()));
    //    }
    //
    //    @ParameterizedTest
    //    @MethodSource("supportsMultiDbParams")
    //    void shouldSupportMultiDb(BoltProtocolVersion boltProtocolVersion, boolean expectedToSupport) {
    //        // given
    //        given(connection.protocolVersion()).willReturn(boltProtocolVersion);
    //        given(connection.writeAndFlush(any(), eq(Messages.reset())))
    //                .willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
    //                    var handler = (ResponseHandler) invocationOnMock.getArgument(0);
    //                    handler.onResetSummary(mock(ResetSummary.class));
    //                    handler.onComplete();
    //                    return CompletableFuture.completedStage(null);
    //                });
    //        given(upstreamProvider.connect(
    //                        eq(address),
    //                        eq(context),
    //                        eq(boltAgent),
    //                        eq(userAgent),
    //                        eq(timeout),
    //                        eq(securityPlan),
    //                        eq(null),
    //                        any(),
    //                        eq(AccessMode.WRITE),
    //                        eq(Collections.emptySet()),
    //                        eq(null),
    //                        eq(null),
    //                        eq(null),
    //                        any(),
    //                        any()))
    //                .willReturn(CompletableFuture.completedStage(connection));
    //
    //        // when
    //        var supports = provider.supportsMultiDb(
    //                        address,
    //                        context,
    //                        boltAgent,
    //                        userAgent,
    //                        timeout,
    //                        SecurityPlans.unencrypted(),
    //                        AuthTokens.custom(Collections.emptyMap()))
    //                .toCompletableFuture()
    //                .join();
    //
    //        // then
    //        assertEquals(expectedToSupport, supports);
    //        then(upstreamProvider)
    //                .should()
    //                .connect(
    //                        eq(address),
    //                        eq(context),
    //                        eq(boltAgent),
    //                        eq(userAgent),
    //                        eq(timeout),
    //                        eq(securityPlan),
    //                        eq(null),
    //                        any(),
    //                        eq(AccessMode.WRITE),
    //                        eq(Collections.emptySet()),
    //                        eq(null),
    //                        eq(null),
    //                        eq(null),
    //                        any(),
    //                        any());
    //        then(connection).should().writeAndFlush(any(), eq(Messages.reset()));
    //    }
    //
    //    private static Stream<Arguments> supportsMultiDbParams() {
    //        return Stream.of(
    //                Arguments.arguments(new BoltProtocolVersion(4, 0), true),
    //                Arguments.arguments(new BoltProtocolVersion(3, 5), false));
    //    }
    //
    //    @ParameterizedTest
    //    @MethodSource("supportsSessionAuthParams")
    //    void shouldSupportsSessionAuth(BoltProtocolVersion boltProtocolVersion, boolean expectedToSupport) {
    //        // given
    //        given(connection.protocolVersion()).willReturn(boltProtocolVersion);
    //        given(connection.writeAndFlush(any(), eq(Messages.reset())))
    //                .willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
    //                    var handler = (ResponseHandler) invocationOnMock.getArgument(0);
    //                    handler.onResetSummary(mock(ResetSummary.class));
    //                    handler.onComplete();
    //                    return CompletableFuture.completedStage(null);
    //                });
    //        given(upstreamProvider.connect(
    //                        eq(address),
    //                        eq(context),
    //                        eq(boltAgent),
    //                        eq(userAgent),
    //                        eq(timeout),
    //                        eq(securityPlan),
    //                        eq(null),
    //                        any(),
    //                        eq(AccessMode.WRITE),
    //                        eq(Collections.emptySet()),
    //                        eq(null),
    //                        eq(null),
    //                        eq(null),
    //                        any(),
    //                        any()))
    //                .willReturn(CompletableFuture.completedStage(connection));
    //
    //        // when
    //        var supports = provider.supportsSessionAuth(
    //                        address,
    //                        context,
    //                        boltAgent,
    //                        userAgent,
    //                        timeout,
    //                        SecurityPlans.unencrypted(),
    //                        AuthTokens.custom(Collections.emptyMap()))
    //                .toCompletableFuture()
    //                .join();
    //
    //        // then
    //        assertEquals(expectedToSupport, supports);
    //        then(upstreamProvider)
    //                .should()
    //                .connect(
    //                        eq(address),
    //                        eq(context),
    //                        eq(boltAgent),
    //                        eq(userAgent),
    //                        eq(timeout),
    //                        eq(securityPlan),
    //                        eq(null),
    //                        any(),
    //                        eq(AccessMode.WRITE),
    //                        eq(Collections.emptySet()),
    //                        eq(null),
    //                        eq(null),
    //                        eq(null),
    //                        any(),
    //                        any());
    //        then(connection).should().writeAndFlush(any(), eq(Messages.reset()));
    //    }
    //
    //    private static Stream<Arguments> supportsSessionAuthParams() {
    //        return Stream.of(
    //                Arguments.arguments(new BoltProtocolVersion(5, 1), true),
    //                Arguments.arguments(new BoltProtocolVersion(5, 0), false));
    //    }
    //
    //    @Test
    //    void shouldThrowOnLowerVersion() {
    //        // given
    //        given(connection.protocolVersion()).willReturn(new BoltProtocolVersion(5, 0));
    //        given(connection.state()).willReturn(BoltConnectionState.OPEN);
    //        given(connection.writeAndFlush(any(), eq(Messages.reset())))
    //                .willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
    //                    var handler = (ResponseHandler) invocationOnMock.getArgument(0);
    //                    handler.onResetSummary(mock(ResetSummary.class));
    //                    handler.onComplete();
    //                    return CompletableFuture.completedStage(null);
    //                });
    //        given(upstreamProvider.connect(
    //                        eq(address),
    //                        eq(context),
    //                        eq(boltAgent),
    //                        eq(userAgent),
    //                        eq(timeout),
    //                        eq(securityPlan),
    //                        eq(databaseName),
    //                        any(),
    //                        eq(mode),
    //                        eq(bookmarks),
    //                        eq(null),
    //                        eq(minVersion),
    //                        eq(notificationConfig),
    //                        any(),
    //                        any()))
    //                .willReturn(CompletableFuture.completedStage(connection));
    //        provider.connect(
    //                        address,
    //                        context,
    //                        boltAgent,
    //                        userAgent,
    //                        timeout,
    //                        securityPlan,
    //                        databaseName,
    //                        authTokenStageSupplier,
    //                        mode,
    //                        bookmarks,
    //                        null,
    //                        minVersion,
    //                        notificationConfig,
    //                        databaseNameConsumer,
    //                        Collections.emptyMap())
    //                .toCompletableFuture()
    //                .join()
    //                .close()
    //                .toCompletableFuture()
    //                .join();
    //
    //        // when
    //        var future = provider.connect(
    //                        address,
    //                        context,
    //                        boltAgent,
    //                        userAgent,
    //                        timeout,
    //                        securityPlan,
    //                        databaseName,
    //                        authTokenStageSupplier,
    //                        mode,
    //                        bookmarks,
    //                        null,
    //                        new BoltProtocolVersion(5, 5),
    //                        NotificationConfig.defaultConfig(),
    //                        databaseNameConsumer,
    //                        Collections.emptyMap())
    //                .toCompletableFuture();
    //
    //        // then
    //        var exception = assertThrows(CompletionException.class, future::join);
    //        assertInstanceOf(MinVersionAcquisitionException.class, exception.getCause());
    //    }
    //
    //    @Test
    //    void shouldTestMaxLifetime() {
    //        // given
    //        given(connection.protocolVersion()).willReturn(minVersion);
    //        given(connection.state()).willReturn(BoltConnectionState.OPEN);
    //        given(connection.writeAndFlush(any(), eq(Messages.reset())))
    //                .willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
    //                    var handler = (ResponseHandler) invocationOnMock.getArgument(0);
    //                    handler.onResetSummary(mock(ResetSummary.class));
    //                    handler.onComplete();
    //                    return CompletableFuture.completedStage(null);
    //                });
    //        var connection2 = mock(BoltConnection.class);
    //        given(upstreamProvider.connect(
    //                        eq(address),
    //                        eq(context),
    //                        eq(boltAgent),
    //                        eq(userAgent),
    //                        eq(timeout),
    //                        eq(securityPlan),
    //                        eq(databaseName),
    //                        any(),
    //                        eq(mode),
    //                        eq(bookmarks),
    //                        eq(null),
    //                        eq(minVersion),
    //                        eq(notificationConfig),
    //                        any(),
    //                        any()))
    //                .willReturn(CompletableFuture.completedStage(connection))
    //                .willReturn(CompletableFuture.completedStage(connection2));
    //        provider.connect(
    //                        address,
    //                        context,
    //                        boltAgent,
    //                        userAgent,
    //                        timeout,
    //                        securityPlan,
    //                        databaseName,
    //                        authTokenStageSupplier,
    //                        mode,
    //                        bookmarks,
    //                        null,
    //                        minVersion,
    //                        notificationConfig,
    //                        databaseNameConsumer,
    //                        Collections.emptyMap())
    //                .toCompletableFuture()
    //                .join()
    //                .close()
    //                .toCompletableFuture()
    //                .join();
    //        given(clock.millis()).willReturn(maxLifetime + 1);
    //
    //        // when
    //        var anotherConnection = provider.connect(
    //                        address,
    //                        context,
    //                        boltAgent,
    //                        userAgent,
    //                        timeout,
    //                        securityPlan,
    //                        databaseName,
    //                        authTokenStageSupplier,
    //                        mode,
    //                        bookmarks,
    //                        null,
    //                        minVersion,
    //                        NotificationConfig.defaultConfig(),
    //                        databaseNameConsumer,
    //                        Collections.emptyMap())
    //                .toCompletableFuture()
    //                .join();
    //
    //        // then
    //        assertEquals(1, provider.inUse());
    //        assertEquals(1, provider.size());
    //        assertEquals(connection2, ((PooledBoltConnection) anotherConnection).delegate());
    //        then(connection).should().close();
    //    }
    //
    //    @Test
    //    void shouldTestLiveness() {
    //        // given
    //        given(connection.protocolVersion()).willReturn(minVersion);
    //        given(connection.state()).willReturn(BoltConnectionState.OPEN);
    //        given(connection.writeAndFlush(any(), eq(Messages.reset())))
    //                .willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
    //                    var handler = (ResponseHandler) invocationOnMock.getArgument(0);
    //                    handler.onResetSummary(mock(ResetSummary.class));
    //                    handler.onComplete();
    //                    return CompletableFuture.completedStage(null);
    //                });
    //        var authInfo = mock(AuthInfo.class);
    //        given(authInfo.authAckMillis()).willReturn(0L);
    //        given(authInfo.authToken()).willReturn(AuthTokens.custom(Collections.emptyMap()));
    //        given(connection.authInfo()).willReturn(CompletableFuture.completedStage(authInfo));
    //        given(upstreamProvider.connect(
    //                        eq(address),
    //                        eq(context),
    //                        eq(boltAgent),
    //                        eq(userAgent),
    //                        eq(timeout),
    //                        eq(securityPlan),
    //                        eq(databaseName),
    //                        any(),
    //                        eq(mode),
    //                        eq(bookmarks),
    //                        eq(null),
    //                        eq(minVersion),
    //                        eq(notificationConfig),
    //                        any(),
    //                        any()))
    //                .willReturn(CompletableFuture.completedStage(connection));
    //        provider.connect(
    //                        address,
    //                        context,
    //                        boltAgent,
    //                        userAgent,
    //                        timeout,
    //                        securityPlan,
    //                        databaseName,
    //                        authTokenStageSupplier,
    //                        mode,
    //                        bookmarks,
    //                        null,
    //                        minVersion,
    //                        notificationConfig,
    //                        databaseNameConsumer,
    //                        Collections.emptyMap())
    //                .toCompletableFuture()
    //                .join()
    //                .close()
    //                .toCompletableFuture()
    //                .join();
    //        given(clock.millis()).willReturn(idleBeforeTest + 1);
    //
    //        // when
    //        var actualConnection = provider.connect(
    //                        address,
    //                        context,
    //                        boltAgent,
    //                        userAgent,
    //                        timeout,
    //                        securityPlan,
    //                        databaseName,
    //                        authTokenStageSupplier,
    //                        mode,
    //                        bookmarks,
    //                        null,
    //                        minVersion,
    //                        NotificationConfig.defaultConfig(),
    //                        databaseNameConsumer,
    //                        Collections.emptyMap())
    //                .toCompletableFuture()
    //                .join();
    //
    //        // then
    //        assertEquals(connection, ((PooledBoltConnection) actualConnection).delegate());
    //        then(connection).should(times(2)).writeAndFlush(any(), eq(Messages.reset()));
    //    }
    //
    //    @Test
    //    void shouldPipelineReauth() {
    //        // given
    //        given(connection.protocolVersion()).willReturn(minVersion);
    //        given(connection.state()).willReturn(BoltConnectionState.OPEN);
    //        given(connection.writeAndFlush(any(), eq(Messages.reset())))
    //                .willAnswer((Answer<CompletionStage<Void>>) invocationOnMock -> {
    //                    var handler = (ResponseHandler) invocationOnMock.getArgument(0);
    //                    handler.onResetSummary(mock(ResetSummary.class));
    //                    handler.onComplete();
    //                    return CompletableFuture.completedStage(null);
    //                });
    //        var authMap = Map.of("key", mock(Value.class));
    //        var authToken = AuthTokens.custom(authMap);
    //        given(connection.write(List.of(Messages.logoff(), Messages.logon(authToken))))
    //                .willReturn(CompletableFuture.completedStage(null));
    //        var authInfo = mock(AuthInfo.class);
    //        given(authInfo.authAckMillis()).willReturn(0L);
    //        given(authInfo.authToken()).willReturn(AuthTokens.custom(Collections.emptyMap()));
    //        given(connection.authInfo()).willReturn(CompletableFuture.completedStage(authInfo));
    //        given(authTokenStageSupplier.get())
    //                .willReturn(CompletableFuture.completedStage(AuthTokens.custom(Collections.emptyMap())))
    //                .willReturn(CompletableFuture.completedStage(authToken));
    //        given(upstreamProvider.connect(
    //                        eq(address),
    //                        eq(context),
    //                        eq(boltAgent),
    //                        eq(userAgent),
    //                        eq(timeout),
    //                        eq(securityPlan),
    //                        eq(databaseName),
    //                        any(),
    //                        eq(mode),
    //                        eq(bookmarks),
    //                        eq(null),
    //                        eq(minVersion),
    //                        eq(notificationConfig),
    //                        any(),
    //                        any()))
    //                .willReturn(CompletableFuture.completedStage(connection));
    //        provider.connect(
    //                        address,
    //                        context,
    //                        boltAgent,
    //                        userAgent,
    //                        timeout,
    //                        securityPlan,
    //                        databaseName,
    //                        authTokenStageSupplier,
    //                        mode,
    //                        bookmarks,
    //                        null,
    //                        minVersion,
    //                        notificationConfig,
    //                        databaseNameConsumer,
    //                        Collections.emptyMap())
    //                .toCompletableFuture()
    //                .join()
    //                .close()
    //                .toCompletableFuture()
    //                .join();
    //
    //        // when
    //        var actualConnection = provider.connect(
    //                        address,
    //                        context,
    //                        boltAgent,
    //                        userAgent,
    //                        timeout,
    //                        securityPlan,
    //                        databaseName,
    //                        authTokenStageSupplier,
    //                        mode,
    //                        bookmarks,
    //                        null,
    //                        minVersion,
    //                        NotificationConfig.defaultConfig(),
    //                        databaseNameConsumer,
    //                        Collections.emptyMap())
    //                .toCompletableFuture()
    //                .join();
    //
    //        // then
    //        var captor = ArgumentCaptor.forClass(Message[].class);
    //        assertEquals(connection, ((PooledBoltConnection) actualConnection).delegate());
    //        then(connection).should().write(List.of(Messages.logoff(), Messages.logon(authToken)));
    //    }
}
