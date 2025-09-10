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

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.util.internal.logging.InternalLoggerFactory;
import java.net.URI;
import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.neo4j.bolt.connection.AuthToken;
import org.neo4j.bolt.connection.BoltAgent;
import org.neo4j.bolt.connection.BoltConnection;
import org.neo4j.bolt.connection.BoltConnectionProvider;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.DomainNameResolver;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.NotificationConfig;
import org.neo4j.bolt.connection.SecurityPlan;
import org.neo4j.bolt.connection.exception.BoltClientException;
import org.neo4j.bolt.connection.exception.MinVersionAcquisitionException;
import org.neo4j.bolt.connection.netty.impl.util.FutureUtil;
import org.neo4j.bolt.connection.observation.ImmutableObservation;
import org.neo4j.bolt.connection.observation.ObservationProvider;
import org.neo4j.bolt.connection.values.ValueFactory;

public final class NettyBoltConnectionProvider implements BoltConnectionProvider {
    private final LoggingProvider logging;
    private final System.Logger log;
    private final EventLoopGroup eventLoopGroup;
    private final ConnectionProvider connectionProvider;
    private final Clock clock;
    private final ValueFactory valueFactory;
    private final boolean shutdownEventLoopGroupOnClose;
    private final ObservationProvider observationProvider;

    private CompletableFuture<Void> closeFuture;

    public NettyBoltConnectionProvider(
            EventLoopGroup eventLoopGroup,
            Class<? extends Channel> channelClass,
            Clock clock,
            DomainNameResolver domainNameResolver,
            LocalAddress localAddress,
            BoltProtocolVersion maxVersion,
            boolean fastOpen,
            long preferredCapabilitiesMask,
            LoggingProvider logging,
            ValueFactory valueFactory,
            boolean shutdownEventLoopGroupOnClose,
            ObservationProvider observationProvider) {
        Objects.requireNonNull(eventLoopGroup);
        this.clock = Objects.requireNonNull(clock);
        this.logging = Objects.requireNonNull(logging);
        this.log = logging.getLog(getClass());
        this.eventLoopGroup = Objects.requireNonNull(eventLoopGroup);
        this.connectionProvider = ConnectionProviders.netty(
                eventLoopGroup,
                channelClass,
                clock,
                domainNameResolver,
                localAddress,
                maxVersion,
                fastOpen,
                preferredCapabilitiesMask,
                logging,
                valueFactory,
                observationProvider);
        this.valueFactory = Objects.requireNonNull(valueFactory);
        InternalLoggerFactory.setDefaultFactory(new NettyLogging(logging));
        this.shutdownEventLoopGroupOnClose = shutdownEventLoopGroupOnClose;
        this.observationProvider = Objects.requireNonNull(observationProvider);
    }

    @Override
    public CompletionStage<BoltConnection> connect(
            URI uri,
            String routingContextAddress,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            long initialisationTimeoutMillis,
            SecurityPlan securityPlan,
            AuthToken authToken,
            BoltProtocolVersion minVersion,
            NotificationConfig notificationConfig,
            ImmutableObservation parentObservation) {
        synchronized (this) {
            if (closeFuture != null) {
                return CompletableFuture.failedFuture(new IllegalStateException("Connection provider is closed."));
            }
        }

        // extract address from the URI
        BoltServerAddress uriAddress;
        try {
            uriAddress = new BoltServerAddress(uri);
        } catch (Throwable throwable) {
            return CompletableFuture.failedStage(
                    new BoltClientException("Failed to parse server address: " + uri, throwable));
        }
        var address = securityPlan != null && securityPlan.expectedHostname() != null
                ? new BoltServerAddress(securityPlan.expectedHostname(), uriAddress.connectionHost(), uriAddress.port())
                : uriAddress;

        // determine routingContext
        RoutingContext routingContext;
        try {
            routingContext = new RoutingContext(uri, routingContextAddress);
        } catch (Exception e) {
            return CompletableFuture.failedStage(e);
        }
        if (!Scheme.isRoutingScheme(uri.getScheme()) && routingContext.isDefined()) {
            return CompletableFuture.failedStage(new IllegalArgumentException(
                    "%s scheme must not contain routing context".formatted(uri.getScheme())));
        }

        var latestAuthMillisFuture = new CompletableFuture<Long>();
        return connectionProvider
                .acquireConnection(
                        address,
                        securityPlan,
                        routingContext,
                        authToken.asMap(),
                        boltAgent,
                        userAgent,
                        connectTimeoutMillis,
                        initialisationTimeoutMillis,
                        latestAuthMillisFuture,
                        notificationConfig,
                        parentObservation)
                .thenCompose(connection -> {
                    if (minVersion != null
                            && minVersion.compareTo(connection.protocol().version()) > 0) {
                        return connection
                                .close()
                                .thenCompose(
                                        (ignored) -> CompletableFuture.failedStage(new MinVersionAcquisitionException(
                                                "lower version",
                                                connection.protocol().version())));
                    } else {
                        return CompletableFuture.completedStage(connection);
                    }
                })
                .handle((connection, throwable) -> {
                    if (throwable != null) {
                        throwable = FutureUtil.completionExceptionCause(throwable);
                        log.log(System.Logger.Level.DEBUG, "Failed to establish BoltConnection " + address, throwable);
                        throw new CompletionException(throwable);
                    } else {
                        return new BoltConnectionImpl(
                                connection.protocol(),
                                connection,
                                connection.eventLoop(),
                                authToken,
                                latestAuthMillisFuture,
                                routingContext,
                                clock,
                                logging,
                                valueFactory,
                                observationProvider);
                    }
                });
    }

    @Override
    public CompletionStage<Void> close() {
        CompletableFuture<Void> closeFuture;
        synchronized (this) {
            if (this.closeFuture == null) {
                this.closeFuture = new CompletableFuture<>();
                if (shutdownEventLoopGroupOnClose) {
                    eventLoopGroup
                            .shutdownGracefully(200, 15_000, TimeUnit.MILLISECONDS)
                            .addListener(future -> this.closeFuture.complete(null));
                } else {
                    this.closeFuture.complete(null);
                }
            }
            closeFuture = this.closeFuture;
        }
        return closeFuture;
    }
}
