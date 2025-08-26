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

import static java.util.Objects.requireNonNull;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.resolver.AddressResolverGroup;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.bolt.connection.BoltAgent;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.DomainNameResolver;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.NotificationConfig;
import org.neo4j.bolt.connection.SecurityPlan;
import org.neo4j.bolt.connection.netty.impl.async.NetworkConnection;
import org.neo4j.bolt.connection.netty.impl.async.connection.ChannelConnectedListener;
import org.neo4j.bolt.connection.netty.impl.async.connection.ChannelPipelineBuilderImpl;
import org.neo4j.bolt.connection.netty.impl.async.connection.NettyChannelInitializer;
import org.neo4j.bolt.connection.netty.impl.async.connection.NettyDomainNameResolverGroup;
import org.neo4j.bolt.connection.netty.impl.messaging.BoltProtocol;
import org.neo4j.bolt.connection.netty.impl.spi.Connection;
import org.neo4j.bolt.connection.netty.impl.util.FutureUtil;
import org.neo4j.bolt.connection.observation.ImmutableObservation;
import org.neo4j.bolt.connection.observation.ObservationProvider;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.bolt.connection.values.ValueFactory;

public final class NettyConnectionProvider implements ConnectionProvider {
    private final EventLoopGroup eventLoopGroup;
    private final Clock clock;
    private final DomainNameResolver domainNameResolver;
    private final AddressResolverGroup<InetSocketAddress> addressResolverGroup;
    private final LocalAddress localAddress;
    private final BoltProtocolVersion maxVersion;

    private final LoggingProvider logging;
    private final ValueFactory valueFactory;
    private final ObservationProvider observationProvider;

    public NettyConnectionProvider(
            EventLoopGroup eventLoopGroup,
            Clock clock,
            DomainNameResolver domainNameResolver,
            LocalAddress localAddress,
            BoltProtocolVersion maxVersion,
            LoggingProvider logging,
            ValueFactory valueFactory,
            ObservationProvider observationProvider) {
        this.eventLoopGroup = eventLoopGroup;
        this.clock = requireNonNull(clock);
        this.domainNameResolver = requireNonNull(domainNameResolver);
        this.addressResolverGroup = new NettyDomainNameResolverGroup(this.domainNameResolver);
        this.localAddress = localAddress;
        this.maxVersion = maxVersion;
        this.logging = logging;
        this.valueFactory = requireNonNull(valueFactory);
        this.observationProvider = Objects.requireNonNull(observationProvider);
    }

    @Override
    public CompletionStage<Connection> acquireConnection(
            BoltServerAddress address,
            SecurityPlan securityPlan,
            RoutingContext routingContext,
            Map<String, Value> authMap,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            long initialisationTimeoutMillis,
            CompletableFuture<Long> latestAuthMillisFuture,
            NotificationConfig notificationConfig,
            ImmutableObservation parentObservation) {
        var sslHandshakeFuture = new CompletableFuture<Duration>();
        var bootstrap = new Bootstrap();
        bootstrap
                .group(this.eventLoopGroup)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Math.max(connectTimeoutMillis, 0))
                .channel(localAddress != null ? LocalChannel.class : NioSocketChannel.class)
                .resolver(addressResolverGroup)
                .handler(new NettyChannelInitializer(
                        address, securityPlan, initialisationTimeoutMillis, clock, logging, sslHandshakeFuture));

        SocketAddress socketAddress;
        if (localAddress == null) {
            try {
                socketAddress =
                        new InetSocketAddress(domainNameResolver.resolve(address.connectionHost())[0], address.port());
            } catch (Throwable t) {
                socketAddress = InetSocketAddress.createUnresolved(address.connectionHost(), address.port());
            }
        } else {
            socketAddress = localAddress;
        }
        return installChannelConnectedListener(
                        address, bootstrap.connect(socketAddress), initialisationTimeoutMillis, sslHandshakeFuture)
                .thenCompose(channel -> {
                    var boltProtocol = BoltProtocol.forChannel(channel);
                    var exchangeObservation = observationProvider.boltExchange(
                            parentObservation,
                            address.connectionHost(),
                            address.port(),
                            boltProtocol.version(),
                            (k, v) -> {});
                    return boltProtocol
                            .initializeChannel(
                                    channel,
                                    requireNonNull(userAgent),
                                    requireNonNull(boltAgent),
                                    authMap,
                                    routingContext,
                                    notificationConfig,
                                    clock,
                                    latestAuthMillisFuture,
                                    valueFactory,
                                    exchangeObservation)
                            .whenComplete((ignored, throwable) -> {
                                if (throwable != null) {
                                    throwable = FutureUtil.completionExceptionCause(throwable);
                                    exchangeObservation.error(throwable);
                                }
                                exchangeObservation.stop();
                            });
                })
                .thenApply(channel -> new NetworkConnection(channel, logging));
    }

    private CompletionStage<Channel> installChannelConnectedListener(
            BoltServerAddress address,
            ChannelFuture channelConnected,
            long initialisationTimeoutMillis,
            CompletableFuture<Duration> sslHandshakeFuture) {
        var pipeline = channelConnected.channel().pipeline();

        // add listener that sends Bolt handshake bytes when channel is connected
        var handshakeCompleted = new CompletableFuture<Channel>();
        channelConnected.addListener(new ChannelConnectedListener(
                address,
                new ChannelPipelineBuilderImpl(),
                handshakeCompleted,
                maxVersion,
                logging,
                valueFactory,
                initialisationTimeoutMillis,
                sslHandshakeFuture));
        return handshakeCompleted;
    }
}
