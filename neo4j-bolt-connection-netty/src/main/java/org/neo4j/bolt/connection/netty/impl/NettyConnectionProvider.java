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
import io.netty.resolver.AddressResolverGroup;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.file.Path;
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
import org.neo4j.bolt.connection.exception.BoltClientException;
import org.neo4j.bolt.connection.netty.impl.async.NetworkConnection;
import org.neo4j.bolt.connection.netty.impl.async.connection.ChannelConnectedListener;
import org.neo4j.bolt.connection.netty.impl.async.connection.ChannelPipelineBuilderImpl;
import org.neo4j.bolt.connection.netty.impl.async.connection.NettyChannelInitializer;
import org.neo4j.bolt.connection.netty.impl.async.connection.NettyDomainNameResolverGroup;
import org.neo4j.bolt.connection.netty.impl.async.connection.NettyTransport;
import org.neo4j.bolt.connection.netty.impl.messaging.BoltProtocol;
import org.neo4j.bolt.connection.netty.impl.spi.Connection;
import org.neo4j.bolt.connection.netty.impl.util.FutureUtil;
import org.neo4j.bolt.connection.observation.ImmutableObservation;
import org.neo4j.bolt.connection.observation.ObservationProvider;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.bolt.connection.values.ValueFactory;

public final class NettyConnectionProvider implements ConnectionProvider {
    private final EventLoopGroup eventLoopGroup;
    private final NettyTransport nettyTransport;
    private final Clock clock;
    private final DomainNameResolver domainNameResolver;
    private final AddressResolverGroup<InetSocketAddress> addressResolverGroup;
    private final LocalAddress localAddress;
    private final BoltProtocolVersion maxVersion;
    private final boolean fastOpen;
    private final long preferredCapabilitiesMask;

    private final LoggingProvider logging;
    private final ValueFactory valueFactory;
    private final ObservationProvider observationProvider;

    public NettyConnectionProvider(
            EventLoopGroup eventLoopGroup,
            NettyTransport nettyTransport,
            Clock clock,
            DomainNameResolver domainNameResolver,
            LocalAddress localAddress,
            BoltProtocolVersion maxVersion,
            boolean fastOpen,
            long preferredCapabilitiesMask,
            LoggingProvider logging,
            ValueFactory valueFactory,
            ObservationProvider observationProvider) {
        this.eventLoopGroup = eventLoopGroup;
        this.nettyTransport = Objects.requireNonNull(nettyTransport);
        this.clock = requireNonNull(clock);
        this.domainNameResolver = requireNonNull(domainNameResolver);
        this.addressResolverGroup = new NettyDomainNameResolverGroup(this.domainNameResolver);
        this.localAddress = localAddress;
        this.maxVersion = maxVersion;
        this.fastOpen = fastOpen;
        this.preferredCapabilitiesMask = preferredCapabilitiesMask;
        this.logging = logging;
        this.valueFactory = requireNonNull(valueFactory);
        this.observationProvider = Objects.requireNonNull(observationProvider);
    }

    @Override
    public CompletionStage<Connection> acquireConnection(
            URI uri,
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
        // extract address from the URI
        BoltServerAddress uriAddress;
        var boltUnixScheme = Scheme.BOLT_UNIX_SCHEME.equals(uri.getScheme());
        try {
            if (boltUnixScheme) {
                if (securityPlan != null) {
                    throw new IllegalArgumentException(
                            "Security plan is not supported with %s scheme".formatted(Scheme.BOLT_UNIX_SCHEME));
                }
                uriAddress = new BoltServerAddress(Path.of(uri.getPath()));
            } else {
                uriAddress = new BoltServerAddress(uri);
            }
        } catch (Throwable throwable) {
            return CompletableFuture.failedStage(
                    new BoltClientException("Failed to parse server address: " + uri, throwable));
        }
        var address = securityPlan != null && securityPlan.expectedHostname() != null
                ? new BoltServerAddress(securityPlan.expectedHostname(), uriAddress.connectionHost(), uriAddress.port())
                : uriAddress;
        var scheme = uri.getScheme();
        var sslHandshakeFuture = new CompletableFuture<Duration>();
        var handshakeCompleted = new CompletableFuture<Channel>();
        var bootstrap = new Bootstrap();
        bootstrap
                .group(this.eventLoopGroup)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Math.max(connectTimeoutMillis, 0))
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.SO_REUSEADDR, true)
                .channel(localAddress != null ? LocalChannel.class : nettyTransport.channelClass(scheme))
                .resolver(addressResolverGroup)
                .handler(new NettyChannelInitializer(
                        address,
                        securityPlan,
                        initialisationTimeoutMillis,
                        clock,
                        logging,
                        sslHandshakeFuture,
                        handshakeCompleted,
                        maxVersion,
                        preferredCapabilitiesMask,
                        valueFactory));
        if (fastOpen) {
            bootstrap.option(ChannelOption.TCP_FASTOPEN_CONNECT, true);
        }

        SocketAddress socketAddress;
        if (localAddress == null) {
            if (boltUnixScheme) {
                socketAddress = nettyTransport.domainSocketAddress(uri.getPath());
            } else {
                try {
                    socketAddress = new InetSocketAddress(
                            domainNameResolver.resolve(address.connectionHost())[0], address.port());
                } catch (Throwable t) {
                    socketAddress = InetSocketAddress.createUnresolved(address.connectionHost(), address.port());
                }
            }
        } else {
            socketAddress = localAddress;
        }
        var appendBoltHanshake = !fastOpen || securityPlan != null;
        installChannelConnectedListener(
                address,
                bootstrap.connect(socketAddress),
                initialisationTimeoutMillis,
                sslHandshakeFuture,
                handshakeCompleted,
                appendBoltHanshake);
        return handshakeCompleted
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

    private void installChannelConnectedListener(
            BoltServerAddress address,
            ChannelFuture channelConnected,
            long initialisationTimeoutMillis,
            CompletableFuture<Duration> sslHandshakeFuture,
            CompletableFuture<Channel> handshakeCompleted,
            boolean appendBoltHanshake) {
        var pipeline = channelConnected.channel().pipeline();

        // add listener that sends Bolt handshake bytes when channel is connected
        channelConnected.addListener(new ChannelConnectedListener(
                address,
                new ChannelPipelineBuilderImpl(),
                handshakeCompleted,
                maxVersion,
                preferredCapabilitiesMask,
                logging,
                valueFactory,
                initialisationTimeoutMillis,
                sslHandshakeFuture,
                appendBoltHanshake));
    }
}
