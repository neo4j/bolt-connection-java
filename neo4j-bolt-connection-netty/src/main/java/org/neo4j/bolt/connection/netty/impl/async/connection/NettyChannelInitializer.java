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
package org.neo4j.bolt.connection.netty.impl.async.connection;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.SslHandler;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.net.ssl.SSLEngine;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.SecurityPlan;
import org.neo4j.bolt.connection.netty.impl.async.inbound.InboundMessageDispatcher;
import org.neo4j.bolt.connection.values.ValueFactory;

public class NettyChannelInitializer extends ChannelInitializer<Channel> {
    private final BoltServerAddress address;
    private final SecurityPlan securityPlan;
    private final long sslHandshakeTimeoutMillis;
    private final Clock clock;
    private final LoggingProvider logging;
    private final CompletableFuture<Duration> sslHandshakeFuture;
    private final CompletableFuture<Channel> handshakeCompleted;
    private final BoltProtocolVersion maxVersion;
    private final long preferredCapabilitiesMask;
    private final ValueFactory valueFactory;

    public NettyChannelInitializer(
            BoltServerAddress address,
            SecurityPlan securityPlan,
            long sslHandshakeTimeoutMillis,
            Clock clock,
            LoggingProvider logging,
            CompletableFuture<Duration> sslHandshakeFuture,
            CompletableFuture<Channel> handshakeCompleted,
            BoltProtocolVersion maxVersion,
            long preferredCapabilitiesMask,
            ValueFactory valueFactory) {
        this.address = address;
        this.securityPlan = securityPlan;
        this.sslHandshakeTimeoutMillis = sslHandshakeTimeoutMillis;
        this.clock = clock;
        this.logging = logging;
        this.sslHandshakeFuture = Objects.requireNonNull(sslHandshakeFuture);
        this.handshakeCompleted = Objects.requireNonNull(handshakeCompleted);
        this.maxVersion = maxVersion;
        this.preferredCapabilitiesMask = preferredCapabilitiesMask;
        this.valueFactory = Objects.requireNonNull(valueFactory);
    }

    @Override
    protected void initChannel(Channel channel) {
        if (securityPlan != null) {
            var sslHandler = createSslHandler();
            var sslHandshakeDurationHandler = new SslHandshakeDurationHandler(channel, sslHandshakeFuture);
            channel.pipeline().addFirst(sslHandler, sslHandshakeDurationHandler);
        } else {
            sslHandshakeFuture.complete(Duration.ZERO);
            var fastOpen = Boolean.TRUE.equals(channel.config().getOption(ChannelOption.TCP_FASTOPEN_CONNECT));
            if (fastOpen) {
                var handshakeHandler = new HandshakeHandler(
                        new ChannelPipelineBuilderImpl(),
                        handshakeCompleted,
                        address,
                        maxVersion,
                        true,
                        sslHandshakeTimeoutMillis,
                        preferredCapabilitiesMask,
                        logging,
                        valueFactory);
                channel.pipeline().addFirst(handshakeHandler);
            }
        }

        updateChannelAttributes(channel);
    }

    private SslHandler createSslHandler() {
        var sslEngine = createSslEngine();
        var sslHandler = new SslHandler(sslEngine);
        if (sslHandshakeTimeoutMillis >= 0) {
            sslHandler.setHandshakeTimeoutMillis(sslHandshakeTimeoutMillis);
        }
        return sslHandler;
    }

    private SSLEngine createSslEngine() {
        var sslContext = securityPlan.sslContext();
        var host = securityPlan.expectedHostname();
        if (host == null) {
            host = address.host();
        }
        var sslEngine = sslContext.createSSLEngine(host, address.port());
        sslEngine.setUseClientMode(true);
        if (securityPlan.verifyHostname()) {
            var sslParameters = sslEngine.getSSLParameters();
            sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
            sslEngine.setSSLParameters(sslParameters);
        }
        return sslEngine;
    }

    private void updateChannelAttributes(Channel channel) {
        ChannelAttributes.setServerAddress(channel, address);
        ChannelAttributes.setCreationTimestamp(channel, clock.millis());
        ChannelAttributes.setMessageDispatcher(channel, new InboundMessageDispatcher(channel, logging));
    }
}
