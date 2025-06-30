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

import static java.lang.String.format;
import static org.neo4j.bolt.connection.netty.impl.async.connection.BoltProtocolUtil.handshakeString;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.net.ssl.SSLHandshakeException;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.exception.BoltServiceUnavailableException;
import org.neo4j.bolt.connection.netty.impl.logging.ChannelActivityLogger;
import org.neo4j.bolt.connection.values.ValueFactory;

public class ChannelConnectedListener implements ChannelFutureListener {
    private final BoltServerAddress address;
    private final ChannelPipelineBuilder pipelineBuilder;
    private final CompletableFuture<Channel> handshakeCompletedFuture;
    private final BoltProtocolVersion maxVersion;
    private final LoggingProvider logging;
    private final ValueFactory valueFactory;

    public ChannelConnectedListener(
            BoltServerAddress address,
            ChannelPipelineBuilder pipelineBuilder,
            CompletableFuture<Channel> handshakeCompletedFuture,
            BoltProtocolVersion maxVersion,
            LoggingProvider logging,
            ValueFactory valueFactory) {
        this.address = address;
        this.pipelineBuilder = pipelineBuilder;
        this.handshakeCompletedFuture = handshakeCompletedFuture;
        this.maxVersion = maxVersion;
        this.logging = logging;
        this.valueFactory = Objects.requireNonNull(valueFactory);
    }

    @Override
    public void operationComplete(ChannelFuture future) {
        if (future.isSuccess()) {
            var channel = future.channel();
            var log = new ChannelActivityLogger(channel, logging, getClass());
            log.log(System.Logger.Level.TRACE, "Channel %s connected, initiating bolt handshake", channel);

            var pipeline = channel.pipeline();
            pipeline.addLast(
                    new HandshakeHandler(pipelineBuilder, handshakeCompletedFuture, maxVersion, logging, valueFactory));
            log.log(System.Logger.Level.DEBUG, "C: [Bolt Handshake] %s", handshakeString());
            channel.writeAndFlush(BoltProtocolUtil.handshakeBuf()).addListener(f -> {
                if (!f.isSuccess()) {
                    var error = f.cause();
                    if (!(error instanceof SSLHandshakeException)) {
                        error = new BoltServiceUnavailableException(
                                String.format("Unable to write Bolt handshake to %s.", this.address), error);
                    }
                    this.handshakeCompletedFuture.completeExceptionally(error);
                }
            });
        } else {
            handshakeCompletedFuture.completeExceptionally(databaseUnavailableError(address, future.cause()));
        }
    }

    private static Throwable databaseUnavailableError(BoltServerAddress address, Throwable cause) {
        return new BoltServiceUnavailableException(
                format(
                        "Unable to connect to %s, ensure the database is running and that there "
                                + "is a working network connection to it.",
                        address),
                cause);
    }
}
