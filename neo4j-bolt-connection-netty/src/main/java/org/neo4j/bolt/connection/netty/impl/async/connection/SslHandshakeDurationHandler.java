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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

final class SslHandshakeDurationHandler extends ChannelInboundHandlerAdapter {
    private final CompletableFuture<Duration> sslHandshakeFuture;
    private Instant startInstant;

    SslHandshakeDurationHandler(Channel channel, CompletableFuture<Duration> sslHandshakeFuture) {
        this.sslHandshakeFuture = sslHandshakeFuture;
        var fastOpen = Boolean.TRUE.equals(channel.config().getOption(ChannelOption.TCP_FASTOPEN_CONNECT));
        if (fastOpen) {
            startInstant = Instant.now();
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        if (startInstant == null) {
            startInstant = Instant.now();
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof SslHandshakeCompletionEvent event) {
            if (event.isSuccess()) {
                var duration = Duration.between(startInstant, Instant.now());
                sslHandshakeFuture.complete(duration);
            } else {
                sslHandshakeFuture.completeExceptionally(event.cause());
            }
            ctx.pipeline().remove(this);
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (!sslHandshakeFuture.isDone()) {
            sslHandshakeFuture.completeExceptionally(
                    new IOException("Inactive channel before SSL handshake is finished"));
        }
        super.channelInactive(ctx);
    }
}
