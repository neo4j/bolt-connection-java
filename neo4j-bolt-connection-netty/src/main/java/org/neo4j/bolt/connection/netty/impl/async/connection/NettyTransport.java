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
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.util.Objects;

public record NettyTransport(Type type, Class<? extends Channel> channelClass, boolean fastOpenAvailable) {
    private static final String EPOLL_NAME = "io.netty.channel.epoll.Epoll";
    private static final String KQUEUE_NAME = "io.netty.channel.kqueue.KQueue";

    public static boolean isEpollAvailable() {
        try {
            Class.forName(EPOLL_NAME);
        } catch (ClassNotFoundException e) {
            return false;
        }
        return Epoll.isAvailable();
    }

    public static boolean isKQueueAvailable() {
        try {
            Class.forName(KQUEUE_NAME);
        } catch (ClassNotFoundException e) {
            return false;
        }
        return KQueue.isAvailable();
    }

    public static NettyTransport nio() {
        return new NettyTransport(Type.NIO, NioSocketChannel.class, false);
    }

    public static NettyTransport epoll() {
        return new NettyTransport(Type.EPOLL, EpollSocketChannel.class, Epoll.isTcpFastOpenClientSideAvailable());
    }

    public static NettyTransport kqueue() {
        return new NettyTransport(Type.KQUEUE, KQueueSocketChannel.class, KQueue.isTcpFastOpenClientSideAvailable());
    }

    public static NettyTransport local() {
        return new NettyTransport(Type.LOCAL, LocalChannel.class, false);
    }

    public NettyTransport {
        Objects.requireNonNull(channelClass);
    }

    public enum Type {
        NIO,
        EPOLL,
        KQUEUE,
        LOCAL
    }
}
