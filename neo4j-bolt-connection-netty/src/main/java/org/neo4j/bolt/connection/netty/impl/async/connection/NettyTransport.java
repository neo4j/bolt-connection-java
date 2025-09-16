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
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueDomainSocketChannel;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.socket.nio.NioDomainSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.channel.uring.IoUring;
import io.netty.channel.uring.IoUringDomainSocketChannel;
import io.netty.channel.uring.IoUringSocketChannel;
import java.net.SocketAddress;
import java.net.UnixDomainSocketAddress;
import org.neo4j.bolt.connection.netty.impl.Scheme;

public interface NettyTransport {
    static boolean isEpollAvailable() {
        return EpollNettyTransport.isEpollAvailable();
    }

    static NettyTransport epoll() {
        return new EpollNettyTransport();
    }

    static boolean isIoUringAvailable() {
        return IoUringNettyTransport.isIoUringAvailable();
    }

    static NettyTransport ioUring() {
        return new IoUringNettyTransport();
    }

    static boolean isKQueueAvailable() {
        return KQueueNettyTransport.isKQueueAvailable();
    }

    static NettyTransport kqueue() {
        return new KQueueNettyTransport();
    }

    static NettyTransport local() {
        return new LocalNettyTransport();
    }

    Type type();

    Class<? extends Channel> channelClass(String scheme);

    SocketAddress domainSocketAddress(String file);

    boolean fastOpenAvailable();

    static NettyTransport nio() {
        return new NioNettyTransport();
    }

    enum Type {
        NIO,
        EPOLL,
        IO_URING,
        KQUEUE,
        LOCAL
    }

    record NioNettyTransport() implements NettyTransport {
        @Override
        public Type type() {
            return Type.NIO;
        }

        @Override
        public Class<? extends Channel> channelClass(String scheme) {
            return Scheme.BOLT_UNIX_SCHEME.equals(scheme) ? NioDomainSocketChannel.class : NioSocketChannel.class;
        }

        @Override
        public SocketAddress domainSocketAddress(String file) {
            return UnixDomainSocketAddress.of(file);
        }

        @Override
        public boolean fastOpenAvailable() {
            return false;
        }
    }

    record EpollNettyTransport() implements NettyTransport {
        private static final String EPOLL_NAME = "io.netty.channel.epoll.Epoll";

        static boolean isEpollAvailable() {
            try {
                Class.forName(EpollNettyTransport.EPOLL_NAME);
            } catch (ClassNotFoundException e) {
                return false;
            }
            return Epoll.isAvailable();
        }

        @Override
        public Type type() {
            return Type.EPOLL;
        }

        @Override
        public Class<? extends Channel> channelClass(String scheme) {
            return Scheme.BOLT_UNIX_SCHEME.equals(scheme) ? EpollDomainSocketChannel.class : EpollSocketChannel.class;
        }

        @Override
        public SocketAddress domainSocketAddress(String file) {
            return new DomainSocketAddress(file);
        }

        @Override
        public boolean fastOpenAvailable() {
            return Epoll.isTcpFastOpenClientSideAvailable();
        }
    }

    record IoUringNettyTransport() implements NettyTransport {
        private static final String IO_URING_NAME = "io.netty.channel.uring.IoUring";

        static boolean isIoUringAvailable() {
            try {
                Class.forName(IoUringNettyTransport.IO_URING_NAME);
            } catch (ClassNotFoundException e) {
                return false;
            }
            return IoUring.isAvailable();
        }

        @Override
        public Type type() {
            return Type.IO_URING;
        }

        @Override
        public Class<? extends Channel> channelClass(String scheme) {
            return Scheme.BOLT_UNIX_SCHEME.equals(scheme)
                    ? IoUringDomainSocketChannel.class
                    : IoUringSocketChannel.class;
        }

        @Override
        public SocketAddress domainSocketAddress(String file) {
            return new DomainSocketAddress(file);
        }

        @Override
        public boolean fastOpenAvailable() {
            return IoUring.isTcpFastOpenClientSideAvailable();
        }
    }

    record KQueueNettyTransport() implements NettyTransport {
        private static final String KQUEUE_NAME = "io.netty.channel.kqueue.KQueue";

        static boolean isKQueueAvailable() {
            try {
                Class.forName(KQueueNettyTransport.KQUEUE_NAME);
            } catch (ClassNotFoundException e) {
                return false;
            }
            return KQueue.isAvailable();
        }

        @Override
        public Type type() {
            return Type.KQUEUE;
        }

        @Override
        public Class<? extends Channel> channelClass(String scheme) {
            return Scheme.BOLT_UNIX_SCHEME.equals(scheme) ? KQueueDomainSocketChannel.class : KQueueSocketChannel.class;
        }

        @Override
        public SocketAddress domainSocketAddress(String file) {
            return new DomainSocketAddress(file);
        }

        @Override
        public boolean fastOpenAvailable() {
            return KQueue.isTcpFastOpenClientSideAvailable();
        }
    }

    record LocalNettyTransport() implements NettyTransport {
        @Override
        public Type type() {
            return Type.LOCAL;
        }

        @Override
        public Class<? extends Channel> channelClass(String scheme) {
            return LocalChannel.class;
        }

        @Override
        public SocketAddress domainSocketAddress(String file) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean fastOpenAvailable() {
            return false;
        }
    }
}
