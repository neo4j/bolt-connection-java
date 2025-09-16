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

import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.local.LocalEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.uring.IoUringIoHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import org.neo4j.bolt.connection.netty.impl.EventLoopThread;

/**
 * Manages creation of Netty {@link EventLoopGroup}s, which are basically {@link Executor}s that perform IO operations.
 */
public final class EventLoopGroupFactory {
    private static final String THREAD_NAME_PREFIX = "Neo4jDriverIO";
    private static final int THREAD_PRIORITY = Thread.MAX_PRIORITY;
    private static final boolean THREAD_IS_DAEMON = true;

    private final String threadNamePrefix;
    private final NettyTransport nettyTransport;

    public EventLoopGroupFactory(String threadNamePrefix, NettyTransport nettyTransport) {
        this.threadNamePrefix = Objects.requireNonNullElse(threadNamePrefix, THREAD_NAME_PREFIX);
        this.nettyTransport = Objects.requireNonNull(nettyTransport);
    }

    public NettyTransport nettyTransport() {
        return nettyTransport;
    }

    @SuppressWarnings("deprecation")
    public EventLoopGroup newEventLoopGroup(int threadCount) {
        return switch (nettyTransport.type()) {
            case NIO -> new DriverEventLoopGroup(threadCount);
            case EPOLL -> new EpollEventLoopGroup(threadCount, new DriverThreadFactory(threadNamePrefix));
            case IO_URING -> new MultiThreadIoEventLoopGroup(
                    threadCount, new DriverThreadFactory(threadNamePrefix), IoUringIoHandler.newFactory());
            case KQUEUE -> new KQueueEventLoopGroup(threadCount, new DriverThreadFactory(threadNamePrefix));
            case LOCAL -> new LocalEventLoopGroup(threadCount, new DriverThreadFactory(threadNamePrefix));
        };
    }

    /**
     * Assert that current thread is not an event loop used for async IO operations. This check is needed because
     * blocking API methods are implemented on top of corresponding async API methods. Deadlocks might happen when
     * IO thread executes blocking API call and has to wait for itself to read from the network.
     *
     * @throws IllegalStateException when current thread is an event loop IO thread.
     */
    public static void assertNotInEventLoopThread() throws IllegalStateException {
        if (isEventLoopThread(Thread.currentThread())) {
            throw new IllegalStateException(
                    "Blocking operation can't be executed in IO thread because it might result in a deadlock. "
                            + "Please do not use blocking API when chaining futures returned by async API methods.");
        }
    }

    /**
     * Check if given thread is an event loop IO thread.
     *
     * @param thread the thread to check.
     * @return {@code true} when given thread belongs to the event loop, {@code false} otherwise.
     */
    public static boolean isEventLoopThread(Thread thread) {
        return thread instanceof EventLoopThread;
    }

    /**
     * Same as {@link NioEventLoopGroup} but uses a different {@link ThreadFactory} that produces threads of
     * {@link EventLoopThread} class. Such threads can be recognized by {@link #assertNotInEventLoopThread()}.
     */
    // use NioEventLoopGroup for now to be compatible with Netty 4.1
    @SuppressWarnings("deprecation")
    private class DriverEventLoopGroup extends NioEventLoopGroup {
        DriverEventLoopGroup(int nThreads) {
            super(nThreads);
        }

        @Override
        protected ThreadFactory newDefaultThreadFactory() {
            return new DriverThreadFactory(threadNamePrefix);
        }
    }

    /**
     * Same as {@link DefaultThreadFactory} created by default, except produces threads of
     * {@link DriverThread} class. Such threads can be recognized by {@link #assertNotInEventLoopThread()}.
     */
    private static class DriverThreadFactory extends DefaultThreadFactory {
        DriverThreadFactory(String threadNamePrefix) {
            super(threadNamePrefix, THREAD_IS_DAEMON, THREAD_PRIORITY);
        }

        @SuppressWarnings("InstantiatingAThreadWithDefaultRunMethod")
        @Override
        protected Thread newThread(Runnable r, String name) {
            return new DriverThread(threadGroup, r, name);
        }
    }

    /**
     * Same as default thread created by {@link DefaultThreadFactory} except this dedicated class can be easily
     * recognized by {@link #assertNotInEventLoopThread()}.
     */
    private static class DriverThread extends FastThreadLocalThread implements EventLoopThread {
        DriverThread(ThreadGroup group, Runnable target, String name) {
            super(group, target, name);
        }
    }
}
