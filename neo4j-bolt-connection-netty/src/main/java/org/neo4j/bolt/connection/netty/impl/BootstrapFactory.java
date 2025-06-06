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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import org.neo4j.bolt.connection.netty.impl.async.connection.EventLoopGroupFactory;

public final class BootstrapFactory {
    private BootstrapFactory() {}

    public static Bootstrap newBootstrap(int threadCount, String threadNamePrefix) {
        var eventLoopGroupFactory = new EventLoopGroupFactory(threadNamePrefix);
        var bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroupFactory.newEventLoopGroup(threadCount));
        bootstrap.channel(eventLoopGroupFactory.channelClass());
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.option(ChannelOption.SO_REUSEADDR, true);
        return bootstrap;
    }
}
