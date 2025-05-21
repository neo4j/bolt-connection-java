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
package org.neo4j.bolt.connection.netty;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalAddress;
import java.time.Clock;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.neo4j.bolt.connection.BoltConnectionProvider;
import org.neo4j.bolt.connection.BoltConnectionProviderFactory;
import org.neo4j.bolt.connection.DefaultDomainNameResolver;
import org.neo4j.bolt.connection.DomainNameResolver;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.MetricsListener;
import org.neo4j.bolt.connection.values.ValueFactory;

public final class NettyBoltConnectionProviderFactory implements BoltConnectionProviderFactory {
    private static final Set<String> SUPPORTED_SCHEMES = Set.of("bolt", "bolt+s", "bolt+ssc");

    // used by java.util.ServiceLoader
    public NettyBoltConnectionProviderFactory() {}

    @Override
    public boolean supports(String scheme) {
        return SUPPORTED_SCHEMES.contains(scheme);
    }

    @Override
    public BoltConnectionProvider create(
            LoggingProvider loggingProvider,
            ValueFactory valueFactory,
            MetricsListener metricsListener,
            Map<String, ?> additionalConfig) {
        var logger = loggingProvider.getLog(getClass());

        // get additional parameters
        var shutdownEventLoopGroupOnClose = false;
        var eventLoopGroup =
                getConfigEntry(logger, additionalConfig, "eventLoopGroup", EventLoopGroup.class, () -> null);
        if (eventLoopGroup == null) {
            eventLoopGroup = createEventLoopGroup(logger, additionalConfig);
            shutdownEventLoopGroupOnClose = true;
        }
        var clock = getConfigEntry(logger, additionalConfig, "clock", Clock.class, Clock::systemUTC);
        var domainNameResolver = getConfigEntry(
                logger,
                additionalConfig,
                "domainNameResolver",
                DomainNameResolver.class,
                DefaultDomainNameResolver::getInstance);
        var localAddress = getConfigEntry(logger, additionalConfig, "localAddress", LocalAddress.class, () -> null);

        return new NettyBoltConnectionProvider(
                eventLoopGroup,
                clock,
                domainNameResolver,
                localAddress,
                loggingProvider,
                valueFactory,
                metricsListener,
                shutdownEventLoopGroupOnClose);
    }

    private EventLoopGroup createEventLoopGroup(System.Logger logger, Map<String, ?> additionalConfig) {
        var size = getConfigEntry(logger, additionalConfig, "eventLoopThreads", Integer.class, () -> 0);
        return BootstrapFactory.newBootstrap(size).config().group();
    }

    private static <T> T getConfigEntry(
            System.Logger logger, Map<String, ?> config, String key, Class<T> type, Supplier<T> defaultValue) {
        var value = config.get(key);
        if (value == null) {
            logger.log(System.Logger.Level.TRACE, "No %s provided, will use default", key);
            return defaultValue.get();
        } else {
            if (type.isAssignableFrom(value.getClass())) {
                logger.log(System.Logger.Level.TRACE, "Found %s provided", key);
                return type.cast(value);
            } else {
                logger.log(System.Logger.Level.ERROR, "Found %s provided, but it is not of type %s", key, type);
                throw new IllegalArgumentException("Expected " + type + " but got " + value.getClass());
            }
        }
    }
}
