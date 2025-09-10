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

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalAddress;
import java.net.URI;
import java.time.Clock;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Supplier;
import org.neo4j.bolt.connection.BoltCapability;
import org.neo4j.bolt.connection.BoltConnection;
import org.neo4j.bolt.connection.BoltConnectionProvider;
import org.neo4j.bolt.connection.BoltConnectionProviderFactory;
import org.neo4j.bolt.connection.BoltConnectionSource;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.DefaultDomainNameResolver;
import org.neo4j.bolt.connection.DomainNameResolver;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.RoutedBoltConnectionParameters;
import org.neo4j.bolt.connection.netty.impl.NettyBoltConnectionProvider;
import org.neo4j.bolt.connection.netty.impl.async.connection.EventLoopGroupFactory;
import org.neo4j.bolt.connection.netty.impl.async.connection.NettyTransport;
import org.neo4j.bolt.connection.observation.ObservationProvider;
import org.neo4j.bolt.connection.values.ValueFactory;

/**
 * A factory for creating instances of {@link BoltConnectionProvider}.
 * <p>
 * The created instances support the following URI schemes:
 * <ul>
 *     <li><b>bolt</b>, <b>bolt+s</b>, <b>bolt+ssc</b> - establishes a {@link BoltConnection} to a given address without
 *     routing context. In a clustered Neo4j DBMS the absense of routing context instructs the server to <b>NOT</b>
 *     carry out server-side routing even if it might be needed. The {@link URI#getQuery()} part is prohibited as
 *     routing context is not supported.</li>
 *     <li><b>neo4j</b>, <b>neo4j+s</b>, <b>neo4j+ssc</b> - establishes a {@link BoltConnection} to a given address with
 *     routing context, which is automatically parsed from the connection {@link URI}. In a clustered Neo4j DBMS the
 *     presence of routing context enables the server to perform server-side routing within the cluster if necessary.
 *     Effectively, using these schemes is sufficient for establishing a connection to a clustered Neo4j DBMS <b>that
 *     has server-side routing enabled</b>. However, it should <b>NOT</b> be confused with client-side routing that
 *     requires more contextual awareness and fits {@link BoltConnectionSource} with
 *     {@link RoutedBoltConnectionParameters} support better.</li>
 * </ul>
 * Supported additional parameters:
 * <ul>
 *     <li><b>eventLoopGroup</b> - Sets {@link EventLoopGroup} to be used instead of creating a new one. Defaults to
 *     {@literal null}.</li>
 *     <li><b>eventLoopThreads</b> - Sets the number of Event Loop threads. Defaults to {@literal null}. This option is
 *     used only when eventLoopGroup is {@literal null}.</li>
 *     <li><b>localAddress</b> - Sets {@link LocalAddress} to be used instead of {@link URI}. Defaults to
 *     {@literal null}.</li>
 *     <li><b>clock</b> - Sets the {@link Clock} to be used. Defaults to {@link Clock#systemUTC()}.</li>
 *     <li><b>domainNameResolver</b> - Sets the {@link DomainNameResolver} to be used. Defaults to
 *     {@link DefaultDomainNameResolver#getInstance()}.</li>
 *     <li><b>maxVersion</b> - Sets the meximum {@link BoltProtocolVersion} that will be negotiated. Defaults to
 *     {@literal null}.</li>
 *     <li><b>nettyTransport</b> - Defines Netty transport to be used. Supported values: auto (default), nio, io_uring
 *     (requires adding io_uring dependency explicitly, Netty 4.2+ only), epoll (requires adding epoll dependency
 *     explicitly), kqueue (requires adding kqueue dependency explicitly), local. The default auto mode selects local
 *     when localAddress is provided, otherwise it attempts to detect if native transport is available in runtime (the
 *     dependency needs to be added explicitly) and falls back to nio otherwise. When eventLoopGroup is not
 *     {@literal null}, this option defaults to nio.</li>
 *     <li> <b>enableFastOpen</b> - Enables client-side TCP Fast Open if it is available. Supported values: true and
 *     false (default). Note that only Netty native transports support this and extra system configuration may be
 *     needed. When either native transport or TCP Fast Open is unavaible, this option is ignored and is effectively
 *     false.</li>
 *     <li> <b>preferredCapabilities</b> - A {@link Set} of preferred {@link BoltCapability} that should be
 *     selected when server offers support for them during Bolt handshake. This set or individual entries in the set are
 *     ignored when no support is available or handshake does not support this feature at all.</li>
 * </ul>
 *
 * @since 4.0.0
 */
public final class NettyBoltConnectionProviderFactory implements BoltConnectionProviderFactory {
    private static final Set<String> SUPPORTED_SCHEMES =
            Set.of("bolt", "bolt+s", "bolt+ssc", "neo4j", "neo4j+s", "neo4j+ssc");

    /**
     * Creates a new instance of this factory.
     * <p>
     * It is used by {@link ServiceLoader}.
     */
    public NettyBoltConnectionProviderFactory() {}

    @Override
    public boolean supports(String scheme) {
        return SUPPORTED_SCHEMES.contains(scheme);
    }

    @Override
    public BoltConnectionProvider create(
            LoggingProvider loggingProvider,
            ValueFactory valueFactory,
            ObservationProvider observationProvider,
            Map<String, ?> additionalConfig) {
        var logger = loggingProvider.getLog(getClass());

        // get additional parameters
        var shutdownEventLoopGroupOnClose = false;
        Class<? extends Channel> channelClass;
        var localAddress = getConfigEntry(logger, additionalConfig, "localAddress", LocalAddress.class, () -> null);
        var fastOpen = getConfigEntry(logger, additionalConfig, "enableFastOpen", Boolean.class, () -> false);
        var eventLoopGroup =
                getConfigEntry(logger, additionalConfig, "eventLoopGroup", EventLoopGroup.class, () -> null);
        if (eventLoopGroup == null) {
            var factory = createEventLoopGroupFactory(logger, localAddress, additionalConfig);
            var size = getConfigEntry(logger, additionalConfig, "eventLoopThreads", Integer.class, () -> 0);
            if (fastOpen && !factory.fastOpenAvailable()) {
                logger.log(System.Logger.Level.WARNING, "Fast Open is not supported and will be ignored");
                fastOpen = false;
            }
            eventLoopGroup = factory.newEventLoopGroup(size);
            channelClass = factory.channelClass();
            shutdownEventLoopGroupOnClose = true;
        } else {
            var nettyTransport = determineTransportType(logger, localAddress, additionalConfig, "nio");
            logger.log(System.Logger.Level.TRACE, "Selected nettyTransport %s", nettyTransport);
            channelClass = nettyTransport.channelClass();
            if (fastOpen && !nettyTransport.fastOpenAvailable()) {
                logger.log(System.Logger.Level.WARNING, "Fast Open is not supported and will be ignored");
                fastOpen = false;
            }
        }
        var clock = getConfigEntry(logger, additionalConfig, "clock", Clock.class, Clock::systemUTC);
        var domainNameResolver = getConfigEntry(
                logger,
                additionalConfig,
                "domainNameResolver",
                DomainNameResolver.class,
                DefaultDomainNameResolver::getInstance);
        var maxVersion = getConfigEntry(logger, additionalConfig, "maxVersion", BoltProtocolVersion.class, () -> null);
        @SuppressWarnings("unchecked")
        Set<BoltCapability> preferredCapabilities =
                getConfigEntry(logger, additionalConfig, "preferredCapabilities", Set.class, Set::of);
        var preferredCapabilitiesMask = toBoltCapabilitiesMask(preferredCapabilities);

        return new NettyBoltConnectionProvider(
                eventLoopGroup,
                channelClass,
                clock,
                domainNameResolver,
                localAddress,
                maxVersion,
                fastOpen,
                preferredCapabilitiesMask,
                loggingProvider,
                valueFactory,
                shutdownEventLoopGroupOnClose,
                observationProvider);
    }

    private EventLoopGroupFactory createEventLoopGroupFactory(
            System.Logger logger, LocalAddress localAddress, Map<String, ?> additionalConfig) {
        var eventLoopThreadNamePrefix =
                getConfigEntry(logger, additionalConfig, "eventLoopThreadNamePrefix", String.class, () -> null);
        var nettyTransport = determineTransportType(logger, localAddress, additionalConfig, "auto");
        logger.log(System.Logger.Level.TRACE, "Selected nettyTransport %s", nettyTransport);
        return new EventLoopGroupFactory(eventLoopThreadNamePrefix, nettyTransport);
    }

    private NettyTransport determineTransportType(
            System.Logger logger,
            LocalAddress localAddress,
            Map<String, ?> additionalConfig,
            String defaultNettyTransport) {
        var nettyTransport =
                getConfigEntry(logger, additionalConfig, "nettyTransport", String.class, () -> defaultNettyTransport);
        return switch (nettyTransport) {
            case "auto" -> {
                if (localAddress != null) {
                    yield NettyTransport.local();
                } else if (NettyTransport.isIoUringAvailable()) {
                    yield NettyTransport.ioUring();
                } else if (NettyTransport.isEpollAvailable()) {
                    yield NettyTransport.epoll();
                } else if (NettyTransport.isKQueueAvailable()) {
                    yield NettyTransport.kqueue();
                } else {
                    yield NettyTransport.nio();
                }
            }
            case "nio" -> NettyTransport.nio();
            case "io_uring" -> NettyTransport.ioUring();
            case "epoll" -> NettyTransport.epoll();
            case "kqueue" -> NettyTransport.kqueue();
            case "local" -> NettyTransport.local();
            default -> throw new IllegalArgumentException("Unexpected nettyTransport value: " + nettyTransport);
        };
    }

    private static long toBoltCapabilitiesMask(Set<BoltCapability> boltConnectionCapabilities) {
        var mask = 0L;
        for (var boltConnectionCapability : boltConnectionCapabilities) {
            var id = boltConnectionCapability.id();
            if (id < 1 || id > 64) {
                throw new IllegalArgumentException("Bolt capability id must be between 1 and 64: " + id);
            }
            mask |= 1L << (id - 1);
        }
        return mask;
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
