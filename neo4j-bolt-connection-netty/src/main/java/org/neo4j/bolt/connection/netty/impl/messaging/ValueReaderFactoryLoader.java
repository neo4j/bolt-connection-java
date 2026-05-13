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
package org.neo4j.bolt.connection.netty.impl.messaging;

import java.util.Optional;
import java.util.ServiceLoader;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.codec.network.ValueDecoderFactory;

public final class ValueReaderFactoryLoader {
    private static final String DEFAULT_FACTORY_NAME = "org.neo4j.bolt.connection.packstream.ValueReaderFactoryImpl";

    private final System.Logger logger;

    private final ValueDecoderFactory readerFactory;

    public ValueReaderFactoryLoader(LoggingProvider logging) {
        this.logger = logging.getLog(getClass());
        this.readerFactory = findReader().orElse(null);
    }

    public Optional<ValueDecoderFactory> readerFactory() {
        return Optional.ofNullable(readerFactory);
    }

    private Optional<ValueDecoderFactory> findReader() {
        var result = Optional.<ValueDecoderFactory>empty();
        try {
            var serviceLoader = ServiceLoader.load(
                    ValueDecoderFactory.class, this.getClass().getClassLoader());
            result = serviceLoader.stream().map(ServiceLoader.Provider::get).findFirst();
        } catch (Exception e) {
            logger.log(System.Logger.Level.WARNING, "Loading of ValueReaderFactory service has failed", e);
        }
        if (result.isEmpty()) {
            try {
                // an extra attempt in case the factory is visible
                @SuppressWarnings("Java9ReflectionClassVisibility")
                var factoryCls = Class.forName(DEFAULT_FACTORY_NAME);
                if (ValueDecoderFactory.class.isAssignableFrom(factoryCls)) {
                    var factory =
                            (ValueDecoderFactory) factoryCls.getConstructor().newInstance();
                    result = Optional.of(factory);
                }
            } catch (Exception e) {
                logger.log(
                        System.Logger.Level.ERROR,
                        "Failed to load default '%s' factory".formatted(DEFAULT_FACTORY_NAME),
                        e);
            }
        }
        result.ifPresentOrElse(
                factory -> logger.log(System.Logger.Level.TRACE, "Selected '%s' factory", factory.getClass()),
                () -> logger.log(System.Logger.Level.WARNING, "No factory has been found"));
        return result;
    }
}
