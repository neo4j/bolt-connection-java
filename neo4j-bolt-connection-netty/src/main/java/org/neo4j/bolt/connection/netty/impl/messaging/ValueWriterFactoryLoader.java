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
import org.neo4j.bolt.connection.codec.network.ValueEncoderFactory;

public final class ValueWriterFactoryLoader {
    private static final String DEFAULT_FACTORY_NAME = "org.neo4j.bolt.connection.packstream.ValueWriterFactoryImpl";

    private final System.Logger logger;

    private final ValueEncoderFactory writerFactory;

    public ValueWriterFactoryLoader(LoggingProvider logging) {
        this.logger = logging.getLog(getClass());
        this.writerFactory = findWriter().orElse(null);
    }

    public Optional<ValueEncoderFactory> writerFactory() {
        return Optional.ofNullable(writerFactory);
    }

    private Optional<ValueEncoderFactory> findWriter() {
        var result = Optional.<ValueEncoderFactory>empty();
        try {
            var serviceLoader = ServiceLoader.load(
                    ValueEncoderFactory.class, this.getClass().getClassLoader());
            result = serviceLoader.stream().map(ServiceLoader.Provider::get).findFirst();
        } catch (Exception e) {
            logger.log(System.Logger.Level.WARNING, "Loading of ValueWriterFactory service has failed", e);
        }
        if (result.isEmpty()) {
            try {
                // an extra attempt in case the factory is visible
                @SuppressWarnings("Java9ReflectionClassVisibility")
                var factoryCls = Class.forName(DEFAULT_FACTORY_NAME);
                if (ValueEncoderFactory.class.isAssignableFrom(factoryCls)) {
                    var factory =
                            (ValueEncoderFactory) factoryCls.getConstructor().newInstance();
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
