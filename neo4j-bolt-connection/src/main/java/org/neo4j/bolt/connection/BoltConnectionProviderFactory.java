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
package org.neo4j.bolt.connection;

import java.util.Map;
import org.neo4j.bolt.connection.values.ValueFactory;

/**
 * A factory for creating instances of {@link BoltConnectionProvider}.
 * <p>
 * It should be discovered using the {@link java.util.ServiceLoader}.
 * @since 4.0.0
 */
public interface BoltConnectionProviderFactory {
    /**
     * Indicates if {@link BoltConnectionProvider} instances created by this factory support the given
     * {@link java.net.URI} scheme.
     * @param scheme the {@link java.net.URI} scheme
     * @return {@code true} if support is available, {@code false} if not
     */
    boolean supports(String scheme);

    /**
     * Creates a new {@link BoltConnectionProvider} instance.
     * @param loggingProvider the {@link LoggingProvider} that should be used for logging
     * @param valueFactory the {@link ValueFactory} that should be used for value management
     * @param metricsListener the {@link MetricsListener} that should be used for metrics
     * @param additionalConfig the additional config with arbitrary values that may be used by factories that recognise
     *                         them
     * @return the new {@link BoltConnectionProvider} instance
     */
    BoltConnectionProvider create(
            LoggingProvider loggingProvider,
            ValueFactory valueFactory,
            MetricsListener metricsListener,
            Map<String, ?> additionalConfig);

    /**
     * Returns the order of this factory.
     * <p>
     * This may be used for sorting factories that support the same scheme in order to select the one with highest
     * precedence.
     * <p>
     * The higher the value is, the lower the precedence is. For example, the {@link Integer#MIN_VALUE} has highest
     * precedence.
     * <p>
     * The default is {@link Integer#MAX_VALUE}.
     * @return the order
     */
    default int getOrder() {
        return Integer.MAX_VALUE;
    }
}
