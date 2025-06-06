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
package org.neo4j.bolt.connection.query_api;

import java.util.Map;
import java.util.Set;
import org.neo4j.bolt.connection.BoltConnectionProvider;
import org.neo4j.bolt.connection.BoltConnectionProviderFactory;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.MetricsListener;
import org.neo4j.bolt.connection.query_api.impl.QueryApiBoltConnectionProvider;
import org.neo4j.bolt.connection.values.ValueFactory;

/**
 * A factory that creates instances of {@link BoltConnectionProvider} that connect to Neo4j Query API.
 * <p>
 * <b>The Neo4j Query API support is experimental.</b>
 * @since 4.0.0
 */
public final class QueryApiBoltConnectionProviderFactory implements BoltConnectionProviderFactory {
    private static final Set<String> SUPPORTED_SCHEMES = Set.of("http", "https");

    /**
     * Creates a new instance of this factory.
     * <p>
     * It is used by {@link java.util.ServiceLoader}.
     */
    public QueryApiBoltConnectionProviderFactory() {}

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
        return new QueryApiBoltConnectionProvider(loggingProvider, valueFactory);
    }
}
