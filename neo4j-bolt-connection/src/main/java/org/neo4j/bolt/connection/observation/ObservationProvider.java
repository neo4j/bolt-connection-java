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
package org.neo4j.bolt.connection.observation;

import java.net.URI;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.neo4j.bolt.connection.BoltProtocolVersion;

/**
 * An observation provider responsible for managing new and existing {@link Observation} instances.
 * @since 7.0.0
 */
public interface ObservationProvider {
    /**
     * Creates an observation for Bolt exchange.
     * <p>
     * The returned observation MUST be started.
     *
     * @return the new observation
     */
    BoltExchangeObservation boltExchange(
            ImmutableObservation observationParent,
            String host,
            int port,
            BoltProtocolVersion boltVersion,
            BiConsumer<String, String> setter);

    /**
     * Creates an observation for HTTP exchange.
     * <p>
     * The returned observation MUST be started.
     *
     * @return the new observation
     */
    HttpExchangeObservation httpExchange(
            ImmutableObservation observationParent,
            URI uri,
            String method,
            String uriTemplate,
            BiConsumer<String, String> setter);

    /**
     * Returns an observation from the current scope if there is any.
     * @return an observation or {@literal null} if there is no observation in the scope
     */
    ImmutableObservation scopedObservation();

    /**
     * Runs the supplied logic in a scope of the given observation. If the supplied observation is {@literal null},
     * the supplier is executed without the scope.
     * @param observation the observation or {@literal null}
     * @param supplier the supplier
     * @return the supplier's result
     * @param <T> the supplier's result type
     */
    <T> T supplyInScope(ImmutableObservation observation, Supplier<T> supplier);
}
