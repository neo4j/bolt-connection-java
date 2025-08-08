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
package org.neo4j.bolt.connection.pooled.observation;

import java.net.URI;
import org.neo4j.bolt.connection.observation.ImmutableObservation;
import org.neo4j.bolt.connection.observation.Observation;
import org.neo4j.bolt.connection.observation.ObservationProvider;

/**
 * An {@link ObservationProvider} responsible for creating new {@link Observation} instances for connection pool and its
 * connections.
 * @since 7.0.0
 */
public interface PoolObservationProvider extends ObservationProvider {
    /**
     * Creates an {@link Observation} for a new connection pool creation.
     * <p>
     * The returned observation MUST be started.
     *
     * @param id the pool id
     * @param uri the target {@link URI}
     * @param maxSize the maximum connections allowed, {@literal -1} for unbounded
     * @return a new observation
     */
    Observation connectionPoolCreate(String id, URI uri, int maxSize);

    /**
     * Creates an {@link Observation} for connection pool closure.
     * <p>
     * The returned observation MUST be started.
     *
     * @param id the pool id
     * @return a new observation
     */
    Observation connectionPoolClose(String id, URI uri);

    /**
     * Creates an {@link Observation} for a new connection creation.
     * <p>
     * The returned observation MUST be started.
     *
     * @param id the pool id
     * @return a new observation
     */
    Observation pooledConnectionCreate(String id, URI uri);

    /**
     * Creates an {@link Observation} for connection closure.
     * <p>
     * The returned observation MUST be started.
     *
     * @param id the pool id
     * @return a new observation
     */
    Observation pooledConnectionClose(String id, URI uri);

    /**
     * Creates an {@link Observation} for connection aquisition.
     * <p>
     * The returned observation MUST be started.
     *
     * @param id the pool id
     * @return a new observation
     */
    Observation pooledConnectionAcquire(String id, URI uri);

    /**
     * Creates an {@link Observation} for connection usage.
     * <p>
     * The returned observation MUST be started.
     *
     * @param parentObsevation the parent observation
     * @param id the pool id
     * @return a new observation
     */
    Observation pooledConnectionInUse(ImmutableObservation parentObsevation, String id, URI uri);
}
