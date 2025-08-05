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
package org.neo4j.bolt.connection.routed;

import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.neo4j.bolt.connection.BoltConnectionParameters;
import org.neo4j.bolt.connection.BoltConnectionSource;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.RoutedBoltConnectionParameters;
import org.neo4j.bolt.connection.observation.ImmutableObservation;

/**
 * Provides cluster composition lookup capabilities and initial router address resolution.
 */
public interface Rediscovery {
    CompletionStage<ClusterCompositionLookupResult> lookupClusterComposition(
            RoutingTable routingTable,
            Function<BoltServerAddress, BoltConnectionSource<BoltConnectionParameters>> connectionSourceGetter,
            RoutedBoltConnectionParameters parameters,
            ImmutableObservation parentObservation);

    List<BoltServerAddress> resolve() throws UnknownHostException;
}
