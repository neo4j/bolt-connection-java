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
package org.neo4j.bolt.connection.routed.impl.cluster;

import java.util.Set;
import java.util.concurrent.CompletionStage;
import org.neo4j.bolt.connection.AccessMode;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.RoutedBoltConnectionParameters;
import org.neo4j.bolt.connection.routed.ClusterCompositionLookupResult;
import org.neo4j.bolt.connection.routed.RoutingTable;

public interface RoutingTableHandler extends RoutingErrorHandler {
    Set<BoltServerAddress> servers();

    boolean isRoutingTableAged();

    CompletionStage<RoutingTable> ensureRoutingTable(RoutedBoltConnectionParameters parameters);

    CompletionStage<RoutingTable> updateRoutingTable(ClusterCompositionLookupResult compositionLookupResult);

    RoutingTable routingTable();

    boolean isStaleFor(AccessMode mode);
}
