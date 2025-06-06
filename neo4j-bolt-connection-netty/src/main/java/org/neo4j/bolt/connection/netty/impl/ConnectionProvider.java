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
package org.neo4j.bolt.connection.netty.impl;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.bolt.connection.BoltAgent;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.MetricsListener;
import org.neo4j.bolt.connection.NotificationConfig;
import org.neo4j.bolt.connection.SecurityPlan;
import org.neo4j.bolt.connection.netty.impl.spi.Connection;
import org.neo4j.bolt.connection.values.Value;

public interface ConnectionProvider {

    CompletionStage<Connection> acquireConnection(
            BoltServerAddress address,
            SecurityPlan securityPlan,
            RoutingContext routingContext,
            Map<String, Value> authMap,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            CompletableFuture<Long> latestAuthMillisFuture,
            NotificationConfig notificationConfig,
            MetricsListener metricsListener);
}
