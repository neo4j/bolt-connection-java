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

import java.net.URI;
import java.util.concurrent.CompletionStage;

/**
 * A Neo4j <a href="https://neo4j.com/docs/bolt/current/bolt">Bolt Protocol</a> connection provider.
 * <p>
 * Its main objective is to establish Bolt connections.
 * <p>
 * Its intances are expected to be created using {@link BoltConnectionProviderFactory} that may be discovered using the
 * {@link java.util.ServiceLoader}.
 * @since 1.0.0
 */
public interface BoltConnectionProvider {
    /**
     * Connects to the given {@link URI} using the provided parameters and returns {@link BoltConnection} instance.
     * @param uri the connection {@link URI}
     * @param routingContext the {@link RoutingContext}, this usually used in the Bolt {@code HELLO} message
     * @param boltAgent the {@link BoltAgent}
     * @param userAgent the User Agent
     * @param connectTimeoutMillis the connection timeout
     * @param securityPlan the {@link SecurityPlan}
     * @param authToken the {@link AuthToken}
     * @param minVersion the minimum {@link BoltProtocolVersion}
     * @param notificationConfig the {@link NotificationConfig}, this usually used in the Bolt {@code HELLO} message
     * @return the {@link BoltConnection} instance
     */
    CompletionStage<BoltConnection> connect(
            URI uri,
            RoutingContext routingContext,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            SecurityPlan securityPlan,
            AuthToken authToken,
            BoltProtocolVersion minVersion,
            NotificationConfig notificationConfig);

    /**
     * Closes the {@link BoltConnectionProvider} instance.
     * @return the close {@link CompletionStage}
     */
    CompletionStage<Void> close();
}
