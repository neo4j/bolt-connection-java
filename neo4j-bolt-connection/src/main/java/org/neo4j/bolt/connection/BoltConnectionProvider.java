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
import org.neo4j.bolt.connection.observation.ImmutableObservation;

/**
 * A Neo4j <a href="https://neo4j.com/docs/bolt/current/bolt">Bolt Protocol</a> connection provider.
 * <p>
 * Its main objective is to establish Bolt connections.
 * <p>
 * Its intances are expected to be created using {@link BoltConnectionProviderFactory} that may be discovered using the
 * {@link java.util.ServiceLoader}.
 *
 * @since 1.0.0
 */
public interface BoltConnectionProvider {
    /**
     * Connects to the given {@link URI} using the provided parameters and returns {@link BoltConnection} instance.
     *
     * @param uri                         the connection {@link URI}
     * @param routingContextAddress       the address to be used in the 'address' field of routing context. This applies to
     *                                    URI schemes that support routing context only. When set to {@code null}, the default
     *                                    behaviour of getting the address from the URI applies. This parameter should be used
     *                                    when an explicit address that differs from the one in the URI should be used.
     * @param boltAgent                   the {@link BoltAgent}
     * @param userAgent                   the User Agent
     * @param connectTimeoutMillis        the connection timeout, {@literal 0} or negative disables timeout
     * @param initialisationTimeoutMillis the connection initialisation timeout, includes SSL and Bolt Handshake, {@literal 0} or negative disables timeout
     * @param securityPlan                the {@link SecurityPlan}
     * @param authToken                   the {@link AuthToken}
     * @param minVersion                  the minimum {@link BoltProtocolVersion}
     * @param notificationConfig          the {@link NotificationConfig}, this usually used in the Bolt {@code HELLO} message
     * @param parentObservation           the parent {@link ImmutableObservation} that should be used as a parent for nested observations
     * @return the {@link BoltConnection} instance
     */
    CompletionStage<BoltConnection> connect(
            URI uri,
            String routingContextAddress,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            long initialisationTimeoutMillis,
            SecurityPlan securityPlan,
            AuthToken authToken,
            BoltProtocolVersion minVersion,
            NotificationConfig notificationConfig,
            ImmutableObservation parentObservation);

    /**
     * Closes the {@link BoltConnectionProvider} instance.
     *
     * @return the close {@link CompletionStage}
     */
    CompletionStage<Void> close();
}
