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

import java.util.concurrent.CompletionStage;

/**
 * A source of Neo4j <a href="https://neo4j.com/docs/bolt/current/bolt">Bolt Protocol</a> connections, typically to a
 * specific Neo4j DBMS.
 * <p>
 * There are two main types of implementations at the moment:
 * <ol>
 *     <li>Pooling implementation - supplies {@link BoltConnection} instance that is pooled automatically.</li>
 *     <li>Routing implementation - supplies {@link BoltConnection} instance that connects to a Neo4j DBMS cluster
 *     member that is selected based on automatically managed routing table information and the
 *     {@link RoutedBoltConnectionParameters} provided by the user.</li>
 * </ol>
 * @param <T> the type of parameters supported
 * @since 4.0.0
 */
public interface BoltConnectionSource<T extends BoltConnectionParameters> {
    /**
     * Gets {@link BoltConnection} from the source.
     * @return the {@link BoltConnection}
     */
    CompletionStage<BoltConnection> getConnection();

    /**
     * Gets {@link BoltConnection} from the source using the provided {@link BoltConnectionParameters}.
     * @param parameters the {@link BoltConnectionParameters}
     * @return the {@link BoltConnection}
     */
    CompletionStage<BoltConnection> getConnection(T parameters);

    /**
     * Verifies connectivity.
     * @return the {@link CompletionStage} that fails of there is an issue
     */
    CompletionStage<Void> verifyConnectivity();

    /**
     * Checks if there is a {@link BoltConnection} that supports multi database feature.
     * @return the {@link CompletionStage}
     */
    CompletionStage<Boolean> supportsMultiDb();

    /**
     * Checks if there is a {@link BoltConnection} that supports re-auth feature.
     * @return the {@link CompletionStage}
     */
    CompletionStage<Boolean> supportsSessionAuth();

    /**
     * Closes the {@link BoltConnectionSource} instance.
     * @return the close {@link CompletionStage}
     */
    CompletionStage<Void> close();
}
