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
package org.neo4j.bolt.connection.pooled;

import java.util.concurrent.CompletionStage;
import org.neo4j.bolt.connection.AuthToken;
import org.neo4j.bolt.connection.BoltConnection;
import org.neo4j.bolt.connection.exception.BoltFailureException;

/**
 * A manager of {@link AuthToken} instances used by {@link PooledBoltConnectionSource} during establishing a new
 * {@link BoltConnection}.
 *
 * @since 4.0.0
 */
public interface AuthTokenManager {
    /**
     * Returns a {@link CompletionStage} for a valid {@link AuthToken}.
     */
    CompletionStage<AuthToken> getToken();

    /**
     * Handles {@link BoltFailureException} that is created based on the server's error response together with the used
     * {@link AuthToken}.
     * @param authToken the {@link AuthToken} used
     * @param exception the {@link BoltFailureException} representing an error response
     * @return the exception that the {@link PooledBoltConnectionSource} MUST use
     */
    BoltFailureException handleBoltFailureException(AuthToken authToken, BoltFailureException exception);
}
