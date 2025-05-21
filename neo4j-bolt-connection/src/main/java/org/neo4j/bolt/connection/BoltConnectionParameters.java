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

import java.util.Optional;
import org.neo4j.bolt.connection.message.Messages;

/**
 * A set of parameters used by {@link BoltConnectionSource} when getting {@link BoltConnection}.
 * @since 4.0.0
 * @see BoltConnectionSource
 * @see RoutedBoltConnectionParameters
 */
public interface BoltConnectionParameters {
    /**
     * An optional {@link AuthToken} that the given connection must use.
     * <p>
     * It is intended for cases when a specific {@link AuthToken} must be used that may differ from the
     * {@link BoltConnectionSource} default one.
     * <p>
     * {@link BoltConnectionSource} might pipeline {@link Messages#logoff()} and {@link Messages#logon(AuthToken)}
     * when it already has a suitable {@link BoltConnection}, the {@link ResponseHandler} must handle response errors in
     * such cases.
     * @return an optional {@link AuthToken} or {@code null} if {@link BoltConnectionSource} default should be used
     */
    AuthToken authToken();

    /**
     * An optional minimum {@link BoltProtocolVersion} that the connection must support.
     * @return an {@link Optional} of {@link BoltProtocolVersion}
     */
    BoltProtocolVersion minVersion();

    /**
     * Returns default {@link BoltConnectionParameters}.
     * @return the default {@link BoltConnectionParameters}
     */
    static BoltConnectionParameters defaultParameters() {
        return BoltConnectionParametersImpl.DEFAULT;
    }

    /**
     * Returns a new {@link BoltConnectionParameters.Builder} instance.
     * @return a new builder
     */
    static BoltConnectionParameters.Builder builder() {
        return new RoutingParametersBuilderImpl();
    }

    /**
     * A builder for creating {@link BoltConnectionParameters}.
     * @since 4.0.0
     */
    interface Builder {
        /**
         * Sets an {@link AuthToken} that the given connection must use.
         * <p>
         * It is intended for cases when a specific {@link AuthToken} must be used that may differ from the
         * {@link BoltConnectionSource} default one.
         * <p>
         * The default is {@code null}.
         * @param authToken the {@link AuthToken} or {@code null} to use the {@link BoltConnectionSource} default one
         * @return this builder
         */
        Builder withAuthToken(AuthToken authToken);

        /**
         * Sets an optional minimum {@link BoltProtocolVersion} that the connection must support.
         * <p>
         * The default is {@code null}.
         * @param minVersion the minimum {@link BoltProtocolVersion} or {@code null} if there is no minimum
         * @return this builder
         */
        Builder withMinVersion(BoltProtocolVersion minVersion);

        /**
         * Builds a new {@link BoltConnectionParameters}.
         * @return the new {@link BoltConnectionParameters}
         */
        BoltConnectionParameters build();
    }
}
