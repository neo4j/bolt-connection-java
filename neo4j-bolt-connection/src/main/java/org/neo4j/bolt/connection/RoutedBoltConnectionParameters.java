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

import java.util.Set;
import java.util.function.Consumer;

/**
 * An extended {@link BoltConnectionParameters} version that includes Neo4j routing parameters and is used by
 * {@link BoltConnectionSource} instances that implement routing.
 * @since 4.0.0
 */
public interface RoutedBoltConnectionParameters extends BoltConnectionParameters {
    /**
     * An {@link AccessMode} that the connection must support.
     * @return the access mode
     */
    AccessMode accessMode();

    /**
     * A database name that the connection should be established for.
     * <p>
     * In a clustered Neo4j DBMS not every cluster member may host the desired database, so the
     * {@link BoltConnectionSource} must know which database the given connection must be established for.
     * <p>
     * When it is set to {@code null}, the user's home database must be used on Bolt 4.1 and above. For previous Bolt
     * versions, the default database determined by the server is used instead.
     * @return the database name
     */
    DatabaseName databaseName();

    /**
     * A database name consumer that will be notified with the database name used for the given connection.
     * <p>
     * This option is only used when the {@link #databaseName()} is set to {@code null}. It is intended for getting
     * user's home database when it gets resolved.
     *
     * @since 4.0.0
     * @return the database name consumer
     */
    Consumer<DatabaseName> databaseNameListener();

    /**
     * A home database name hint.
     * <p>
     * This option is only used when the {@link #databaseName()} is set to {@code null}.
     * <p>
     * The objective is to minimise the need for an explicit home database resolution that requires an extra network
     * exchange.
     * <p>
     * The home database hint should be used to check if a server-side routing (SSR) enabled connection may be
     * returned without having to explicitly resolve home database. If such connection is returned, the following
     * applies:
     * <ul>
     *     <li>The {@link #databaseNameListener()} is not notified as no home database resolution takes place.</li>
     *     <li>This value MUST NOT be considered to be the user's home database as it may have changed. The home
     *     database can be determined later by sending {@code BEGIN} or autocommit {@code RUN} message without database
     *     name set. In that case, the server includes the home database in the response summary. This behaviour
     *     only applies from Bolt 5.8 and higher.</li>
     *     <li>The server-side routing is required to make sure the messages can be forwarded to a relevant server if
     *     this value has actually changed and is no longer true. However, such forwarding may inccur extra network
     *     exchanges within the cluster.</li>
     * </ul>
     *
     * @since 4.0.0
     * @return the database name hint
     */
    String homeDatabaseHint();

    /**
     * Bookmarks used for routing.
     * @return the routing bookmarks.
     */
    Set<String> bookmarks();

    /**
     * An optional impersonated user.
     * @return the impersonated user or {@code null}
     */
    String impersonatedUser();

    /**
     * Returns default {@link RoutedBoltConnectionParameters}.
     * @return the default {@link RoutedBoltConnectionParameters}
     */
    static RoutedBoltConnectionParameters defaultParameters() {
        return RoutedBoltConnectionParametersImpl.DEFAULT;
    }

    /**
     * Returns a new {@link RoutedBoltConnectionParameters.Builder} instance.
     * @return a new builder
     */
    static Builder builder() {
        return new RoutingParametersBuilderImpl();
    }

    /**
     * A builder for creating {@link RoutedBoltConnectionParameters}.
     * @since 4.0.0
     */
    interface Builder extends BoltConnectionParameters.Builder {
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
         * Sets an {@link AccessMode} that the connection must support.
         * <p>
         * The default is {@link AccessMode#WRITE}.
         * @param accessMode the access mode
         * @return this builder
         */
        Builder withAccessMode(AccessMode accessMode);

        /**
         * Sets a database that the connection must lead to.
         * <p>
         * The default is {@code null}.
         * @param databaseName the database name or {@code null} for home database
         * @return this builder
         */
        Builder withDatabaseName(DatabaseName databaseName);

        /**
         * Sets a database name consumer that will be notified with the database name used for the given connection.
         * <p>
         * The default is a noop consumer.
         * @param databaseNameListener the database name consumer
         * @return this builder
         */
        Builder withDatabaseNameListener(Consumer<DatabaseName> databaseNameListener);

        /**
         * Sets a home database name hint.
         * <p>
         * The default is {@code null}.
         * @param homeDatabaseHint the home database hint or {@code null}
         * @return this builder
         */
        Builder withHomeDatabaseHint(String homeDatabaseHint);

        /**
         * Sets routing bookmarks.
         * <p>
         * The default is an empty set.
         * @param bookmarks the bookmarks
         * @return this builder
         */
        Builder withBookmarks(Set<String> bookmarks);

        /**
         * Sets impersonated user.
         * <p>
         * The default is {@code null}.
         * @param impersonatedUser the impersonated user or {@code null}
         * @return this builder
         */
        Builder withImpersonatedUser(String impersonatedUser);

        /**
         * Builds a new {@link RoutedBoltConnectionParameters}.
         * @return the new {@link RoutedBoltConnectionParameters}
         */
        RoutedBoltConnectionParameters build();
    }
}
