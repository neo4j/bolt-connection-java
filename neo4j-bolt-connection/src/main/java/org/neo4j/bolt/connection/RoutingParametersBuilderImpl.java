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

final class RoutingParametersBuilderImpl extends BoltConnectionParametersBuilderImpl
        implements RoutedBoltConnectionParameters.Builder {
    private AccessMode accessMode = AccessMode.WRITE;
    private DatabaseName databaseName;
    private Consumer<DatabaseName> databaseNameConsumer = ignored -> {};
    private String homeDatabase;
    private Set<String> bookmarks = Set.of();
    private String impersonatedUser;

    @Override
    public RoutedBoltConnectionParameters.Builder withAuthToken(AuthToken authToken) {
        super.withAuthToken(authToken);
        return this;
    }

    @Override
    public RoutedBoltConnectionParameters.Builder withMinVersion(BoltProtocolVersion minVersion) {
        super.withMinVersion(minVersion);
        return this;
    }

    @Override
    public RoutedBoltConnectionParameters.Builder withAccessMode(AccessMode accessMode) {
        this.accessMode = accessMode;
        return this;
    }

    @Override
    public RoutedBoltConnectionParameters.Builder withDatabaseName(DatabaseName databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    @Override
    public RoutedBoltConnectionParameters.Builder withDatabaseNameConsumer(
            Consumer<DatabaseName> databaseNameConsumer) {
        this.databaseNameConsumer = databaseNameConsumer;
        return this;
    }

    @Override
    public RoutedBoltConnectionParameters.Builder withHomeDatabase(String homeDatabase) {
        this.homeDatabase = homeDatabase;
        return this;
    }

    @Override
    public RoutedBoltConnectionParameters.Builder withBookmarks(Set<String> bookmarks) {
        this.bookmarks = bookmarks;
        return this;
    }

    @Override
    public RoutedBoltConnectionParameters.Builder withImpersonatedUser(String impersonatedUser) {
        this.impersonatedUser = impersonatedUser;
        return this;
    }

    @Override
    public RoutedBoltConnectionParameters build() {
        return new RoutedBoltConnectionParametersImpl(
                authToken,
                minVersion,
                accessMode,
                databaseName,
                databaseNameConsumer,
                homeDatabase,
                bookmarks,
                impersonatedUser);
    }
}
