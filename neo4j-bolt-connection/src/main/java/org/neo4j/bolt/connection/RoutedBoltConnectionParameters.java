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

public interface RoutedBoltConnectionParameters extends BoltConnectionParameters {
    AccessMode accessMode();

    DatabaseName databaseName();

    Consumer<DatabaseName> databaseNameConsumer();

    String homeDatabase();

    Set<String> bookmarks();

    String impersonatedUser();

    static RoutedBoltConnectionParameters defaultParameters() {
        return RoutedBoltConnectionParametersImpl.DEFAULT;
    }

    static Builder builder() {
        return new RoutingParametersBuilderImpl();
    }

    interface Builder extends BoltConnectionParameters.Builder {
        Builder withAuthToken(AuthToken authToken);

        Builder withMinVersion(BoltProtocolVersion minVersion);

        Builder withAccessMode(AccessMode accessMode);

        Builder withDatabaseName(DatabaseName databaseName);

        Builder withDatabaseNameConsumer(Consumer<DatabaseName> databaseNameConsumer);

        Builder withHomeDatabase(String homeDatabase);

        Builder withBookmarks(Set<String> bookmarks);

        Builder withImpersonatedUser(String impersonatedUser);

        RoutedBoltConnectionParameters build();
    }
}
