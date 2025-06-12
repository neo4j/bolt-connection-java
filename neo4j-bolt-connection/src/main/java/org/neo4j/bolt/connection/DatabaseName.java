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

import static org.neo4j.bolt.connection.DatabaseNameImpl.DEFAULT_DATABASE;
import static org.neo4j.bolt.connection.DatabaseNameImpl.DEFAULT_DATABASE_NAME;
import static org.neo4j.bolt.connection.DatabaseNameImpl.SYSTEM_DATABASE;
import static org.neo4j.bolt.connection.DatabaseNameImpl.SYSTEM_DATABASE_NAME;

import java.util.Objects;
import java.util.Optional;

public sealed interface DatabaseName permits DatabaseNameImpl {
    Optional<String> databaseName();

    String description();

    /**
     * Returns a default database as deternimed by the server.
     *
     * @since 4.0.0
     * @return the default database
     */
    static DatabaseName defaultDatabase() {
        return DEFAULT_DATABASE;
    }

    /**
     * Returns the system database.
     *
     * @since 4.0.0
     * @return the system database
     */
    static DatabaseName systemDatabase() {
        return SYSTEM_DATABASE;
    }

    /**
     * Returns a {@link DatabaseName} instance for the supplied name.
     *
     * @since 4.0.0
     * @param name the database name
     * @return the {@link DatabaseName} instance
     */
    static DatabaseName database(String name) {
        if (Objects.equals(name, DEFAULT_DATABASE_NAME)) {
            return defaultDatabase();
        } else if (Objects.equals(name, SYSTEM_DATABASE_NAME)) {
            return systemDatabase();
        }
        return new DatabaseNameImpl(name, name);
    }
}
