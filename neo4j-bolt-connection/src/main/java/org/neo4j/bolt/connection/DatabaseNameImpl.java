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

record DatabaseNameImpl(String name, String description) implements DatabaseName {
    static final String DEFAULT_DATABASE_NAME = null;
    static final String SYSTEM_DATABASE_NAME = "system";
    static final DatabaseName DEFAULT_DATABASE = new DatabaseNameImpl(DEFAULT_DATABASE_NAME, "<default database>");
    static final DatabaseName SYSTEM_DATABASE = new DatabaseNameImpl(SYSTEM_DATABASE_NAME, SYSTEM_DATABASE_NAME);

    @Override
    public Optional<String> databaseName() {
        return Optional.ofNullable(name);
    }
}
