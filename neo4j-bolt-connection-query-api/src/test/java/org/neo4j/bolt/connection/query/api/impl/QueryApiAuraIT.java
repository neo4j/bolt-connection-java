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
package org.neo4j.bolt.connection.query.api.impl;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.net.URI;
import org.junit.jupiter.api.BeforeAll;

final class QueryApiAuraIT extends AbstractQueryApi {
    private static URI baseUri;
    private static String database;
    private static String username;
    private static String password;

    @BeforeAll
    static void beforeAll() {
        baseUri = URI.create(assumeEnvVar("NEO4J_URI"));
        database = assumeEnvVar("AURA_INSTANCEID");
        username = assumeEnvVar("NEO4J_USERNAME");
        password = assumeEnvVar("NEO4J_PASSWORD");
    }

    @Override
    URI uri() {
        return baseUri;
    }

    @Override
    String database() {
        return database;
    }

    @Override
    String username() {
        return username;
    }

    @Override
    String password() {
        return password;
    }

    private static String assumeEnvVar(String envVar) {
        var value = System.getenv(envVar);
        assumeTrue(
                value != null && !value.isEmpty(), "Skipping tests: %s environment variable not set".formatted(envVar));
        return value;
    }
}
