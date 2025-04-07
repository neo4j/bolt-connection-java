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

import java.net.URI;
import java.util.Optional;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class QueryApiIT extends AbstractQueryApi {
    @SuppressWarnings("resource")
    @Container
    private static final Neo4jContainer<?> neo4jContainer = new Neo4jContainer<>(
                    DockerImageName.parse("neo4j:%s-enterprise"
                            .formatted(Optional.ofNullable(System.getenv("NEO4J_VERSION"))
                                    .orElse("2025.04.0"))))
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes");

    @Override
    URI uri() {
        return URI.create(neo4jContainer.getHttpUrl());
    }

    @Override
    String database() {
        return "neo4j";
    }

    @Override
    String username() {
        return "neo4j";
    }

    @Override
    String password() {
        return neo4jContainer.getAdminPassword();
    }
}
