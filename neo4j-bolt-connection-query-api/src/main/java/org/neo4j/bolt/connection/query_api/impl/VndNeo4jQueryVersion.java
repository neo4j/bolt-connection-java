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
package org.neo4j.bolt.connection.query_api.impl;

import java.util.Objects;

enum VndNeo4jQueryVersion {
    V1_0(1, 0, "application/vnd.neo4j.query"),
    V1_1(1, 1, "application/vnd.neo4j.query.v1.1");

    private final int major;
    private final int minor;
    private final String value;

    VndNeo4jQueryVersion(int major, int minor, String value) {
        this.major = major;
        this.minor = minor;
        this.value = Objects.requireNonNull(value);
    }

    public boolean isBefore(VndNeo4jQueryVersion other) {
        return major < other.major || (major == other.major && minor < other.minor);
    }

    public String value() {
        return value;
    }
}
