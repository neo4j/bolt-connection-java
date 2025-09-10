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

/**
 * Defines Bolt Capabilities used during Bolt Handshake.
 * @since 8.3.0
 */
public enum BoltCapability {
    /**
     * The Neo4j Fabric capability.
     */
    FABRIC(1);

    private final int id;

    BoltCapability(int id) {
        this.id = id;
    }

    /**
     * Returns the unique capability id.
     * @return the id
     */
    public int id() {
        return id;
    }
}
