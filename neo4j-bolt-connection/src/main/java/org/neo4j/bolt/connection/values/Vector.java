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
package org.neo4j.bolt.connection.values;

/**
 * Represents Neo4j Vector type.
 *
 * @since 6.0.0
 */
public interface Vector {
    /**
     * Vector element type.
     * @return the element type
     */
    Class<?> elementType();

    /**
     * Vector elements as an array.
     * @return the array of vector elements
     */
    Object elements();
}
