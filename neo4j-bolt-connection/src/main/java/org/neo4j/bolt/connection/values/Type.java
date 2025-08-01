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

public enum Type {
    ANY,
    BOOLEAN,
    BYTES,
    STRING,
    NUMBER,
    INTEGER,
    FLOAT,
    LIST,
    MAP,
    NODE,
    RELATIONSHIP,
    PATH,
    POINT,
    DATE,
    TIME,
    LOCAL_TIME,
    LOCAL_DATE_TIME,
    DATE_TIME,
    DURATION,
    VECTOR,
    NULL
}
