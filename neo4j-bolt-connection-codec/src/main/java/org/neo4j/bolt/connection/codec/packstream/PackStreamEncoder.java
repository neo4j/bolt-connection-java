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
package org.neo4j.bolt.connection.codec.packstream;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.neo4j.bolt.connection.codec.WriteOutput;
import org.neo4j.bolt.connection.codec.packstream.struct.PackStreamStructure;
import org.neo4j.bolt.connection.values.Value;

public interface PackStreamEncoder {
    void encodeNull(WriteOutput output) throws IOException;

    void encode(boolean value, WriteOutput output) throws IOException;

    void encode(long value, WriteOutput output) throws IOException;

    void encode(double value, WriteOutput output) throws IOException;

    void encode(byte[] bytes, WriteOutput output) throws IOException;

    void encode(String value, WriteOutput output) throws IOException;

    void encode(UUID value, WriteOutput output) throws IOException;

    void encode(Value value, WriteOutput output) throws IOException;

    void encode(List<Value> values, WriteOutput output) throws IOException;

    void encode(Map<String, Value> values, WriteOutput output) throws IOException;

    void encodeStructureHeader(int size, byte tagByte, WriteOutput output) throws IOException;

    <T extends PackStreamStructure> void encode(T structure, WriteOutput output) throws IOException;
}
