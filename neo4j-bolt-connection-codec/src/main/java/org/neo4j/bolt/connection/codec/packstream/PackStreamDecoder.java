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
import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.neo4j.bolt.connection.codec.ReadInput;
import org.neo4j.bolt.connection.codec.packstream.struct.PackStreamStructure;
import org.neo4j.bolt.connection.values.Value;

public interface PackStreamDecoder {
    PackStreamType peekNextType(ReadInput input) throws IOException;

    void decodeNull(ReadInput input) throws IOException;

    boolean decodeBoolean(ReadInput input) throws IOException;

    long decodeInteger(ReadInput input) throws IOException;

    double decodeFloat(ReadInput input) throws IOException;

    byte[] decodeBytes(ReadInput input) throws IOException;

    String decodeString(ReadInput input) throws IOException;

    UUID decodeUuid(ReadInput input) throws IOException;

    Value decodeValue(ReadInput input) throws IOException;

    List<Value> decodeList(ReadInput input) throws IOException;

    <R extends Map<String, Value>, A> R decodeMap(ReadInput input, Collector<Map.Entry<String, Value>, A, R> collector)
            throws IOException;

    default Map<String, Value> decodeMap(ReadInput input) throws IOException {
        return decodeMap(input, Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    <T extends PackStreamStructure> T decodeStructure(ReadInput input, Class<T> structureCls) throws IOException;

    StructureDescriptor decodeStructureDescriptor(ReadInput input) throws IOException;

    record StructureDescriptor(long size, byte tagByte) {}
}
