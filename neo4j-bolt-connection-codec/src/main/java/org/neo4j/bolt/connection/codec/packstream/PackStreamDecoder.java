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
import org.neo4j.bolt.connection.codec.packstream.struct.PackStreamStructureDecoder;
import org.neo4j.bolt.connection.values.Value;

/**
 * A decoder implementing PackStream.
 * @since 12.1.0
 */
public interface PackStreamDecoder {
    /**
     * Peeks the next {@link PackStreamType}.
     * @param input the input to read from
     * @return the next type
     * @throws IOException if I/O error occurs
     */
    PackStreamType peekNextType(ReadInput input) throws IOException;

    /**
     * Decodes {@link PackStreamType#NULL}.
     * @param input  the input to read from
     * @throws IOException if I/O error occurs
     */
    void decodeNull(ReadInput input) throws IOException;

    /**
     * Decodes {@link PackStreamType#BOOLEAN}.
     * @param input the input to read from
     * @return the value
     * @throws IOException if I/O error occurs
     */
    boolean decodeBoolean(ReadInput input) throws IOException;

    /**
     * Decodes {@link PackStreamType#INTEGER}.
     * @param input the input to read from
     * @return the value
     * @throws IOException if I/O error occurs
     */
    long decodeInteger(ReadInput input) throws IOException;

    /**
     * Decodes {@link PackStreamType#FLOAT}.
     * @param input the input to read from
     * @return the value
     * @throws IOException if I/O error occurs
     */
    double decodeFloat(ReadInput input) throws IOException;

    /**
     * Decodes {@link PackStreamType#BYTES}.
     * @param input the input to read from
     * @return the value
     * @throws IOException if I/O error occurs
     */
    byte[] decodeBytes(ReadInput input) throws IOException;

    /**
     * Decodes {@link PackStreamType#STRING}.
     * @param input the input to read from
     * @return the value
     * @throws IOException if I/O error occurs
     */
    String decodeString(ReadInput input) throws IOException;

    /**
     * Decodes {@link PackStreamType#UUID}.
     * @param input the input to read from
     * @return the value
     * @throws IOException if I/O error occurs
     */
    UUID decodeUuid(ReadInput input) throws IOException;

    /**
     * Decodes PackStream value to {@link Value}.
     * @param input the input to read from
     * @return the value
     * @throws IOException if I/O error occurs
     */
    Value decodeValue(ReadInput input) throws IOException;

    /**
     * Decodes {@link PackStreamType#LIST}.
     * @param input the input to read from
     * @return the value
     * @throws IOException if I/O error occurs
     */
    List<Value> decodeList(ReadInput input) throws IOException;

    /**
     * Decodes {@link PackStreamType#DICTIONARY}.
     * @param input the input to read from
     * @param collector the collector of dictionary entries
     * @return the value
     * @param <R> the result type
     * @param <A> the accumulator type
     * @throws IOException if I/O error occurs
     */
    <R extends Map<String, Value>, A> R decodeDictionary(
            ReadInput input, Collector<Map.Entry<String, Value>, A, R> collector) throws IOException;

    /**
     * Decodes {@link PackStreamType#DICTIONARY}.
     * @param input the input to read from
     * @return the value
     * @throws IOException if I/O error occurs
     */
    default Map<String, Value> decodeDictionary(ReadInput input) throws IOException {
        return decodeDictionary(input, Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Decodes {@link PackStreamType#STRUCTURE} to the given type. A {@link PackStreamStructureDecoder} MUST be registered that support the given type.
     * @param input the input to read from
     * @param structureCls the type representing the structure
     * @return the structure value
     * @param <T> the structure type
     * @throws IOException if I/O error occurs
     */
    <T extends PackStreamStructure> T decodeStructure(ReadInput input, Class<T> structureCls) throws IOException;

    /**
     * Decodes {@link StructureDescriptor}.
     * @param input the input to read from
     * @return the structure descriptor
     * @throws IOException if I/O error occurs
     */
    StructureDescriptor decodeStructureDescriptor(ReadInput input) throws IOException;

    /**
     * A descriptor of {@link PackStreamType#STRUCTURE}.
     * @param size the structure size
     * @param tagByte the structure tag byte
     */
    record StructureDescriptor(long size, byte tagByte) {}
}
