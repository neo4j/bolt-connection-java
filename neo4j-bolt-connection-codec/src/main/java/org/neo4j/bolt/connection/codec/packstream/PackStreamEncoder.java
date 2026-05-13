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
import org.neo4j.bolt.connection.codec.packstream.struct.PackStreamStructureEncoder;
import org.neo4j.bolt.connection.values.Value;

/**
 * An encoder implementing PackStream.
 * @since 12.1.0
 */
public interface PackStreamEncoder {
    /**
     * Encodes to {@link PackStreamType#NULL}.
     * @param output the output to write to
     * @throws IOException if I/O error occurs
     */
    void encodeNull(WriteOutput<?> output) throws IOException;

    /**
     * Encodes to {@link PackStreamType#BOOLEAN}.
     * @param value the value to encode
     * @param output the output to write to
     * @throws IOException if I/O error occurs
     */
    void encode(boolean value, WriteOutput<?> output) throws IOException;

    /**
     * Encodes to {@link PackStreamType#INTEGER}.
     * @param value the value to encode
     * @param output the output to write to
     * @throws IOException if I/O error occurs
     */
    void encode(long value, WriteOutput<?> output) throws IOException;

    /**
     * Encodes to {@link PackStreamType#FLOAT}.
     * @param value the value to encode
     * @param output the output to write to
     * @throws IOException if I/O error occurs
     */
    void encode(double value, WriteOutput<?> output) throws IOException;

    /**
     * Encodes to {@link PackStreamType#BYTES}.
     * @param bytes the value to encode
     * @param output the output to write to
     * @throws IOException if I/O error occurs
     */
    void encode(byte[] bytes, WriteOutput<?> output) throws IOException;

    /**
     * Encodes to {@link PackStreamType#STRING}.
     * @param value the value to encode
     * @param output the output to write to
     * @throws IOException if I/O error occurs
     */
    void encode(String value, WriteOutput<?> output) throws IOException;

    /**
     * Encodes to {@link PackStreamType#UUID}.
     * @param value the value to encode
     * @param output the output to write to
     * @throws IOException if I/O error occurs
     */
    void encode(UUID value, WriteOutput<?> output) throws IOException;

    /**
     * Encodes the given {@link Value} to the respective PackStream value.
     * @param value the value to encode
     * @param output the output to write to
     * @throws IOException if I/O error occurs
     */
    void encode(Value value, WriteOutput<?> output) throws IOException;

    /**
     * Encodes to {@link PackStreamType#LIST}.
     * @param values the values to encode
     * @param output the output to write to
     * @throws IOException if I/O error occurs
     */
    void encode(List<Value> values, WriteOutput<?> output) throws IOException;

    /**
     * Encodes to {@link PackStreamType#DICTIONARY}.
     * @param values the value to encode
     * @param output the output to write to
     * @throws IOException if I/O error occurs
     */
    void encode(Map<String, Value> values, WriteOutput<?> output) throws IOException;

    /**
     * Encodes a {@link  PackStreamType#STRUCTURE} header.
     * @param size the structure size
     * @param tagByte the structure tag byte
     * @param output  the output to write to
     * @throws IOException if I/O error occurs
     */
    void encodeStructureHeader(int size, byte tagByte, WriteOutput<?> output) throws IOException;

    /**
     * Encodes the given {@link PackStreamStructure}. A {@link PackStreamStructureEncoder} MUST be registered that support the given type.
     * @param structure the structure to encode
     * @param output  the output to write to
     * @param <T> the structure type
     * @throws IOException if I/O error occurs
     */
    <T extends PackStreamStructure> void encode(T structure, WriteOutput<?> output) throws IOException;
}
