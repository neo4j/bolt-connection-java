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
package org.neo4j.bolt.connection.codec.packstream.struct;

import java.io.IOException;
import org.neo4j.bolt.connection.codec.WriteOutput;
import org.neo4j.bolt.connection.codec.packstream.PackStreamEncoder;

/**
 * An encoder of {@link PackStreamStructure}.
 * @param <T> the structure type
 * @since 12.1.0
 */
public interface PackStreamStructureEncoder<T extends PackStreamStructure> {
    /**
     * Returns the tag byte of the {@link PackStreamStructure}.
     * @return the tag byte
     */
    byte tagByte();

    /**
     * Encodes the given {@link PackStreamStructure} value.
     * @param structure the value to encode
     * @param encoder the encoder to encode structure fields with
     * @param output the output to write to
     * @throws IOException if I/O error occurs
     */
    void encode(T structure, PackStreamEncoder encoder, WriteOutput<?> output) throws IOException;
}
