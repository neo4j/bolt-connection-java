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
import org.neo4j.bolt.connection.codec.ReadInput;
import org.neo4j.bolt.connection.codec.packstream.PackStreamDecoder;
import org.neo4j.bolt.connection.values.Type;
import org.neo4j.bolt.connection.values.Value;

/**
 * A decoder of {@link PackStreamStructure}.
 * @param <T> the structure type
 * @since 12.1.0
 */
public interface PackStreamStructureDecoder<T extends PackStreamStructure> {
    /**
     * Returns the tag byte of the {@link PackStreamStructure}.
     * @return the tag byte
     */
    byte tagByte();

    /**
     * Decodes the {@link PackStreamStructure}.
     * @param decoder the decoder to decode the structure fields with
     * @param input the input to read from
     * @return the decoded structure
     * @throws IOException if I/O error occurs
     */
    T decode(PackStreamDecoder decoder, ReadInput input) throws IOException;

    /**
     * Decodes the structure to {@link Value} if there is a supported {@link Type}.
     * @param decoder the decoder to decode the structure fields with
     * @param input the input to read from
     * @return the decoded structure
     * @throws IOException if I/O error occurs
     */
    Value decodeValue(PackStreamDecoder decoder, ReadInput input) throws IOException;
}
