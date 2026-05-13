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

import java.util.SortedMap;
import org.neo4j.bolt.connection.values.Value;

/**
 * An Encrypted Structure used to encapsulate encrypted values.
 * @param profileName the encryption profile name
 * @param cipherOutput the cipher output
 * @param typeName the type name
 * @param typeEncodingSchemeMajor the major version of the encoding scheme used to encode the value
 * @param typeEncodingSchemeMinor the minor version of the encoding scheme used to encode the value
 * @param metadata the metadata associated with the encrypted value
 * @since 12.1.0
 */
public record EncryptedStructure(
        String profileName,
        byte[] cipherOutput,
        String typeName,
        long typeEncodingSchemeMajor,
        long typeEncodingSchemeMinor,
        SortedMap<String, Value> metadata)
        implements PackStreamStructure {
    static final byte ENCRYPTED = 'e';
    static final int ENCRYPTED_STRUCT_SIZE = 6;

    @Override
    public byte tagByte() {
        return ENCRYPTED;
    }

    @Override
    public int size() {
        return ENCRYPTED_STRUCT_SIZE;
    }
}
