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
 * A {@link PackStreamStructureEncoder} for {@link EncryptedStructure}.
 * @since 12.1.0
 */
public final class EncryptedStructureEncoder implements PackStreamStructureEncoder<EncryptedStructure> {
    private static final EncryptedStructureEncoder INSTANCE = new EncryptedStructureEncoder();

    /**
     * Returns an instance of {@link EncryptedStructureEncoder}.
     * @return the encoder instance
     */
    public static EncryptedStructureEncoder getInstance() {
        return INSTANCE;
    }

    private EncryptedStructureEncoder() {}

    @Override
    public byte tagByte() {
        return EncryptedStructure.ENCRYPTED;
    }

    @Override
    public void encode(EncryptedStructure structure, PackStreamEncoder encoder, WriteOutput<?> output)
            throws IOException {
        encoder.encode(structure.profileName(), output);
        encoder.encode(structure.cipherOutput(), output);
        encoder.encode(structure.typeName(), output);
        encoder.encode(structure.typeEncodingSchemeMajor(), output);
        encoder.encode(structure.typeEncodingSchemeMinor(), output);
        encoder.encode(structure.metadata(), output);
    }
}
