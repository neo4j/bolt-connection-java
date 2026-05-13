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
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.neo4j.bolt.connection.codec.ReadInput;
import org.neo4j.bolt.connection.codec.packstream.PackStreamDecoder;
import org.neo4j.bolt.connection.exception.BoltClientException;
import org.neo4j.bolt.connection.values.Value;

/**
 * A {@link PackStreamStructureDecoder} for {@link EncryptedStructure}.
 * @since 12.1.0
 */
public final class EncryptedStructureDecoder implements PackStreamStructureDecoder<EncryptedStructure> {
    private static final EncryptedStructureDecoder INSTANCE = new EncryptedStructureDecoder();

    /**
     * Returns an instance of {@link EncryptedStructureDecoder}.
     * @return the decoder instance
     */
    public static EncryptedStructureDecoder getInstance() {
        return INSTANCE;
    }

    private EncryptedStructureDecoder() {}

    @Override
    public byte tagByte() {
        return EncryptedStructure.ENCRYPTED;
    }

    @Override
    public EncryptedStructure decode(PackStreamDecoder decoder, ReadInput input) throws IOException {
        var profileName = decoder.decodeString(input);
        var cipherOutput = decoder.decodeBytes(input);
        var typeName = decoder.decodeString(input);
        var typeSerializationSchemeMajor = decoder.decodeInteger(input);
        var typeSerializationSchemeMinor = decoder.decodeInteger(input);
        var metadata = decoder.decodeDictionary(
                input,
                Collectors.collectingAndThen(
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> b, TreeMap::new),
                        Collections::unmodifiableSortedMap));
        return new EncryptedStructure(
                profileName,
                cipherOutput,
                typeName,
                typeSerializationSchemeMajor,
                typeSerializationSchemeMinor,
                metadata);
    }

    @Override
    public Value decodeValue(PackStreamDecoder decoder, ReadInput input) {
        throw new BoltClientException("%s is not coercible to %s"
                .formatted(EncryptedStructure.class.getSimpleName(), Value.class.getSimpleName()));
    }
}
