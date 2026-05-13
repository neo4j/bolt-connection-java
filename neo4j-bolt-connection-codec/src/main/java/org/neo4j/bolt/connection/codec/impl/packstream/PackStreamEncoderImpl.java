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
package org.neo4j.bolt.connection.codec.impl.packstream;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.neo4j.bolt.connection.codec.WriteOutput;
import org.neo4j.bolt.connection.codec.impl.PackStream;
import org.neo4j.bolt.connection.codec.impl.network.WriteOutputAdapter;
import org.neo4j.bolt.connection.codec.packstream.PackStreamEncoder;
import org.neo4j.bolt.connection.codec.packstream.struct.PackStreamStructure;
import org.neo4j.bolt.connection.codec.packstream.struct.PackStreamStructureEncoder;
import org.neo4j.bolt.connection.exception.BoltClientException;
import org.neo4j.bolt.connection.values.Value;

final class PackStreamEncoderImpl implements PackStreamEncoder {
    private final Map<Byte, PackStreamStructureEncoder<?>> tagByteToStructureEncoder;

    PackStreamEncoderImpl(Set<PackStreamStructureEncoder<?>> structureEncoders) {
        this.tagByteToStructureEncoder = structureEncoders.stream()
                .collect(Collectors.toUnmodifiableMap(
                        PackStreamStructureEncoder::tagByte, Function.identity(), (a, b) -> b));
    }

    @Override
    public void encodeNull(WriteOutput<?> output) throws IOException {
        packer(output).packNull();
    }

    @Override
    public void encode(boolean value, WriteOutput<?> output) throws IOException {
        packer(output).pack(value);
    }

    @Override
    public void encode(long value, WriteOutput<?> output) throws IOException {
        packer(output).pack(value);
    }

    @Override
    public void encode(double value, WriteOutput<?> output) throws IOException {
        packer(output).pack(value);
    }

    @Override
    public void encode(byte[] bytes, WriteOutput<?> output) throws IOException {
        packer(output).pack(bytes);
    }

    @Override
    public void encode(String value, WriteOutput<?> output) throws IOException {
        packer(output).pack(value);
    }

    @Override
    public void encode(UUID value, WriteOutput<?> output) throws IOException {
        packer(output).pack(value);
    }

    @Override
    public void encode(Value value, WriteOutput<?> output) throws IOException {
        switch (value.boltValueType()) {
            case BOOLEAN -> encode(value.asBoolean(), output);
            case BYTES -> encode(value.asByteArray(), output);
            case STRING -> encode(value.asString(), output);
            case INTEGER -> encode(value.asLong(), output);
            case FLOAT -> encode(value.asDouble(), output);
            case LIST ->
                encode(
                        StreamSupport.stream(value.boltValues().spliterator(), false)
                                .collect(Collectors.toList()),
                        output);
            case NULL -> encodeNull(output);
            case UUID -> encode(value.asUUID(), output);
            default -> throw new BoltClientException("Unsupported value type " + value.boltValueType());
        }
    }

    @Override
    public void encode(List<Value> values, WriteOutput<?> output) throws IOException {
        if (values == null) {
            encodeNull(output);
        } else {
            packer(output).packListHeader(values.size());
            for (var value : values) {
                encode(value, output);
            }
        }
    }

    @Override
    public void encode(Map<String, Value> values, WriteOutput<?> output) throws IOException {
        if (values == null) {
            encodeNull(output);
        } else {
            packer(output).packMapHeader(values.size());
            for (var key : values.keySet()) {
                encode(key, output);
                encode(values.get(key), output);
            }
        }
    }

    @Override
    public void encodeStructureHeader(int size, byte tagByte, WriteOutput<?> output) throws IOException {
        var packer = packer(output);
        packer.packStructHeader(size, tagByte);
    }

    @Override
    public <T extends PackStreamStructure> void encode(T structure, WriteOutput<?> output) throws IOException {
        if (structure == null) {
            encodeNull(output);
        } else {
            var tagByte = structure.tagByte();
            @SuppressWarnings("unchecked")
            var encoder = (PackStreamStructureEncoder<T>) tagByteToStructureEncoder.get(tagByte);
            if (encoder == null) {
                throw new BoltClientException("No encoder for tag byte " + tagByte);
            }
            packer(output).packStructHeader(structure.size(), structure.tagByte());
            encoder.encode(structure, this, output);
        }
    }

    private PackStream.Packer packer(WriteOutput<?> output) {
        return new PackStream.Packer(new WriteOutputAdapter(output));
    }
}
