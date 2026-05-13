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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.neo4j.bolt.connection.codec.ReadInput;
import org.neo4j.bolt.connection.codec.impl.PackStream;
import org.neo4j.bolt.connection.codec.impl.network.ReadInputAdapter;
import org.neo4j.bolt.connection.codec.packstream.PackStreamDecoder;
import org.neo4j.bolt.connection.codec.packstream.PackStreamType;
import org.neo4j.bolt.connection.codec.packstream.struct.PackStreamStructure;
import org.neo4j.bolt.connection.codec.packstream.struct.PackStreamStructureDecoder;
import org.neo4j.bolt.connection.exception.BoltClientException;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.bolt.connection.values.ValueFactory;

final class PackStreamDecoderImpl implements PackStreamDecoder {
    private final ValueFactory valueFactory;
    private final Map<Byte, PackStreamStructureDecoder<?>> tagByteToStructureDecoder;

    PackStreamDecoderImpl(ValueFactory valueFactory, Set<PackStreamStructureDecoder<?>> structureDecoders) {
        this.valueFactory = Objects.requireNonNull(valueFactory);
        this.tagByteToStructureDecoder = structureDecoders.stream()
                .collect(Collectors.toUnmodifiableMap(
                        PackStreamStructureDecoder::tagByte, Function.identity(), (a, b) -> b));
    }

    @Override
    public PackStreamType peekNextType(ReadInput input) throws IOException {
        return unpacker(input).peekNextType();
    }

    @Override
    public void decodeNull(ReadInput input) throws IOException {
        unpacker(input).unpackNull();
    }

    @Override
    public boolean decodeBoolean(ReadInput input) throws IOException {
        return unpacker(input).unpackBoolean();
    }

    @Override
    public long decodeInteger(ReadInput input) throws IOException {
        return unpacker(input).unpackLong();
    }

    @Override
    public double decodeFloat(ReadInput input) throws IOException {
        return unpacker(input).unpackDouble();
    }

    @Override
    public byte[] decodeBytes(ReadInput input) throws IOException {
        return unpacker(input).unpackBytes();
    }

    @Override
    public String decodeString(ReadInput input) throws IOException {
        return unpacker(input).unpackString();
    }

    @Override
    public UUID decodeUuid(ReadInput input) throws IOException {
        return unpacker(input).unpackUUID();
    }

    @Override
    public Value decodeValue(ReadInput input) throws IOException {
        var unpacker = unpacker(input);
        var type = unpacker.peekNextType();
        return switch (type) {
            case NULL -> valueFactory.value((Object) null);
            case BOOLEAN -> valueFactory.value(unpacker.unpackBoolean());
            case INTEGER -> valueFactory.value(unpacker.unpackLong());
            case FLOAT -> valueFactory.value(unpacker.unpackDouble());
            case BYTES -> valueFactory.value(unpacker.unpackBytes());
            case STRING -> valueFactory.value(unpacker.unpackString());
            case UUID -> valueFactory.value(unpacker.unpackUUID());
            case DICTIONARY -> valueFactory.value(decodeDictionary(input));
            case LIST -> valueFactory.value(decodeList(input));
            case STRUCTURE -> {
                var size = unpacker.unpackStructHeader();
                var tagByte = unpacker.unpackStructSignature();
                var decoder = tagByteToStructureDecoder.get(tagByte);
                if (decoder == null) {
                    throw new BoltClientException("No decoder found for tag byte " + tagByte);
                }
                yield decoder.decodeValue(this, input);
            }
        };
    }

    @Override
    public List<Value> decodeList(ReadInput input) throws IOException {
        var unpacker = unpacker(input);
        var size = (int) unpacker.unpackListHeader();
        var values = new Value[size];
        for (var i = 0; i < size; i++) {
            values[i] = decodeValue(input);
        }
        return Arrays.asList(values);
    }

    @Override
    public <R extends Map<String, Value>, A> R decodeDictionary(
            ReadInput input, Collector<Map.Entry<String, Value>, A, R> collector) throws IOException {
        var accumulator = collector.supplier().get();

        var unpacker = unpacker(input);
        var size = (int) unpacker.unpackMapHeader();

        for (var i = 0; i < size; i++) {
            var entry = Map.entry(unpacker.unpackString(), decodeValue(input));

            collector.accumulator().accept(accumulator, entry);
        }

        return collector.finisher().apply(accumulator);
    }

    @Override
    public StructureDescriptor decodeStructureDescriptor(ReadInput input) throws IOException {
        var unpacker = unpacker(input);
        var size = unpacker.unpackStructHeader();
        var tagByte = unpacker.unpackStructSignature();
        return new StructureDescriptor(size, tagByte);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends PackStreamStructure> T decodeStructure(ReadInput input, Class<T> structureCls)
            throws IOException {
        var unpacker = unpacker(input);
        var size = unpacker.unpackStructHeader();
        var tagByte = unpacker.unpackStructSignature();
        var decoder = tagByteToStructureDecoder.get(tagByte);
        if (decoder == null) {
            throw new BoltClientException("No decoder found for tag byte " + tagByte);
        }
        return (T) decoder.decode(this, input);
    }

    private PackStream.Unpacker unpacker(ReadInput input) {
        return new PackStream.Unpacker(new ReadInputAdapter(input));
    }
}
