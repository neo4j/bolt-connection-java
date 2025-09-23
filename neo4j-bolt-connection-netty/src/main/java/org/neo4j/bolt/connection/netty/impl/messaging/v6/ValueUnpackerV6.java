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
package org.neo4j.bolt.connection.netty.impl.messaging.v6;

import java.io.IOException;
import java.lang.reflect.Array;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.exception.BoltProtocolException;
import org.neo4j.bolt.connection.netty.impl.messaging.v5.ValueUnpackerV5;
import org.neo4j.bolt.connection.netty.impl.packstream.PackInput;
import org.neo4j.bolt.connection.netty.impl.packstream.PackStream;
import org.neo4j.bolt.connection.values.Type;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.bolt.connection.values.ValueFactory;

final class ValueUnpackerV6 extends ValueUnpackerV5 {
    public ValueUnpackerV6(PackInput input, ValueFactory valueFactory) {
        super(input, valueFactory);
    }

    @Override
    protected Value unpackVector(long size) throws IOException {
        ensureCorrectStructSize(Type.VECTOR, VECTOR_STRUCT_SIZE, size);
        var elementTypeBytes = unpacker.unpackBytes();
        if (elementTypeBytes.length != 1) {
            throw new BoltProtocolException("Expected 1 element type, but got " + elementTypeBytes.length);
        }
        var elementTypeAsByte = elementTypeBytes[0];

        Class<?> elementType;
        int sizeDivider;
        RawUnpacker rawUnpacker;
        switch (elementTypeAsByte) {
            case PackStream.INT_64 -> {
                elementType = long.class;
                sizeDivider = Long.BYTES;
                rawUnpacker = unpacker::unpackRawLong;
            }
            case PackStream.INT_32 -> {
                elementType = int.class;
                sizeDivider = Integer.BYTES;
                rawUnpacker = unpacker::unpackRawInt;
            }
            case PackStream.INT_16 -> {
                elementType = short.class;
                sizeDivider = Short.BYTES;
                rawUnpacker = unpacker::unpackRawShort;
            }
            case PackStream.INT_8 -> {
                elementType = byte.class;
                sizeDivider = Byte.BYTES;
                rawUnpacker = unpacker::unpackRawByte;
            }
            case PackStream.FLOAT_64 -> {
                elementType = double.class;
                sizeDivider = Double.BYTES;
                rawUnpacker = unpacker::unpackRawDouble;
            }
            case PackStream.FLOAT_32 -> {
                elementType = float.class;
                sizeDivider = Float.BYTES;
                rawUnpacker = unpacker::unpackRawFloat;
            }
            default -> throw new BoltProtocolException("Unexpected element type " + elementTypeAsByte);
        }

        var lenght = unpacker.unpackBytesSize() / sizeDivider;

        var array = Array.newInstance(elementType, lenght);
        for (var i = 0; i < lenght; i++) {
            Array.set(array, i, rawUnpacker.unpackRaw());
        }

        return valueFactory.vector(elementType, array);
    }

    @Override
    protected Value unpackUnsupported(long size) throws IOException {
        ensureCorrectStructSize(Type.UNSUPPORTED, UNSUPPORTED_STRUCT_SIZE, size);
        var name = unpacker.unpackString();
        var minMajorBoltVersion = unpacker.unpackLong();
        var minMinorBoltVersion = unpacker.unpackLong();
        var minProtocolVersion =
                new BoltProtocolVersion(Math.toIntExact(minMajorBoltVersion), Math.toIntExact(minMinorBoltVersion));
        var extra = unpackMap();
        return valueFactory.unsupportedType(name, minProtocolVersion, extra);
    }

    private interface RawUnpacker {
        Object unpackRaw() throws IOException;
    }
}
