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
import org.neo4j.bolt.connection.netty.impl.messaging.common.CommonValuePacker;
import org.neo4j.bolt.connection.netty.impl.packstream.PackOutput;
import org.neo4j.bolt.connection.netty.impl.packstream.PackStream;
import org.neo4j.bolt.connection.values.Vector;

final class ValuePackerV6 extends CommonValuePacker {
    public ValuePackerV6(PackOutput output) {
        super(output, true);
    }

    @Override
    protected void packVector(Vector vector) throws IOException {
        var elementType = vector.elementType();
        var elements = vector.elements();
        var length = Array.getLength(elements);

        byte elementyTypeAsByte;
        int sizeMultiplier;
        RawPacker rawPacker;
        if (elementType.equals(long.class) || elementType.isAssignableFrom(Long.class)) {
            elementyTypeAsByte = PackStream.INT_64;
            sizeMultiplier = Long.BYTES;
            rawPacker = i -> packer.packRaw(Array.getLong(elements, i));
        } else if (elementType.equals(int.class) || elementType.equals(Integer.class)) {
            elementyTypeAsByte = PackStream.INT_32;
            sizeMultiplier = Integer.BYTES;
            rawPacker = i -> packer.packRaw(Array.getInt(elements, i));
        } else if (elementType.equals(double.class) || elementType.equals(Double.class)) {
            elementyTypeAsByte = PackStream.FLOAT_64;
            sizeMultiplier = Double.BYTES;
            rawPacker = i -> packer.packRaw(Array.getDouble(elements, i));
        } else if (elementType.equals(float.class) || elementType.equals(Float.class)) {
            elementyTypeAsByte = PackStream.FLOAT_32;
            sizeMultiplier = Float.BYTES;
            rawPacker = i -> packer.packRaw(Array.getFloat(elements, i));
        } else if (elementType.equals(short.class) || elementType.equals(Short.class)) {
            elementyTypeAsByte = PackStream.INT_16;
            sizeMultiplier = Short.BYTES;
            rawPacker = i -> packer.packRaw(Array.getShort(elements, i));
        } else if (elementType.equals(byte.class) || elementType.equals(Byte.class)) {
            elementyTypeAsByte = PackStream.INT_8;
            sizeMultiplier = Byte.BYTES;
            rawPacker = i -> packer.packRaw(Array.getByte(elements, i));
        } else {
            throw new IOException("Unsupported vector element type: " + elementType);
        }

        packStructHeader(VECTOR_STRUCT_SIZE, VECTOR);
        packer.pack(new byte[] {elementyTypeAsByte});
        packer.packBytesHeader(length * sizeMultiplier);
        for (var i = 0; i < length; i++) {
            rawPacker.packRaw(i);
        }
    }

    private interface RawPacker {
        void packRaw(int index) throws IOException;
    }
}
