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
package org.neo4j.bolt.connection.codec.impl.value_encoding;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;
import org.neo4j.bolt.connection.codec.impl.PackOutput;

final class NonChunkedPackOutput implements PackOutput {
    private final ByteArrayOutputStream byteStream;
    private final DataOutputStream out;

    public NonChunkedPackOutput() {
        this(new ByteArrayOutputStream());
    }

    public NonChunkedPackOutput(ByteArrayOutputStream byteStream) {
        this.byteStream = Objects.requireNonNull(byteStream);
        this.out = new DataOutputStream(byteStream);
    }

    @Override
    public PackOutput writeByte(byte value) throws IOException {
        out.writeByte(value);
        return this;
    }

    @Override
    public PackOutput writeBytes(byte[] data) throws IOException {
        out.write(data);
        return this;
    }

    @Override
    public PackOutput writeShort(short value) throws IOException {
        out.writeShort(value);
        return this;
    }

    @Override
    public PackOutput writeInt(int value) throws IOException {
        out.writeInt(value);
        return this;
    }

    @Override
    public PackOutput writeLong(long value) throws IOException {
        out.writeLong(value);
        return this;
    }

    @Override
    public PackOutput writeDouble(double value) throws IOException {
        out.writeDouble(value);
        return this;
    }

    @Override
    public PackOutput writeFloat(float value) throws IOException {
        out.writeFloat(value);
        return this;
    }

    public byte[] toByteArray() {
        return byteStream.toByteArray();
    }

    public int size() {
        return byteStream.size();
    }

    public void reset() {
        byteStream.reset();
    }
}
