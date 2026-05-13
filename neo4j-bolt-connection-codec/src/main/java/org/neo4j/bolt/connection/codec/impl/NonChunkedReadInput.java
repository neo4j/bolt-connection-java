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
package org.neo4j.bolt.connection.codec.impl;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Objects;
import org.neo4j.bolt.connection.codec.ReadInput;

public final class NonChunkedReadInput implements ReadInput {

    private final ByteArrayInputStream buffer;
    private final DataInputStream in;

    public NonChunkedReadInput(byte[] data) {
        this(new ByteArrayInputStream(data));
    }

    public NonChunkedReadInput(ByteArrayInputStream buffer) {
        this.buffer = Objects.requireNonNull(buffer);
        this.in = new DataInputStream(buffer);
    }

    @Override
    public byte readByte() throws IOException {
        return in.readByte();
    }

    @Override
    public short readShort() throws IOException {
        return in.readShort();
    }

    @Override
    public int readInt() throws IOException {
        return in.readInt();
    }

    @Override
    public long readLong() throws IOException {
        return in.readLong();
    }

    @Override
    public double readDouble() throws IOException {
        return in.readDouble();
    }

    @Override
    public float readFloat() throws IOException {
        return in.readFloat();
    }

    @Override
    public void readBytes(byte[] into, int offset, int toRead) throws IOException {
        in.readFully(into, offset, toRead);
    }

    @Override
    public byte peekByte() throws IOException {
        buffer.mark(1);

        var value = in.read();

        if (value < 0) {
            throw new IOException("End of stream");
        }

        buffer.reset();

        return (byte) value;
    }

    public int available() {
        return buffer.available();
    }
}
