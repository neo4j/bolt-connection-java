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
package org.neo4j.bolt.connection.codec.impl.network;

import java.io.IOException;
import java.util.Objects;
import org.neo4j.bolt.connection.codec.ReadInput;
import org.neo4j.bolt.connection.codec.impl.PackInput;

public final class ReadInputAdapter implements PackInput {
    private final ReadInput input;

    public ReadInputAdapter(ReadInput input) {
        this.input = Objects.requireNonNull(input);
    }

    @Override
    public byte readByte() throws IOException {
        return input.readByte();
    }

    @Override
    public short readShort() throws IOException {
        return input.readShort();
    }

    @Override
    public int readInt() throws IOException {
        return input.readInt();
    }

    @Override
    public long readLong() throws IOException {
        return input.readLong();
    }

    @Override
    public double readDouble() throws IOException {
        return input.readDouble();
    }

    @Override
    public float readFloat() throws IOException {
        return input.readFloat();
    }

    @Override
    public void readBytes(byte[] into, int offset, int toRead) throws IOException {
        input.readBytes(into, offset, toRead);
    }

    @Override
    public byte peekByte() throws IOException {
        return input.peekByte();
    }
}
