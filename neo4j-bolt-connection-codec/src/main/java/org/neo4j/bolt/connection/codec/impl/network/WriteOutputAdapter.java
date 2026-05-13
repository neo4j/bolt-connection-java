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
import org.neo4j.bolt.connection.codec.WriteOutput;
import org.neo4j.bolt.connection.codec.impl.PackOutput;

public final class WriteOutputAdapter implements PackOutput {
    private final WriteOutput<?> output;

    public WriteOutputAdapter(WriteOutput<?> output) {
        this.output = Objects.requireNonNull(output);
    }

    @Override
    public PackOutput writeByte(byte value) throws IOException {
        output.writeByte(value);
        return this;
    }

    @Override
    public PackOutput writeBytes(byte[] data) throws IOException {
        output.writeBytes(data);
        return this;
    }

    @Override
    public PackOutput writeShort(short value) throws IOException {
        output.writeShort(value);
        return this;
    }

    @Override
    public PackOutput writeInt(int value) throws IOException {
        output.writeInt(value);
        return this;
    }

    @Override
    public PackOutput writeLong(long value) throws IOException {
        output.writeLong(value);
        return this;
    }

    @Override
    public PackOutput writeDouble(double value) throws IOException {
        output.writeDouble(value);
        return this;
    }

    @Override
    public PackOutput writeFloat(float value) throws IOException {
        output.writeFloat(value);
        return this;
    }
}
