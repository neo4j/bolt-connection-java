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
package org.neo4j.bolt.connection.codec;

import java.io.IOException;

/**
 * Write output used for encoding purposes.
 * @see WriteOutputs
 * @param <T> underlying output format if it is surfaced by a given implementation
 * @since 12.1.0
 */
public interface WriteOutput<T> {
    /**
     * Writes byte.
     * @param value the value to write
     * @throws IOException when I/O failure occurs
     */
    void writeByte(byte value) throws IOException;

    /**
     * Writes bytes.
     * @param data the values to write
     * @throws IOException when I/O failure occurs
     */
    void writeBytes(byte[] data) throws IOException;

    /**
     * Writes short.
     * @param value the value to write
     * @throws IOException when I/O failure occurs
     */
    void writeShort(short value) throws IOException;

    /**
     * Writes int.
     * @param value the value to write
     * @throws IOException when I/O failure occurs
     */
    void writeInt(int value) throws IOException;

    /**
     * Writes long.
     * @param value the value to write
     * @throws IOException when I/O failure occurs
     */
    void writeLong(long value) throws IOException;

    /**
     * Writes double.
     * @param value the value to write
     * @throws IOException when I/O failure occurs
     */
    void writeDouble(double value) throws IOException;

    /**
     * Writes float.
     * @param value the value to write
     * @throws IOException when I/O failure occurs
     */
    void writeFloat(float value) throws IOException;

    /**
     * Returns the underlying output format if surfaced by a given implementation.
     * @return the underlying output format
     * @throws UnsupportedOperationException if the underlying output format is not surfaced
     */
    T output();
}
