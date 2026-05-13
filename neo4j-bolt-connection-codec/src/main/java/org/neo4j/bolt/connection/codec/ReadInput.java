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
 * Read input used for decoding purposes.
 * @see ReadInputs
 * @since 12.1.0
 */
public interface ReadInput {
    /**
     * Reads next byte.
     * @return next byte
     * @throws IOException when I/O failure occurs
     */
    byte readByte() throws IOException;

    /**
     * Reads next short.
     * @return next short
     * @throws IOException when I/O failure occurs
     */
    short readShort() throws IOException;

    /**
     * Reads next int.
     * @return next int
     * @throws IOException when I/O failure occurs
     */
    int readInt() throws IOException;

    /**
     * Reads next long.
     * @return next long
     * @throws IOException when I/O failure occurs
     */
    long readLong() throws IOException;

    /**
     * Reads next double.
     * @return next double
     * @throws IOException when I/O failure occurs
     */
    double readDouble() throws IOException;

    /**
     * Reads next float.
     * @return next float
     * @throws IOException when I/O failure occurs
     */
    float readFloat() throws IOException;

    /**
     * Reads next bytes into the provided array.
     * @param into destination array
     * @param offset offset
     * @param toRead number of bytes to read
     * @throws IOException when I/O failure occurs
     */
    void readBytes(byte[] into, int offset, int toRead) throws IOException;

    /**
     * Reads next byte, but does not move the pointer.
     * @return next byte
     * @throws IOException when I/O failure occurs
     */
    byte peekByte() throws IOException;
}
