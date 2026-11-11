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
package org.neo4j.bolt.connection.netty.impl.async.outbound;

import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.neo4j.bolt.connection.codec.WriteOutput;
import org.neo4j.bolt.connection.netty.impl.async.connection.BoltProtocolUtil;

import static java.util.Objects.requireNonNull;

/**
 * Writes a message as Bolt chunks into a sequence of buffers. A new buffer is started at the first chunk boundary
 * after the current chunk reaches the maximum size.
 */
public class ChunkAwareByteBufOutput implements WriteOutput<Void> {
    private final int maxChunkSize;
    private final List<ByteBuf> buffers = new ArrayList<>();

    private ByteBufAllocator allocator;
    private ByteBuf buf;
    private int currentChunkStartIndex;
    private int currentChunkSize;

    public ChunkAwareByteBufOutput() {
        this(BoltProtocolUtil.DEFAULT_MAX_OUTBOUND_CHUNK_SIZE_BYTES);
    }

    ChunkAwareByteBufOutput(int maxChunkSize) {
        this.maxChunkSize = verifyPositive("Max chunk size", maxChunkSize);
    }

    public void start(ByteBufAllocator allocator) {
        assertNotStarted();
        this.allocator = requireNonNull(allocator);
        buf = allocator.ioBuffer();
        startNewChunk(0);
    }

    /**
     * Completes the message and transfers ownership of its buffers, in write order, to the caller.
     */
    public List<ByteBuf> stop() {
        writeChunkSizeHeader();
        var result = new ArrayList<ByteBuf>(buffers.size() + 1);
        result.addAll(buffers);
        result.add(buf);
        reset();
        return result;
    }

    public void abort() {
        try {
            buffers.forEach(ByteBuf::release);
            buf.release();
        } finally {
            reset();
        }
    }

    @Override
    public void writeByte(byte value) {
        ensureCanFitInCurrentChunk(1);
        buf.writeByte(value);
        currentChunkSize += 1;
    }

    @Override
    public void writeBytes(byte[] data) {
        var offset = 0;
        var length = data.length;
        while (offset < length) {
            // Ensure there is an open chunk, and that it has at least one byte of space left
            ensureCanFitInCurrentChunk(1);

            // Write as much as we can into the current chunk
            var amountToWrite = Math.min(availableBytesInCurrentChunk(), length - offset);

            buf.writeBytes(data, offset, amountToWrite);
            currentChunkSize += amountToWrite;
            offset += amountToWrite;
        }
    }

    @Override
    public void writeShort(short value) {
        ensureCanFitInCurrentChunk(2);
        buf.writeShort(value);
        currentChunkSize += 2;
    }

    @Override
    public void writeInt(int value) {
        ensureCanFitInCurrentChunk(4);
        buf.writeInt(value);
        currentChunkSize += 4;
    }

    @Override
    public void writeLong(long value) {
        ensureCanFitInCurrentChunk(8);
        buf.writeLong(value);
        currentChunkSize += 8;
    }

    @Override
    public void writeDouble(double value) {
        ensureCanFitInCurrentChunk(8);
        buf.writeDouble(value);
        currentChunkSize += 8;
    }

    @Override
    public void writeFloat(float value) {
        ensureCanFitInCurrentChunk(4);
        buf.writeFloat(value);
        currentChunkSize += 4;
    }

    @Override
    public Void output() {
        return null;
    }

    private void ensureCanFitInCurrentChunk(int numberOfBytes) {
        var targetChunkSize = currentChunkSize + numberOfBytes;
        if (targetChunkSize > maxChunkSize) {
            writeChunkSizeHeader();
            startNewBuffer();
            startNewChunk(buf.writerIndex());
        }
    }

    private void startNewBuffer() {
        var next = allocator.ioBuffer(BoltProtocolUtil.DEFAULT_MAX_OUTBOUND_CHUNK_SIZE_BYTES);
        buffers.add(buf);
        buf = next;
    }

    private void reset() {
        buffers.clear();
        allocator = null;
        buf = null;
        currentChunkStartIndex = 0;
        currentChunkSize = 0;
    }

    private void startNewChunk(int index) {
        currentChunkStartIndex = index;
        BoltProtocolUtil.writeEmptyChunkHeader(buf);
        currentChunkSize = BoltProtocolUtil.CHUNK_HEADER_SIZE_BYTES;
    }

    private void writeChunkSizeHeader() {
        // go to the beginning of the chunk and write the size header
        var chunkBodySize = currentChunkSize - BoltProtocolUtil.CHUNK_HEADER_SIZE_BYTES;
        BoltProtocolUtil.writeChunkHeader(buf, currentChunkStartIndex, chunkBodySize);
    }

    private int availableBytesInCurrentChunk() {
        return maxChunkSize - currentChunkSize;
    }

    private void assertNotStarted() {
        if (buf != null) {
            throw new IllegalStateException("Already started");
        }
    }

    private static int verifyPositive(String name, int value) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " should be > 0, given: " + value);
        }
        return value;
    }
}
