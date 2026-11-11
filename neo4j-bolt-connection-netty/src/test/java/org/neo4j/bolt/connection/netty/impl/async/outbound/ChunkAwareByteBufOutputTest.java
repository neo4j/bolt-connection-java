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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.bolt.connection.netty.impl.util.TestUtil.assertByteBufContains;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.bolt.connection.netty.impl.async.connection.BoltProtocolUtil;

class ChunkAwareByteBufOutputTest {
    private static final UnpooledByteBufAllocator ALLOCATOR = UnpooledByteBufAllocator.DEFAULT;

    @Test
    void shouldThrowForIllegalMaxChunkSize() {
        assertThrows(IllegalArgumentException.class, () -> new ChunkAwareByteBufOutput(-42));
    }

    @Test
    void shouldThrowWhenStartedWithNullAllocator() {
        var output = new ChunkAwareByteBufOutput(16);
        assertThrows(NullPointerException.class, () -> output.start(null));
    }

    @Test
    void shouldThrowWhenStartedTwice() {
        var output = new ChunkAwareByteBufOutput(16);
        output.start(ALLOCATOR);

        assertThrows(IllegalStateException.class, () -> output.start(ALLOCATOR));
        output.abort();
    }

    @Test
    void shouldStartNewBufferWhenFirstChunkNearsMaxSize() {
        var output = new ChunkAwareByteBufOutput(7);

        output.start(ALLOCATOR);
        output.writeBytes(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        var buffers = output.stop();

        assertEquals(2, buffers.size());
        assertByteBufContains(
                buffers.get(0),
                (short) 5,
                (byte) 1,
                (byte) 2,
                (byte) 3,
                (byte) 4,
                (byte) 5);
        assertByteBufContains(
                buffers.get(1),
                (short) 5,
                (byte) 6,
                (byte) 7,
                (byte) 8,
                (byte) 9,
                (byte) 10);
    }

    @Test
    void shouldRequestMaxChunkSizeCapacityByDefault() {
        var output = new ChunkAwareByteBufOutput();

        output.start(ALLOCATOR);
        output.writeBytes(new byte[1024 * 1024]);
        var buffers = output.stop();

        try {
            assertTrue(buffers.size() > 1, "Expected more than one buffer, got " + buffers.size());
            var capacity = buffers.get(1).capacity();
            assertEquals(BoltProtocolUtil.DEFAULT_MAX_OUTBOUND_CHUNK_SIZE_BYTES, capacity);
        } finally {
            buffers.forEach(ByteBuf::release);
        }
    }

    @Test
    void shouldReleaseEachBufferOnceAndBeReusableWhenBufferAllocationFails() {
        var allocator = new RecordingByteBufAllocator(1);
        var output = new ChunkAwareByteBufOutput(7);

        output.start(allocator);
        assertThrows(OutOfMemoryError.class, () -> output.writeBytes(new byte[100]));
        output.abort();

        assertEquals(1, allocator.allocated().size());
        assertEquals(0, allocator.allocated().get(0).refCnt());

        output.start(ALLOCATOR);
        output.writeByte((byte) 42);
        assertByteBufContains(join(output.stop()), (short) 1, (byte) 42);
    }

    @Test
    void shouldReleaseAllBuffersOnAbortAndBeReusable() {
        var allocator = new RecordingByteBufAllocator();
        var output = new ChunkAwareByteBufOutput(7);

        output.start(allocator);
        output.writeBytes(new byte[100]);
        output.abort();

        assertTrue(allocator.allocated().size() > 1);
        allocator.allocated().forEach(buf -> assertEquals(0, buf.refCnt()));

        output.start(ALLOCATOR);
        output.writeByte((byte) 42);
        assertByteBufContains(join(output.stop()), (short) 1, (byte) 42);
    }

    private static ByteBuf join(List<ByteBuf> buffers) {
        return Unpooled.wrappedBuffer(buffers.toArray(ByteBuf[]::new));
    }

    @Test
    void shouldWriteByteAtTheBeginningOfChunk() {
        var output = new ChunkAwareByteBufOutput(16);

        output.start(ALLOCATOR);
        output.writeByte((byte) 42);
        var buf = join(output.stop());

        assertByteBufContains(buf, (short) 1, (byte) 42);
    }

    @Test
    void shouldWriteByteWhenCurrentChunkContainsSpace() {
        var output = new ChunkAwareByteBufOutput(16);

        output.start(ALLOCATOR);
        output.writeByte((byte) 1);
        output.writeByte((byte) 2);
        output.writeByte((byte) -24);

        output.writeByte((byte) 42);
        var buf = join(output.stop());

        assertByteBufContains(buf, (short) 4, (byte) 1, (byte) 2, (byte) -24, (byte) 42);
    }

    @Test
    void shouldWriteByteWhenCurrentChunkIsFull() {
        var output = new ChunkAwareByteBufOutput(5);

        output.start(ALLOCATOR);
        output.writeByte((byte) 5);
        output.writeByte((byte) 3);
        output.writeByte((byte) -5);

        output.writeByte((byte) 42);
        var buf = join(output.stop());

        assertByteBufContains(
                buf,
                (short) 3,
                (byte) 5,
                (byte) 3,
                (byte) -5, // chunk 1
                (short) 1,
                (byte) 42 // chunk 2
                );
    }

    @Test
    void shouldWriteShortAtTheBeginningOfChunk() {
        var output = new ChunkAwareByteBufOutput(10);

        output.start(ALLOCATOR);
        output.writeShort(Short.MAX_VALUE);
        var buf = join(output.stop());

        assertByteBufContains(buf, (short) 2, Short.MAX_VALUE);
    }

    @Test
    void shouldWriteShortWhenCurrentChunkContainsSpace() {
        var output = new ChunkAwareByteBufOutput(12);

        output.start(ALLOCATOR);
        output.writeShort((short) 1);
        output.writeShort((short) 42);
        output.writeShort((short) 4242);
        output.writeShort((short) 4242);

        output.writeShort((short) -30);
        var buf = join(output.stop());

        assertByteBufContains(buf, (short) 10, (short) 1, (short) 42, (short) 4242, (short) 4242, (short) -30);
    }

    @Test
    void shouldWriteShortWhenCurrentChunkIsFull() {
        var output = new ChunkAwareByteBufOutput(8);

        output.start(ALLOCATOR);
        output.writeShort((short) 14);
        output.writeShort((short) -99);
        output.writeShort((short) 202);

        output.writeShort(Short.MIN_VALUE);
        var buf = join(output.stop());

        assertByteBufContains(
                buf,
                (short) 6,
                (short) 14,
                (short) -99,
                (short) 202, // chunk 1
                (short) 2,
                Short.MIN_VALUE // chunk 2
                );
    }

    @Test
    void shouldWriteIntAtTheBeginningOfChunk() {
        var output = new ChunkAwareByteBufOutput(18);

        output.start(ALLOCATOR);
        output.writeInt(73649);
        var buf = join(output.stop());

        assertByteBufContains(buf, (short) 4, 73649);
    }

    @Test
    void shouldWriteIntWhenCurrentChunkContainsSpace() {
        var output = new ChunkAwareByteBufOutput(40);

        output.start(ALLOCATOR);
        output.writeInt(Integer.MAX_VALUE);
        output.writeInt(20);
        output.writeInt(-173);

        output.writeInt(Integer.MIN_VALUE);
        var buf = join(output.stop());

        assertByteBufContains(buf, (short) 16, Integer.MAX_VALUE, 20, -173, Integer.MIN_VALUE);
    }

    @Test
    void shouldWriteIntWhenCurrentChunkIsFull() {
        var output = new ChunkAwareByteBufOutput(27);

        output.start(ALLOCATOR);
        output.writeInt(42);
        output.writeInt(-73467193);
        output.writeInt(373);
        output.writeInt(-93);
        output.writeInt(1312345);
        output.writeInt(785);

        output.writeInt(42);
        var buf = join(output.stop());

        assertByteBufContains(
                buf,
                (short) 24,
                42,
                -73467193,
                373,
                -93,
                1312345,
                785, // chunk 1
                (short) 4,
                42 // chunk 2
                );
    }

    @Test
    void shouldWriteLongAtTheBeginningOfChunk() {
        var output = new ChunkAwareByteBufOutput(12);

        output.start(ALLOCATOR);
        output.writeLong(15);
        var buf = join(output.stop());

        assertByteBufContains(buf, (short) 8, 15L);
    }

    @Test
    void shouldWriteLongWhenCurrentChunkContainsSpace() {
        var output = new ChunkAwareByteBufOutput(34);

        output.start(ALLOCATOR);
        output.writeLong(Long.MAX_VALUE);
        output.writeLong(-1);
        output.writeLong(-100);

        output.writeLong(Long.MIN_VALUE / 2);
        var buf = join(output.stop());

        assertByteBufContains(buf, (short) 32, Long.MAX_VALUE, -1L, -100L, Long.MIN_VALUE / 2);
    }

    @Test
    void shouldWriteLongWhenCurrentChunkIsFull() {
        var output = new ChunkAwareByteBufOutput(38);

        output.start(ALLOCATOR);
        output.writeLong(12);
        output.writeLong(8741);
        output.writeLong(2314);
        output.writeLong(-85793);

        output.writeLong(-57999999);
        var buf = join(output.stop());

        assertByteBufContains(
                buf,
                (short) 32,
                12L,
                8741L,
                2314L,
                -85793L, // chunk 1
                (short) 8,
                -57999999L // chunk 2
                );
    }

    @Test
    void shouldWriteDoubleAtTheBeginningOfChunk() {
        var output = new ChunkAwareByteBufOutput(10);

        output.start(ALLOCATOR);
        output.writeDouble(12.99937);
        var buf = join(output.stop());

        assertByteBufContains(buf, (short) 8, 12.99937D);
    }

    @Test
    void shouldWriteDoubleWhenCurrentChunkContainsSpace() {
        var output = new ChunkAwareByteBufOutput(18);

        output.start(ALLOCATOR);
        output.writeDouble(-5);

        output.writeDouble(991.3333);
        var buf = join(output.stop());

        assertByteBufContains(buf, (short) 16, -5D, 991.3333D);
    }

    @Test
    void shouldWriteDoubleWhenCurrentChunkIsFull() {
        var output = new ChunkAwareByteBufOutput(20);

        output.start(ALLOCATOR);
        output.writeDouble(1839);
        output.writeDouble(5710923.34873);

        output.writeDouble(-47389.333399);
        var buf = join(output.stop());

        assertByteBufContains(
                buf,
                (short) 16,
                1839D,
                5710923.34873D, // chunk 1
                (short) 8,
                -47389.333399D // chunk 2
                );
    }

    @Test
    void shouldWriteBytesAtTheBeginningOfChunk() {
        var output = new ChunkAwareByteBufOutput(10);

        output.start(ALLOCATOR);
        output.writeBytes(new byte[] {1, 2, 3, -1, -2, -3, 127});
        var buf = join(output.stop());

        assertByteBufContains(
                buf, (short) 7, (byte) 1, (byte) 2, (byte) 3, (byte) -1, (byte) -2, (byte) -3, (byte) 127);
    }

    @Test
    void shouldWriteBytesWhenCurrentChunkContainsSpace() {
        var output = new ChunkAwareByteBufOutput(13);

        output.start(ALLOCATOR);
        output.writeBytes(new byte[] {9, 8, -10});
        output.writeBytes(new byte[] {127, 126, -128, -126});
        output.writeBytes(new byte[] {0, 99});

        output.writeBytes(new byte[] {-42, 42});
        var buf = join(output.stop());

        assertByteBufContains(
                buf,
                (short) 11,
                (byte) 9,
                (byte) 8,
                (byte) -10,
                (byte) 127,
                (byte) 126,
                (byte) -128,
                (byte) -126,
                (byte) 0,
                (byte) 99,
                (byte) -42,
                (byte) 42);
    }

    @Test
    void shouldWriteBytesWhenCurrentChunkIsFull() {
        var output = new ChunkAwareByteBufOutput(9);

        output.start(ALLOCATOR);
        output.writeBytes(new byte[] {1, 2});
        output.writeBytes(new byte[] {3, 4, 5});
        output.writeBytes(new byte[] {10});

        output.writeBytes(new byte[] {-1, -42, -43});
        var buf = join(output.stop());

        assertByteBufContains(
                buf,
                (short) 7,
                (byte) 1,
                (byte) 2,
                (byte) 3,
                (byte) 4,
                (byte) 5,
                (byte) 10,
                (byte) -1, // chunk 1
                (short) 2,
                (byte) -42,
                (byte) -43 // chunk 2
                );
    }

    @Test
    void shouldWriteBytesThatSpanMultipleChunks() {
        var output = new ChunkAwareByteBufOutput(7);

        output.start(ALLOCATOR);
        output.writeBytes(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18});
        var buf = join(output.stop());

        assertByteBufContains(
                buf,
                (short) 5,
                (byte) 1,
                (byte) 2,
                (byte) 3,
                (byte) 4,
                (byte) 5, // chunk 1
                (short) 5,
                (byte) 6,
                (byte) 7,
                (byte) 8,
                (byte) 9,
                (byte) 10, // chunk 2
                (short) 5,
                (byte) 11,
                (byte) 12,
                (byte) 13,
                (byte) 14,
                (byte) 15, // chunk 3
                (short) 3,
                (byte) 16,
                (byte) 17,
                (byte) 18 // chunk 4
                );
    }

    @Test
    void shouldWriteDataToMultipleChunks() {
        var output = new ChunkAwareByteBufOutput(13);

        output.start(ALLOCATOR);
        output.writeDouble(12.3);
        output.writeByte((byte) 42);
        output.writeInt(-10);
        output.writeInt(99);
        output.writeLong(99);
        output.writeBytes(new byte[] {9, 8, 7, 6});
        output.writeDouble(0.333);
        output.writeShort((short) 0);
        output.writeShort((short) 1);
        output.writeInt(12345);
        output.writeBytes(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        var buf = join(output.stop());

        assertByteBufContains(
                buf,
                (short) 9,
                12.3D,
                (byte) 42, // chunk 1
                (short) 8,
                -10,
                99, // chunk 2
                (short) 11,
                99L,
                (byte) 9,
                (byte) 8,
                (byte) 7, // chunk 3
                (short) 11,
                (byte) 6,
                0.333D,
                (short) 0, // chunk 4
                (short) 11,
                (short) 1,
                12345,
                (byte) 1,
                (byte) 2,
                (byte) 3,
                (byte) 4,
                (byte) 5, // chunk 5
                (short) 5,
                (byte) 6,
                (byte) 7,
                (byte) 8,
                (byte) 9,
                (byte) 10 // chunk 6
                );
    }
}
