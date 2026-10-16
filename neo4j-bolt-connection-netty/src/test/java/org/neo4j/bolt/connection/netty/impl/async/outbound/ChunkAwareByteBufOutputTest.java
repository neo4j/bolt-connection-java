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

    private static IntStream segmentSizes() {
        return IntStream.iterate(1, size -> size * 2).limit(20);
    }

    @Test
    void shouldThrowForIllegalMaxChunkSize() {
        assertThrows(IllegalArgumentException.class, () -> new ChunkAwareByteBufOutput(-42, 16));
    }

    @Test
    void shouldThrowForIllegalSegmentSize() {
        assertThrows(IllegalArgumentException.class, () -> new ChunkAwareByteBufOutput(16, 0));
    }

    @Test
    void shouldThrowWhenStartedWithNullAllocator() {
        var output = new ChunkAwareByteBufOutput(16, 16);
        assertThrows(NullPointerException.class, () -> output.start(null));
    }

    @Test
    void shouldThrowWhenStartedTwice() {
        var output = new ChunkAwareByteBufOutput(16, 16);
        output.start(ALLOCATOR);

        assertThrows(IllegalStateException.class, () -> output.start(ALLOCATOR));
        output.abort();
    }

    @Test
    void shouldStartNewSegmentAtTheFirstChunkBoundaryAfterReachingSegmentSize() {
        // chunks of 2 + 5 bytes: the second chunk takes the first segment past 10 bytes
        var output = new ChunkAwareByteBufOutput(7, 10);

        output.start(ALLOCATOR);
        output.writeBytes(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18});
        var segments = output.stop();

        assertEquals(2, segments.size());
        assertByteBufContains(
                segments.get(0),
                (short) 5,
                (byte) 1,
                (byte) 2,
                (byte) 3,
                (byte) 4,
                (byte) 5,
                (short) 5,
                (byte) 6,
                (byte) 7,
                (byte) 8,
                (byte) 9,
                (byte) 10);
        assertByteBufContains(
                segments.get(1),
                (short) 5,
                (byte) 11,
                (byte) 12,
                (byte) 13,
                (byte) 14,
                (byte) 15,
                (short) 3,
                (byte) 16,
                (byte) 17,
                (byte) 18);
    }

    @Test
    void shouldNotGrowSegmentWhenItsLastChunkStartsJustBeforeSegmentSizeAndIsFull() {
        // full chunks of 7 bytes start at 0, 7 and 14, one byte before the segment size of 15, so each segment holds
        // 21 bytes and the boundary takes the last one to 23 of its 24 bytes
        var output = new ChunkAwareByteBufOutput(7, 15);

        output.start(ALLOCATOR);
        output.writeBytes(new byte[6 * 5]);
        var segments = output.stop();
        var last = segments.get(segments.size() - 1);
        var capacityBeforeBoundary = last.capacity();
        BoltProtocolUtil.writeMessageBoundary(last);

        try {
            assertEquals(2, segments.size());
            assertEquals(15 + 7 + BoltProtocolUtil.CHUNK_HEADER_SIZE_BYTES, capacityBeforeBoundary);
            assertEquals(23, last.readableBytes());
            assertEquals(capacityBeforeBoundary, last.capacity());
        } finally {
            segments.forEach(ByteBuf::release);
        }
    }

    @Test
    void shouldRequestPowerOfTwoSegmentCapacityByDefault() {
        var output = new ChunkAwareByteBufOutput();

        output.start(ALLOCATOR);
        output.writeBytes(new byte[1024 * 1024]);
        var segments = output.stop();

        try {
            assertTrue(segments.size() > 1, "Expected more than one segment, got " + segments.size());
            var capacity = segments.get(1).capacity();
            assertEquals(1, Integer.bitCount(capacity), "Segment capacity is not a power of two: " + capacity);
        } finally {
            segments.forEach(ByteBuf::release);
        }
    }

    @Test
    void shouldReleaseEachBufferOnceAndBeReusableWhenSegmentAllocationFails() {
        var allocator = new RecordingByteBufAllocator(1);
        var output = new ChunkAwareByteBufOutput(7, 10);

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
    void shouldReleaseAllSegmentsOnAbortAndBeReusable() {
        var allocator = new RecordingByteBufAllocator();
        var output = new ChunkAwareByteBufOutput(7, 10);

        output.start(allocator);
        output.writeBytes(new byte[100]);
        output.abort();

        assertTrue(allocator.allocated().size() > 1);
        allocator.allocated().forEach(buf -> assertEquals(0, buf.refCnt()));

        output.start(ALLOCATOR);
        output.writeByte((byte) 42);
        assertByteBufContains(join(output.stop()), (short) 1, (byte) 42);
    }

    private static ByteBuf join(List<ByteBuf> segments) {
        return Unpooled.wrappedBuffer(segments.toArray(ByteBuf[]::new));
    }

    @ParameterizedTest
    @MethodSource("segmentSizes")
    void shouldWriteByteAtTheBeginningOfChunk(int segmentSize) {
        var output = new ChunkAwareByteBufOutput(16, segmentSize);

        output.start(ALLOCATOR);
        output.writeByte((byte) 42);
        var buf = join(output.stop());

        assertByteBufContains(buf, (short) 1, (byte) 42);
    }

    @ParameterizedTest
    @MethodSource("segmentSizes")
    void shouldWriteByteWhenCurrentChunkContainsSpace(int segmentSize) {
        var output = new ChunkAwareByteBufOutput(16, segmentSize);

        output.start(ALLOCATOR);
        output.writeByte((byte) 1);
        output.writeByte((byte) 2);
        output.writeByte((byte) -24);

        output.writeByte((byte) 42);
        var buf = join(output.stop());

        assertByteBufContains(buf, (short) 4, (byte) 1, (byte) 2, (byte) -24, (byte) 42);
    }

    @ParameterizedTest
    @MethodSource("segmentSizes")
    void shouldWriteByteWhenCurrentChunkIsFull(int segmentSize) {
        var output = new ChunkAwareByteBufOutput(5, segmentSize);

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

    @ParameterizedTest
    @MethodSource("segmentSizes")
    void shouldWriteShortAtTheBeginningOfChunk(int segmentSize) {
        var output = new ChunkAwareByteBufOutput(10, segmentSize);

        output.start(ALLOCATOR);
        output.writeShort(Short.MAX_VALUE);
        var buf = join(output.stop());

        assertByteBufContains(buf, (short) 2, Short.MAX_VALUE);
    }

    @ParameterizedTest
    @MethodSource("segmentSizes")
    void shouldWriteShortWhenCurrentChunkContainsSpace(int segmentSize) {
        var output = new ChunkAwareByteBufOutput(12, segmentSize);

        output.start(ALLOCATOR);
        output.writeShort((short) 1);
        output.writeShort((short) 42);
        output.writeShort((short) 4242);
        output.writeShort((short) 4242);

        output.writeShort((short) -30);
        var buf = join(output.stop());

        assertByteBufContains(buf, (short) 10, (short) 1, (short) 42, (short) 4242, (short) 4242, (short) -30);
    }

    @ParameterizedTest
    @MethodSource("segmentSizes")
    void shouldWriteShortWhenCurrentChunkIsFull(int segmentSize) {
        var output = new ChunkAwareByteBufOutput(8, segmentSize);

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

    @ParameterizedTest
    @MethodSource("segmentSizes")
    void shouldWriteIntAtTheBeginningOfChunk(int segmentSize) {
        var output = new ChunkAwareByteBufOutput(18, segmentSize);

        output.start(ALLOCATOR);
        output.writeInt(73649);
        var buf = join(output.stop());

        assertByteBufContains(buf, (short) 4, 73649);
    }

    @ParameterizedTest
    @MethodSource("segmentSizes")
    void shouldWriteIntWhenCurrentChunkContainsSpace(int segmentSize) {
        var output = new ChunkAwareByteBufOutput(40, segmentSize);

        output.start(ALLOCATOR);
        output.writeInt(Integer.MAX_VALUE);
        output.writeInt(20);
        output.writeInt(-173);

        output.writeInt(Integer.MIN_VALUE);
        var buf = join(output.stop());

        assertByteBufContains(buf, (short) 16, Integer.MAX_VALUE, 20, -173, Integer.MIN_VALUE);
    }

    @ParameterizedTest
    @MethodSource("segmentSizes")
    void shouldWriteIntWhenCurrentChunkIsFull(int segmentSize) {
        var output = new ChunkAwareByteBufOutput(27, segmentSize);

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

    @ParameterizedTest
    @MethodSource("segmentSizes")
    void shouldWriteLongAtTheBeginningOfChunk(int segmentSize) {
        var output = new ChunkAwareByteBufOutput(12, segmentSize);

        output.start(ALLOCATOR);
        output.writeLong(15);
        var buf = join(output.stop());

        assertByteBufContains(buf, (short) 8, 15L);
    }

    @ParameterizedTest
    @MethodSource("segmentSizes")
    void shouldWriteLongWhenCurrentChunkContainsSpace(int segmentSize) {
        var output = new ChunkAwareByteBufOutput(34, segmentSize);

        output.start(ALLOCATOR);
        output.writeLong(Long.MAX_VALUE);
        output.writeLong(-1);
        output.writeLong(-100);

        output.writeLong(Long.MIN_VALUE / 2);
        var buf = join(output.stop());

        assertByteBufContains(buf, (short) 32, Long.MAX_VALUE, -1L, -100L, Long.MIN_VALUE / 2);
    }

    @ParameterizedTest
    @MethodSource("segmentSizes")
    void shouldWriteLongWhenCurrentChunkIsFull(int segmentSize) {
        var output = new ChunkAwareByteBufOutput(38, segmentSize);

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

    @ParameterizedTest
    @MethodSource("segmentSizes")
    void shouldWriteDoubleAtTheBeginningOfChunk(int segmentSize) {
        var output = new ChunkAwareByteBufOutput(10, segmentSize);

        output.start(ALLOCATOR);
        output.writeDouble(12.99937);
        var buf = join(output.stop());

        assertByteBufContains(buf, (short) 8, 12.99937D);
    }

    @ParameterizedTest
    @MethodSource("segmentSizes")
    void shouldWriteDoubleWhenCurrentChunkContainsSpace(int segmentSize) {
        var output = new ChunkAwareByteBufOutput(18, segmentSize);

        output.start(ALLOCATOR);
        output.writeDouble(-5);

        output.writeDouble(991.3333);
        var buf = join(output.stop());

        assertByteBufContains(buf, (short) 16, -5D, 991.3333D);
    }

    @ParameterizedTest
    @MethodSource("segmentSizes")
    void shouldWriteDoubleWhenCurrentChunkIsFull(int segmentSize) {
        var output = new ChunkAwareByteBufOutput(20, segmentSize);

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

    @ParameterizedTest
    @MethodSource("segmentSizes")
    void shouldWriteBytesAtTheBeginningOfChunk(int segmentSize) {
        var output = new ChunkAwareByteBufOutput(10, segmentSize);

        output.start(ALLOCATOR);
        output.writeBytes(new byte[] {1, 2, 3, -1, -2, -3, 127});
        var buf = join(output.stop());

        assertByteBufContains(
                buf, (short) 7, (byte) 1, (byte) 2, (byte) 3, (byte) -1, (byte) -2, (byte) -3, (byte) 127);
    }

    @ParameterizedTest
    @MethodSource("segmentSizes")
    void shouldWriteBytesWhenCurrentChunkContainsSpace(int segmentSize) {
        var output = new ChunkAwareByteBufOutput(13, segmentSize);

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

    @ParameterizedTest
    @MethodSource("segmentSizes")
    void shouldWriteBytesWhenCurrentChunkIsFull(int segmentSize) {
        var output = new ChunkAwareByteBufOutput(9, segmentSize);

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

    @ParameterizedTest
    @MethodSource("segmentSizes")
    void shouldWriteBytesThatSpanMultipleChunks(int segmentSize) {
        var output = new ChunkAwareByteBufOutput(7, segmentSize);

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

    @ParameterizedTest
    @MethodSource("segmentSizes")
    void shouldWriteDataToMultipleChunks(int segmentSize) {
        var output = new ChunkAwareByteBufOutput(13, segmentSize);

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
