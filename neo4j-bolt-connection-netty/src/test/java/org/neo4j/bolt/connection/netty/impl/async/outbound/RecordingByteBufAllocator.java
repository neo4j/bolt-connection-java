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

import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledDirectByteBuf;
import io.netty.buffer.UnpooledHeapByteBuf;
import java.util.ArrayList;
import java.util.List;

class RecordingByteBufAllocator extends AbstractByteBufAllocator {
    private final List<ByteBuf> allocated = new ArrayList<>();
    private final int maxAllocations;

    RecordingByteBufAllocator() {
        this(Integer.MAX_VALUE);
    }

    RecordingByteBufAllocator(int maxAllocations) {
        this.maxAllocations = maxAllocations;
    }

    List<ByteBuf> allocated() {
        return allocated;
    }

    @Override
    protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
        failIfExhausted();
        return record(new UnpooledHeapByteBuf(this, initialCapacity, maxCapacity));
    }

    @Override
    protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
        failIfExhausted();
        return record(new UnpooledDirectByteBuf(this, initialCapacity, maxCapacity));
    }

    @Override
    public boolean isDirectBufferPooled() {
        return false;
    }

    private void failIfExhausted() {
        if (allocated.size() >= maxAllocations) {
            throw new OutOfMemoryError("Allocation " + (allocated.size() + 1) + " exceeds the test limit");
        }
    }

    private ByteBuf record(ByteBuf buf) {
        allocated.add(buf);
        return buf;
    }
}
