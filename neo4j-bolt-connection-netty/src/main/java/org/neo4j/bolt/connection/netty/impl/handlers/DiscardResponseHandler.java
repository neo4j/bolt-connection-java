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
package org.neo4j.bolt.connection.netty.impl.handlers;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.neo4j.bolt.connection.netty.impl.spi.ResponseHandler;
import org.neo4j.bolt.connection.summary.DiscardSummary;
import org.neo4j.bolt.connection.values.Value;

public class DiscardResponseHandler implements ResponseHandler {
    private final CompletableFuture<DiscardSummary> future;

    public DiscardResponseHandler(CompletableFuture<DiscardSummary> future) {
        this.future = Objects.requireNonNull(future, "future must not be null");
    }

    @Override
    public void onSuccess(Map<String, Value> metadata) {
        this.future.complete(new DiscardSummaryImpl(metadata));
    }

    @Override
    public void onFailure(Throwable error) {
        this.future.completeExceptionally(error);
    }

    @Override
    public void onRecord(List<Value> fields) {}

    private record DiscardSummaryImpl(Map<String, Value> metadata) implements DiscardSummary {
        @Override
        public Map<String, Value> metadata() {
            return metadata;
        }
    }
}
