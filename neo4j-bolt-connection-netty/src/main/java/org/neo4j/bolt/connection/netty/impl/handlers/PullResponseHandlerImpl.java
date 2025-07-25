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
import org.neo4j.bolt.connection.netty.impl.messaging.PullMessageHandler;
import org.neo4j.bolt.connection.netty.impl.spi.ResponseHandler;
import org.neo4j.bolt.connection.summary.PullSummary;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.bolt.connection.values.ValueFactory;

public class PullResponseHandlerImpl implements ResponseHandler {
    private final PullMessageHandler handler;
    private final ValueFactory valueFactory;

    public PullResponseHandlerImpl(PullMessageHandler handler, ValueFactory valueFactory) {
        this.handler = handler;
        this.valueFactory = Objects.requireNonNull(valueFactory);
    }

    @Override
    public void onSuccess(Map<String, Value> metadata) {
        var hasMore =
                metadata.getOrDefault("has_more", valueFactory.value(false)).asBoolean();
        handler.onSummary(new PullSummaryImpl(hasMore, metadata));
    }

    @Override
    public void onFailure(Throwable throwable) {
        handler.onError(throwable);
    }

    @Override
    public void onRecord(List<Value> fields) {
        handler.onRecord(fields);
    }

    public record PullSummaryImpl(boolean hasMore, Map<String, Value> metadata) implements PullSummary {}
}
