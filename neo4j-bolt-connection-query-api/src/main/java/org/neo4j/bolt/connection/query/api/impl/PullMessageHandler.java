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
package org.neo4j.bolt.connection.query.api.impl;

import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.ResponseHandler;
import org.neo4j.bolt.connection.message.PullMessage;
import org.neo4j.bolt.connection.values.Value;

final class PullMessageHandler implements MessageHandler<Void> {
    private final System.Logger log;
    private final ResponseHandler handler;
    private final PullMessage message;
    private final Function<Long, Query> queryFinder;
    private final Consumer<Long> queryDeleteConsumer;

    PullMessageHandler(
            ResponseHandler handler,
            PullMessage message,
            Function<Long, Query> queryFinder,
            Consumer<Long> queryDeleteConsumer,
            LoggingProvider logging) {
        this.log = logging.getLog(getClass());
        this.handler = Objects.requireNonNull(handler);
        this.message = Objects.requireNonNull(message);
        this.queryFinder = Objects.requireNonNull(queryFinder);
        this.queryDeleteConsumer = Objects.requireNonNull(queryDeleteConsumer);
    }

    @Override
    public CompletionStage<Void> exchange() {
        return CompletableFuture.<Void>completedStage(null).thenApply(ignored -> {
            var query = queryFinder.apply(message.qid());
            var records = new ArrayList<Value[]>();
            var deleted = false;

            var request = message.request() > 0 ? message.request() : Long.MAX_VALUE;
            var iterator = query.values().iterator();
            while (iterator.hasNext() && request > 0) {
                records.add(iterator.next());
                iterator.remove();
                request--;
            }
            if (query.values().isEmpty()) {
                queryDeleteConsumer.accept(query.id());
                deleted = true;
            }
            queryDeleteConsumer.accept(-1L);

            records.forEach(handler::onRecord);
            var hasMore = !deleted;
            handler.onPullSummary(new PullSummaryImpl(hasMore, hasMore ? Map.of() : query.metadata()));
            return null;
        });
    }
}
