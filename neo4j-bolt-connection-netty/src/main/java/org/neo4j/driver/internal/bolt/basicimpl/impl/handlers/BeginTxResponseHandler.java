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
package org.neo4j.driver.internal.bolt.basicimpl.impl.handlers;

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.neo4j.driver.internal.bolt.api.summary.BeginSummary;
import org.neo4j.driver.internal.bolt.api.values.Value;
import org.neo4j.driver.internal.bolt.basicimpl.impl.spi.ResponseHandler;

public class BeginTxResponseHandler implements ResponseHandler {
    private final CompletableFuture<BeginSummary> beginTxFuture;

    public BeginTxResponseHandler(CompletableFuture<BeginSummary> beginTxFuture) {
        this.beginTxFuture = requireNonNull(beginTxFuture);
    }

    @Override
    public void onSuccess(Map<String, Value> metadata) {
        var db = metadata.get("db");
        var databaseName = db != null ? db.asString() : null;
        beginTxFuture.complete(new BeginSummaryImpl(databaseName));
    }

    @Override
    public void onFailure(Throwable error) {
        beginTxFuture.completeExceptionally(error);
    }

    @Override
    public void onRecord(Value[] fields) {
        throw new UnsupportedOperationException(
                "Transaction begin is not expected to receive records: " + Arrays.toString(fields));
    }

    private record BeginSummaryImpl(String database) implements BeginSummary {
        @Override
        public Optional<String> databaseName() {
            return Optional.ofNullable(database);
        }
    }
}
