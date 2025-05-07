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
package org.neo4j.bolt.connection.message;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.neo4j.bolt.connection.AccessMode;
import org.neo4j.bolt.connection.NotificationConfig;
import org.neo4j.bolt.connection.TransactionType;
import org.neo4j.bolt.connection.values.Value;

record BeginMessageImpl(
        String rawDatabaseName,
        AccessMode accessMode,
        String rawImpersonatedUser,
        Set<String> bookmarks,
        TransactionType transactionType,
        Duration rawTxTimeout,
        Map<String, Value> txMetadata,
        NotificationConfig notificationConfig)
        implements BeginMessage {
    @Override
    public Optional<String> databaseName() {
        return Optional.ofNullable(rawDatabaseName);
    }

    @Override
    public Optional<String> impersonatedUser() {
        return Optional.ofNullable(rawImpersonatedUser);
    }

    @Override
    public Optional<Duration> txTimeout() {
        return Optional.ofNullable(rawTxTimeout);
    }
}
