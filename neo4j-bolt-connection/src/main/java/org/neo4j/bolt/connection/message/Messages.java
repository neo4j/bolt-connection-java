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
import java.util.Set;
import org.neo4j.bolt.connection.AccessMode;
import org.neo4j.bolt.connection.AuthToken;
import org.neo4j.bolt.connection.NotificationConfig;
import org.neo4j.bolt.connection.TelemetryApi;
import org.neo4j.bolt.connection.TransactionType;
import org.neo4j.bolt.connection.values.Value;

public final class Messages {

    public static RouteMessage route(String databaseName, String impersonatedUser, Set<String> bookmarks) {
        return new RouteMessageImpl(databaseName, impersonatedUser, bookmarks);
    }

    public static BeginMessage beginTransaction(
            String databaseName,
            AccessMode accessMode,
            String impersonatedUser,
            Set<String> bookmarks,
            TransactionType transactionType,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            NotificationConfig notificationConfig) {
        return new BeginMessageImpl(
                databaseName,
                accessMode,
                impersonatedUser,
                bookmarks,
                transactionType,
                txTimeout,
                txMetadata,
                notificationConfig);
    }

    public static RunMessage run(
            String databaseName,
            AccessMode accessMode,
            String impersonatedUser,
            Set<String> bookmarks,
            String query,
            Map<String, Value> parameters,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            NotificationConfig notificationConfig) {
        var extra = new RunMessageImpl.Extra(
                databaseName, accessMode, impersonatedUser, bookmarks, txTimeout, txMetadata, notificationConfig);
        return new RunMessageImpl(query, parameters, extra);
    }

    public static RunMessage run(String query, Map<String, Value> parameters) {
        return new RunMessageImpl(query, parameters, null);
    }

    public static PullMessage pull(long qid, long request) {
        return new PullMessageImpl(qid, request);
    }

    public static DiscardMessage discard(long qid, long number) {
        return new DiscardMessageImpl(qid, number);
    }

    public static CommitMessage commit() {
        return COMMIT_MESSAGE;
    }

    public static RollbackMessage rollback() {
        return ROLLBACK_MESSAGE;
    }

    public static ResetMessage reset() {
        return RESET_MESSAGE;
    }

    public static LogoffMessage logoff() {
        return LOGOFF_MESSAGE;
    }

    public static LogonMessage logon(AuthToken authToken) {
        return new LogonMessageImpl(authToken);
    }

    public static TelemetryMessage telemetry(TelemetryApi telemetryApi) {
        return new TelemetryMessageImpl(telemetryApi);
    }

    private static final CommitMessage COMMIT_MESSAGE = new CommitMessageImpl();

    private static final RollbackMessage ROLLBACK_MESSAGE = new RollbackMessageImpl();

    private static final ResetMessage RESET_MESSAGE = new ResetMessageImpl();

    private static final LogoffMessage LOGOFF_MESSAGE = new LogoffMessageImpl();

    private Messages() {}
}
