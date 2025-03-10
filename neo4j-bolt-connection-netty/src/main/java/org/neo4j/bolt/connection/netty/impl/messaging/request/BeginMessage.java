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
package org.neo4j.bolt.connection.netty.impl.messaging.request;

import static org.neo4j.bolt.connection.netty.impl.messaging.request.TransactionMetadataBuilder.buildMetadata;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.neo4j.bolt.connection.AccessMode;
import org.neo4j.bolt.connection.DatabaseName;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.NotificationConfig;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.bolt.connection.values.ValueFactory;

public class BeginMessage extends MessageWithMetadata {
    public static final byte SIGNATURE = 0x11;

    public BeginMessage(
            Set<String> bookmarks,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            DatabaseName databaseName,
            AccessMode mode,
            String impersonatedUser,
            String txType,
            NotificationConfig notificationConfig,
            boolean legacyNotifications,
            LoggingProvider logging,
            ValueFactory valueFactory) {
        this(
                bookmarks,
                txTimeout,
                txMetadata,
                mode,
                databaseName,
                impersonatedUser,
                txType,
                notificationConfig,
                legacyNotifications,
                logging,
                valueFactory);
    }

    public BeginMessage(
            Set<String> bookmarks,
            Duration txTimeout,
            Map<String, Value> txMetadata,
            AccessMode mode,
            DatabaseName databaseName,
            String impersonatedUser,
            String txType,
            NotificationConfig notificationConfig,
            boolean legacyNotifications,
            LoggingProvider logging,
            ValueFactory valueFactory) {
        super(buildMetadata(
                txTimeout,
                txMetadata,
                databaseName,
                mode,
                bookmarks,
                impersonatedUser,
                txType,
                notificationConfig,
                legacyNotifications,
                logging,
                valueFactory));
    }

    @Override
    public byte signature() {
        return SIGNATURE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        var that = (BeginMessage) o;
        return Objects.equals(metadata(), that.metadata());
    }

    @Override
    public int hashCode() {
        return Objects.hash(metadata());
    }

    @Override
    public String toString() {
        return "BEGIN " + metadata();
    }
}
