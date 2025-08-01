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
package org.neo4j.bolt.connection.netty.impl.messaging.v44;

import static java.time.Duration.ofSeconds;
import static java.util.Calendar.DECEMBER;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.neo4j.bolt.connection.AccessMode.READ;
import static org.neo4j.bolt.connection.AccessMode.WRITE;
import static org.neo4j.bolt.connection.DatabaseName.database;
import static org.neo4j.bolt.connection.DatabaseName.defaultDatabase;
import static org.neo4j.bolt.connection.netty.impl.messaging.request.CommitMessage.COMMIT;
import static org.neo4j.bolt.connection.netty.impl.messaging.request.DiscardAllMessage.DISCARD_ALL;
import static org.neo4j.bolt.connection.netty.impl.messaging.request.GoodbyeMessage.GOODBYE;
import static org.neo4j.bolt.connection.netty.impl.messaging.request.PullAllMessage.PULL_ALL;
import static org.neo4j.bolt.connection.netty.impl.messaging.request.ResetMessage.RESET;
import static org.neo4j.bolt.connection.netty.impl.messaging.request.RollbackMessage.ROLLBACK;
import static org.neo4j.bolt.connection.netty.impl.messaging.request.RunWithMetadataMessage.autoCommitTxRunMessage;
import static org.neo4j.bolt.connection.netty.impl.messaging.request.RunWithMetadataMessage.unmanagedTxRunMessage;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.neo4j.bolt.connection.netty.impl.BoltAgentUtil;
import org.neo4j.bolt.connection.netty.impl.NoopLoggingProvider;
import org.neo4j.bolt.connection.netty.impl.messaging.Message;
import org.neo4j.bolt.connection.netty.impl.messaging.MessageFormat;
import org.neo4j.bolt.connection.netty.impl.messaging.request.BeginMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.DiscardMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.HelloMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.PullMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.RouteMessage;
import org.neo4j.bolt.connection.netty.impl.packstream.PackOutput;
import org.neo4j.bolt.connection.netty.impl.util.messaging.AbstractMessageWriterTestBase;
import org.neo4j.bolt.connection.values.Value;

/**
 * The MessageWriter under tests is the one provided by the {@link BoltProtocolV44} and not a specific class
 * implementation.
 * <p>
 * It's done on this way to make easy to replace the implementation and still getting the same behaviour.
 */
public class MessageWriterV44Test extends AbstractMessageWriterTestBase {
    @Override
    protected MessageFormat.Writer newWriter(PackOutput output) {
        return BoltProtocolV44.INSTANCE.createMessageFormat().newWriter(output, valueFactory);
    }

    @Override
    protected Stream<Message> supportedMessages() {
        return Stream.of(
                // Bolt V2 Data Types
                unmanagedTxRunMessage("RETURN $point", singletonMap("point", valueFactory.point(42, 12.99, -180.0))),
                unmanagedTxRunMessage(
                        "RETURN $point", singletonMap("point", valueFactory.point(42, 0.51, 2.99, 100.123))),
                unmanagedTxRunMessage(
                        "RETURN $date", singletonMap("date", valueFactory.value(LocalDate.ofEpochDay(2147483650L)))),
                unmanagedTxRunMessage(
                        "RETURN $time",
                        singletonMap("time", valueFactory.value(OffsetTime.of(4, 16, 20, 999, ZoneOffset.MIN)))),
                unmanagedTxRunMessage(
                        "RETURN $time", singletonMap("time", valueFactory.value(LocalTime.of(12, 9, 18, 999_888)))),
                unmanagedTxRunMessage(
                        "RETURN $dateTime",
                        singletonMap(
                                "dateTime", valueFactory.value(LocalDateTime.of(2049, DECEMBER, 12, 17, 25, 49, 199)))),
                unmanagedTxRunMessage(
                        "RETURN $dateTime",
                        singletonMap(
                                "dateTime",
                                valueFactory.value(ZonedDateTime.of(
                                        2000, 1, 10, 12, 2, 49, 300, ZoneOffset.ofHoursMinutes(9, 30))))),
                unmanagedTxRunMessage(
                        "RETURN $dateTime",
                        singletonMap(
                                "dateTime",
                                valueFactory.value(
                                        ZonedDateTime.of(2000, 1, 10, 12, 2, 49, 300, ZoneId.of("Europe/Stockholm"))))),

                // New Bolt V4 messages
                new PullMessage(100, 200, valueFactory),
                new DiscardMessage(300, 400, valueFactory),

                // Bolt V3 messages
                new HelloMessage(
                        "MyDriver/1.2.3",
                        BoltAgentUtil.VALUE,
                        Map.of(
                                "scheme",
                                valueFactory.value("basic"),
                                "principal",
                                valueFactory.value("neo4j"),
                                "credentials",
                                valueFactory.value("neo4j")),
                        Collections.emptyMap(),
                        false,
                        null,
                        false,
                        valueFactory),
                GOODBYE,
                new BeginMessage(
                        Collections.singleton("neo4j:bookmark:v1:tx123"),
                        ofSeconds(5),
                        singletonMap("key", valueFactory.value(42)),
                        READ,
                        defaultDatabase(),
                        null,
                        null,
                        null,
                        false,
                        NoopLoggingProvider.INSTANCE,
                        valueFactory),
                new BeginMessage(
                        Collections.singleton("neo4j:bookmark:v1:tx123"),
                        ofSeconds(5),
                        singletonMap("key", valueFactory.value(42)),
                        WRITE,
                        database("foo"),
                        null,
                        null,
                        null,
                        false,
                        NoopLoggingProvider.INSTANCE,
                        valueFactory),
                COMMIT,
                ROLLBACK,
                RESET,
                autoCommitTxRunMessage(
                        "RETURN 1",
                        Collections.emptyMap(),
                        ofSeconds(5),
                        singletonMap("key", valueFactory.value(42)),
                        defaultDatabase(),
                        READ,
                        Collections.singleton("neo4j:bookmark:v1:tx1"),
                        null,
                        null,
                        false,
                        NoopLoggingProvider.INSTANCE,
                        valueFactory),
                autoCommitTxRunMessage(
                        "RETURN 1",
                        Collections.emptyMap(),
                        ofSeconds(5),
                        singletonMap("key", valueFactory.value(42)),
                        database("foo"),
                        WRITE,
                        Collections.singleton("neo4j:bookmark:v1:tx1"),
                        null,
                        null,
                        false,
                        NoopLoggingProvider.INSTANCE,
                        valueFactory),
                unmanagedTxRunMessage("RETURN 1", Collections.emptyMap()),

                // Bolt V3 messages with struct values
                autoCommitTxRunMessage(
                        "RETURN $x",
                        singletonMap("x", valueFactory.value(ZonedDateTime.now())),
                        ofSeconds(1),
                        emptyMap(),
                        defaultDatabase(),
                        READ,
                        Collections.emptySet(),
                        null,
                        null,
                        false,
                        NoopLoggingProvider.INSTANCE,
                        valueFactory),
                autoCommitTxRunMessage(
                        "RETURN $x",
                        singletonMap("x", valueFactory.value(ZonedDateTime.now())),
                        ofSeconds(1),
                        emptyMap(),
                        database("foo"),
                        WRITE,
                        Collections.emptySet(),
                        null,
                        null,
                        false,
                        NoopLoggingProvider.INSTANCE,
                        valueFactory),
                unmanagedTxRunMessage("RETURN $x", singletonMap("x", valueFactory.point(42, 1, 2, 3))),

                // New 4.3 Messages
                routeMessage());
    }

    @Override
    protected Stream<Message> unsupportedMessages() {
        return Stream.of(
                // Bolt V1, V2 and V3 messages
                PULL_ALL, DISCARD_ALL);
    }

    private RouteMessage routeMessage() {
        Map<String, Value> routeContext = new HashMap<>();
        routeContext.put("someContext", valueFactory.value(124));
        return new RouteMessage(routeContext, Collections.emptySet(), "dbName", null);
    }
}
