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
package org.neo4j.bolt.connection.netty.impl.messaging.encode;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.bolt.connection.codec.WriteOutput;
import org.neo4j.bolt.connection.codec.network.ValueEncoder;
import org.neo4j.bolt.connection.netty.impl.messaging.Message;
import org.neo4j.bolt.connection.netty.impl.messaging.request.RouteMessage;
import org.neo4j.bolt.connection.test.values.TestValueFactory;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.bolt.connection.values.ValueFactory;

class RouteMessageEncoderTest {
    private static final ValueFactory valueFactory = TestValueFactory.INSTANCE;
    private final ValueEncoder valueEncoder = mock(ValueEncoder.class);
    private final RouteMessageEncoder encoder = new RouteMessageEncoder();

    @ParameterizedTest
    @ValueSource(strings = {"neo4j"})
    @NullSource
    void shouldEncodeRouteMessage(String databaseName) throws IOException {
        var routingContext = getRoutingContext();

        var output = mock(WriteOutput.class);
        encoder.encode(
                new RouteMessage(getRoutingContext(), Collections.emptySet(), databaseName, null),
                valueEncoder,
                output,
                valueFactory);

        var inOrder = inOrder(valueEncoder);

        inOrder.verify(valueEncoder).encodeStructHeader(3, (byte) 0x66, output);
        inOrder.verify(valueEncoder).encode(routingContext, output);
        inOrder.verify(valueEncoder).encode(valueFactory.value(emptyList()), output);
        inOrder.verify(valueEncoder).encode(databaseName, output);
    }

    @ParameterizedTest
    @ValueSource(strings = {"neo4j"})
    @NullSource
    void shouldEncodeRouteMessageWithBookmark(String databaseName) throws IOException {
        var routingContext = getRoutingContext();
        var bookmark = "somebookmark";

        var output = mock(WriteOutput.class);
        encoder.encode(
                new RouteMessage(getRoutingContext(), Collections.singleton(bookmark), databaseName, null),
                valueEncoder,
                output,
                valueFactory);

        var inOrder = inOrder(valueEncoder);

        inOrder.verify(valueEncoder).encodeStructHeader(3, (byte) 0x66, output);
        inOrder.verify(valueEncoder).encode(routingContext, output);
        inOrder.verify(valueEncoder)
                .encode(valueFactory.value(Collections.singleton(valueFactory.value(bookmark))), output);
        inOrder.verify(valueEncoder).encode(databaseName, output);
    }

    @Test
    void shouldThrowIllegalArgumentIfMessageIsNotRouteMessage() {
        var message = mock(Message.class);

        assertThrows(
                IllegalArgumentException.class,
                () -> encoder.encode(message, valueEncoder, mock(WriteOutput.class), valueFactory));
    }

    private Map<String, Value> getRoutingContext() {
        Map<String, Value> routingContext = new HashMap<>();
        routingContext.put("ip", valueFactory.value("127.0.0.1"));
        return routingContext;
    }
}
