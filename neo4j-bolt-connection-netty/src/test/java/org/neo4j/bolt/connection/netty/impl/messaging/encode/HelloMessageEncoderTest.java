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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.neo4j.bolt.connection.netty.impl.messaging.request.PullAllMessage.PULL_ALL;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.connection.codec.WriteOutput;
import org.neo4j.bolt.connection.codec.network.ValueEncoder;
import org.neo4j.bolt.connection.netty.impl.BoltAgentUtil;
import org.neo4j.bolt.connection.netty.impl.messaging.request.HelloMessage;
import org.neo4j.bolt.connection.test.values.TestValueFactory;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.bolt.connection.values.ValueFactory;

class HelloMessageEncoderTest {
    private static final ValueFactory valueFactory = TestValueFactory.INSTANCE;
    private final HelloMessageEncoder encoder = new HelloMessageEncoder();
    private final ValueEncoder valueEncoder = mock(ValueEncoder.class);

    @Test
    void shouldEncodeHelloMessage() throws Exception {
        Map<String, Value> authToken = new HashMap<>();
        authToken.put("username", valueFactory.value("bob"));
        authToken.put("password", valueFactory.value("secret"));

        var output = mock(WriteOutput.class);
        encoder.encode(
                new HelloMessage("MyDriver", BoltAgentUtil.VALUE, authToken, null, false, null, false, valueFactory),
                valueEncoder,
                output,
                valueFactory);

        var order = inOrder(valueEncoder);
        order.verify(valueEncoder).encodeStructHeader(1, HelloMessage.SIGNATURE, output);

        Map<String, Value> expectedMetadata = new HashMap<>(authToken);
        expectedMetadata.put("user_agent", valueFactory.value("MyDriver"));
        expectedMetadata.put("bolt_agent", valueFactory.value(Map.of("product", BoltAgentUtil.VALUE.product())));
        order.verify(valueEncoder).encode(expectedMetadata, output);
    }

    @Test
    void shouldEncodeHelloMessageWithRoutingContext() throws Exception {
        Map<String, Value> authToken = new HashMap<>();
        authToken.put("username", valueFactory.value("bob"));
        authToken.put("password", valueFactory.value("secret"));

        Map<String, String> routingContext = new HashMap<>();
        routingContext.put("policy", "eu-fast");

        var output = mock(WriteOutput.class);
        encoder.encode(
                new HelloMessage(
                        "MyDriver", BoltAgentUtil.VALUE, authToken, routingContext, false, null, false, valueFactory),
                valueEncoder,
                output,
                valueFactory);

        var order = inOrder(valueEncoder);
        order.verify(valueEncoder).encodeStructHeader(1, HelloMessage.SIGNATURE, output);

        Map<String, Value> expectedMetadata = new HashMap<>(authToken);
        expectedMetadata.put("user_agent", valueFactory.value("MyDriver"));
        expectedMetadata.put("bolt_agent", valueFactory.value(Map.of("product", BoltAgentUtil.VALUE.product())));
        expectedMetadata.put("routing", valueFactory.value(routingContext));
        order.verify(valueEncoder).encode(expectedMetadata, output);
    }

    @Test
    void shouldFailToEncodeWrongMessage() {
        assertThrows(
                IllegalArgumentException.class,
                () -> encoder.encode(PULL_ALL, valueEncoder, mock(WriteOutput.class), valueFactory));
    }
}
