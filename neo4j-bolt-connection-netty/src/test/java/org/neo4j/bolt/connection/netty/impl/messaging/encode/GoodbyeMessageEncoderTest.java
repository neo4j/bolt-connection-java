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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.bolt.connection.netty.impl.messaging.request.DiscardAllMessage.DISCARD_ALL;
import static org.neo4j.bolt.connection.netty.impl.messaging.request.GoodbyeMessage.GOODBYE;

import org.junit.jupiter.api.Test;
import org.neo4j.bolt.connection.codec.WriteOutput;
import org.neo4j.bolt.connection.codec.network.ValueEncoder;
import org.neo4j.bolt.connection.netty.impl.messaging.request.GoodbyeMessage;
import org.neo4j.bolt.connection.values.ValueFactory;

class GoodbyeMessageEncoderTest {
    private final GoodbyeMessageEncoder encoder = new GoodbyeMessageEncoder();
    private final ValueEncoder valueEncoder = mock(ValueEncoder.class);

    @Test
    void shouldEncodeGoodbyeMessage() throws Exception {
        var output = mock(WriteOutput.class);
        encoder.encode(GOODBYE, valueEncoder, output, mock(ValueFactory.class));

        verify(valueEncoder).encodeStructHeader(0, GoodbyeMessage.SIGNATURE, output);
    }

    @Test
    void shouldFailToEncodeWrongMessage() {
        assertThrows(
                IllegalArgumentException.class,
                () -> encoder.encode(DISCARD_ALL, valueEncoder, mock(WriteOutput.class), mock(ValueFactory.class)));
    }
}
