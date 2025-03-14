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
package org.neo4j.bolt.connection.netty.impl.util.messaging;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;
import org.neo4j.bolt.connection.netty.impl.GqlErrorUtil;
import org.neo4j.bolt.connection.netty.impl.async.inbound.ByteBufInput;
import org.neo4j.bolt.connection.netty.impl.messaging.Message;
import org.neo4j.bolt.connection.netty.impl.messaging.MessageFormat;
import org.neo4j.bolt.connection.netty.impl.messaging.ResponseMessageHandler;
import org.neo4j.bolt.connection.netty.impl.messaging.response.FailureMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.response.IgnoredMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.response.RecordMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.response.SuccessMessage;
import org.neo4j.bolt.connection.netty.impl.packstream.PackInput;
import org.neo4j.bolt.connection.netty.impl.util.io.ByteBufOutput;
import org.neo4j.bolt.connection.test.values.TestValueFactory;

public abstract class AbstractMessageReaderTestBase {
    protected static final TestValueFactory valueFactory = TestValueFactory.INSTANCE;

    @TestFactory
    Stream<DynamicNode> shouldReadSupportedMessages() {
        return supportedMessages()
                .map(message -> dynamicTest(message.toString(), () -> testSupportedMessageReading(message)));
    }

    private void testSupportedMessageReading(Message message) throws IOException {
        var handler = testMessageReading(message);

        if (message instanceof SuccessMessage successMessage) {
            verify(handler).handleSuccessMessage(successMessage.metadata());
        } else if (message instanceof FailureMessage failureMessage) {
            verify(handler)
                    .handleFailureMessage(GqlErrorUtil.gqlError(failureMessage.code(), failureMessage.message()));
        } else if (message instanceof IgnoredMessage) {
            verify(handler).handleIgnoredMessage();
        } else if (message instanceof RecordMessage recordMessage) {
            verify(handler).handleRecordMessage(recordMessage.fields());
        } else {
            fail("Unsupported message type " + message.getClass().getSimpleName());
        }
    }

    @TestFactory
    Stream<DynamicNode> shouldFailToReadUnsupportedMessages() {
        return unsupportedMessages()
                .map(message -> dynamicTest(message.toString(), () -> testUnsupportedMessageReading(message)));
    }

    private void testUnsupportedMessageReading(Message message) {
        assertThrows(IOException.class, () -> testMessageReading(message));
    }

    protected abstract Stream<Message> supportedMessages();

    protected abstract Stream<Message> unsupportedMessages();

    protected abstract MessageFormat.Reader newReader(PackInput input);

    protected ResponseMessageHandler testMessageReading(Message message) throws IOException {
        var input = newInputWith(message);
        var reader = newReader(input);

        var handler = mock(ResponseMessageHandler.class);
        reader.read(handler);

        return handler;
    }

    private PackInput newInputWith(Message message) throws IOException {
        var buffer = Unpooled.buffer();

        MessageFormat messageFormat = new KnowledgeableMessageFormat(isElementIdEnabled());
        if (isDateTimeUtcEnabled()) {
            messageFormat.enableDateTimeUtc();
        }
        var writer = messageFormat.newWriter(new ByteBufOutput(buffer), TestValueFactory.INSTANCE);
        writer.write(message);

        var input = new ByteBufInput();
        input.start(buffer);
        return input;
    }

    protected boolean isElementIdEnabled() {
        return false;
    }

    protected boolean isDateTimeUtcEnabled() {
        return false;
    }
}
