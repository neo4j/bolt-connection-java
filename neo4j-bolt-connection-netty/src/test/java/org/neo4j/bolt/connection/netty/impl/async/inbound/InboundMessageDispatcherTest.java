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
package org.neo4j.bolt.connection.netty.impl.async.inbound;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.DefaultChannelId;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.Attribute;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.exception.BoltFailureException;
import org.neo4j.bolt.connection.netty.impl.GqlErrorUtil;
import org.neo4j.bolt.connection.netty.impl.NoopLoggingProvider;
import org.neo4j.bolt.connection.netty.impl.logging.ChannelActivityLogger;
import org.neo4j.bolt.connection.netty.impl.logging.ChannelErrorLogger;
import org.neo4j.bolt.connection.netty.impl.messaging.Message;
import org.neo4j.bolt.connection.netty.impl.messaging.response.FailureMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.response.IgnoredMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.response.RecordMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.response.SuccessMessage;
import org.neo4j.bolt.connection.netty.impl.spi.ResponseHandler;
import org.neo4j.bolt.connection.test.values.TestValueFactory;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.bolt.connection.values.ValueFactory;

class InboundMessageDispatcherTest {
    private static final ValueFactory valueFactory = TestValueFactory.INSTANCE;
    private static final String FAILURE_CODE = "Neo.ClientError.Security.Unauthorized";
    private static final String FAILURE_MESSAGE = "Error Message";

    @Test
    void shouldFailWhenCreatedWithNullChannel() {
        assertThrows(
                NullPointerException.class, () -> new InboundMessageDispatcher(null, NoopLoggingProvider.INSTANCE));
    }

    @Test
    void shouldFailWhenCreatedWithNullLogging() {
        assertThrows(NullPointerException.class, () -> new InboundMessageDispatcher(newChannelMock(), null));
    }

    @Test
    void shouldDequeHandlerOnSuccess() {
        var dispatcher = newDispatcher();

        var handler = mock(ResponseHandler.class);
        dispatcher.enqueue(handler);
        assertEquals(1, dispatcher.queuedHandlersCount());

        Map<String, Value> metadata = new HashMap<>();
        metadata.put("key1", valueFactory.value(1));
        metadata.put("key2", valueFactory.value("2"));
        dispatcher.handleSuccessMessage(metadata);

        assertEquals(0, dispatcher.queuedHandlersCount());
        verify(handler).onSuccess(metadata);
    }

    @Test
    void shouldDequeHandlerOnFailure() {
        var channel = new EmbeddedChannel();
        var dispatcher = newDispatcher(channel);

        var handler = mock(ResponseHandler.class);
        dispatcher.enqueue(handler);
        assertEquals(1, dispatcher.queuedHandlersCount());

        dispatcher.handleFailureMessage(GqlErrorUtil.gqlError(FAILURE_CODE, FAILURE_MESSAGE));

        assertEquals(0, dispatcher.queuedHandlersCount());
        verifyFailure(handler);
    }

    @Test
    void shouldPeekHandlerOnRecord() {
        var dispatcher = newDispatcher();

        var handler = mock(ResponseHandler.class);
        dispatcher.enqueue(handler);
        assertEquals(1, dispatcher.queuedHandlersCount());

        var fields1 = List.of(valueFactory.value(1));
        var fields2 = List.of(valueFactory.value(2));
        var fields3 = List.of(valueFactory.value(3));

        dispatcher.handleRecordMessage(fields1);
        dispatcher.handleRecordMessage(fields2);
        dispatcher.handleRecordMessage(fields3);

        verify(handler).onRecord(fields1);
        verify(handler).onRecord(fields2);
        verify(handler).onRecord(fields3);
        assertEquals(1, dispatcher.queuedHandlersCount());
    }

    @Test
    void shouldFailAllHandlersOnChannelError() {
        var dispatcher = newDispatcher();

        var handler1 = mock(ResponseHandler.class);
        var handler2 = mock(ResponseHandler.class);
        var handler3 = mock(ResponseHandler.class);

        dispatcher.enqueue(handler1);
        dispatcher.enqueue(handler2);
        dispatcher.enqueue(handler3);

        var fatalError = new RuntimeException("Fatal!");
        dispatcher.handleChannelError(fatalError);

        var inOrder = inOrder(handler1, handler2, handler3);
        inOrder.verify(handler1).onFailure(fatalError);
        inOrder.verify(handler2).onFailure(fatalError);
        inOrder.verify(handler3).onFailure(fatalError);
    }

    @Test
    void shouldFailNewHandlerAfterChannelError() {
        var dispatcher = newDispatcher();

        dispatcher.handleChannelError(new RuntimeException("Fatal!"));

        var handler = mock(ResponseHandler.class);
        dispatcher.enqueue(handler);

        verify(handler).onFailure(ArgumentMatchers.any(IllegalStateException.class));
    }

    @Test
    void shouldDequeHandlerOnIgnored() {
        var dispatcher = newDispatcher();
        var handler = mock(ResponseHandler.class);

        dispatcher.enqueue(handler);
        dispatcher.handleIgnoredMessage();

        assertEquals(0, dispatcher.queuedHandlersCount());
    }

    @Test
    void shouldThrowWhenNoHandlerToHandleRecordMessage() {
        var dispatcher = newDispatcher();

        assertThrows(
                IllegalStateException.class,
                () -> dispatcher.handleRecordMessage(List.of(valueFactory.value(1), valueFactory.value(2))));
    }

    @Test
    void shouldKeepSingleAutoReadManagingHandler() {
        var dispatcher = newDispatcher();

        var handler1 = newAutoReadManagingResponseHandler();
        var handler2 = newAutoReadManagingResponseHandler();
        var handler3 = newAutoReadManagingResponseHandler();

        dispatcher.enqueue(handler1);
        dispatcher.enqueue(handler2);
        dispatcher.enqueue(handler3);

        var inOrder = inOrder(handler1, handler2, handler3);
        inOrder.verify(handler1).disableAutoReadManagement();
        inOrder.verify(handler2).disableAutoReadManagement();
        inOrder.verify(handler3, never()).disableAutoReadManagement();
    }

    @Test
    void shouldKeepTrackOfAutoReadManagingHandler() {
        var dispatcher = newDispatcher();

        var handler1 = newAutoReadManagingResponseHandler();
        var handler2 = newAutoReadManagingResponseHandler();

        assertNull(dispatcher.autoReadManagingHandler());

        dispatcher.enqueue(handler1);
        assertEquals(handler1, dispatcher.autoReadManagingHandler());

        dispatcher.enqueue(handler2);
        assertEquals(handler2, dispatcher.autoReadManagingHandler());
    }

    @Test
    void shouldForgetAutoReadManagingHandlerWhenItIsRemoved() {
        var dispatcher = newDispatcher();

        var handler1 = mock(ResponseHandler.class);
        var handler2 = mock(ResponseHandler.class);
        var handler3 = newAutoReadManagingResponseHandler();

        dispatcher.enqueue(handler1);
        dispatcher.enqueue(handler2);
        dispatcher.enqueue(handler3);
        assertEquals(handler3, dispatcher.autoReadManagingHandler());

        dispatcher.handleSuccessMessage(emptyMap());
        dispatcher.handleSuccessMessage(emptyMap());
        dispatcher.handleSuccessMessage(emptyMap());

        assertNull(dispatcher.autoReadManagingHandler());
    }

    @Test
    void shouldReEnableAutoReadWhenAutoReadManagingHandlerIsRemoved() {
        var channel = newChannelMock();
        var dispatcher = newDispatcher(channel);

        var handler = newAutoReadManagingResponseHandler();
        dispatcher.enqueue(handler);
        assertEquals(handler, dispatcher.autoReadManagingHandler());
        verify(handler, never()).disableAutoReadManagement();
        verify(channel.config(), never()).setAutoRead(anyBoolean());

        dispatcher.handleSuccessMessage(emptyMap());

        assertNull(dispatcher.autoReadManagingHandler());
        verify(handler).disableAutoReadManagement();
        verify(channel.config()).setAutoRead(anyBoolean());
    }

    @ParameterizedTest
    @ValueSource(classes = {SuccessMessage.class, FailureMessage.class, RecordMessage.class, IgnoredMessage.class})
    void shouldCreateChannelActivityLoggerAndLogDebugMessageOnMessageHandling(Class<? extends Message> message) {
        // GIVEN
        var channel = new EmbeddedChannel();
        var logging = mock(LoggingProvider.class);
        var logger = mock(System.Logger.class);
        when(logger.isLoggable(System.Logger.Level.DEBUG)).thenReturn(true);
        when(logging.getLog(InboundMessageDispatcher.class)).thenReturn(logger);
        var errorLogger = mock(ChannelErrorLogger.class);
        when(logging.getLog(ChannelErrorLogger.class)).thenReturn(errorLogger);
        var dispatcher = new InboundMessageDispatcher(channel, logging);
        var handler = mock(ResponseHandler.class);
        dispatcher.enqueue(handler);
        Runnable loggerVerification = () -> {};

        // WHEN
        if (SuccessMessage.class.isAssignableFrom(message)) {
            dispatcher.handleSuccessMessage(new HashMap<>());
            loggerVerification = () -> {
                verify(logger).isLoggable(System.Logger.Level.DEBUG);
                verify(logger)
                        .log(eq(System.Logger.Level.DEBUG), eq((ResourceBundle) null), anyString(), any(Map.class));
            };
        } else if (FailureMessage.class.isAssignableFrom(message)) {
            dispatcher.handleFailureMessage(GqlErrorUtil.gqlError(FAILURE_CODE, FAILURE_MESSAGE));
            loggerVerification = () -> {
                verify(logger).isLoggable(System.Logger.Level.DEBUG);
                verify(logger)
                        .log(
                                eq(System.Logger.Level.DEBUG),
                                eq((ResourceBundle) null),
                                anyString(),
                                anyString(),
                                anyString());
            };
        } else if (RecordMessage.class.isAssignableFrom(message)) {
            dispatcher.handleRecordMessage(List.of());
            loggerVerification = () -> {
                verify(logger).isLoggable(System.Logger.Level.DEBUG);
                verify(logger).log(eq(System.Logger.Level.DEBUG), eq((ResourceBundle) null), anyString(), anyString());
            };
        } else if (IgnoredMessage.class.isAssignableFrom(message)) {
            dispatcher.handleIgnoredMessage();
            loggerVerification = () -> {
                verify(logger).isLoggable(System.Logger.Level.DEBUG);
                verify(logger)
                        .log(eq(System.Logger.Level.DEBUG), eq((ResourceBundle) null), anyString(), eq((String) null));
            };
        } else {
            fail("Unexpected message type parameter provided");
        }

        // THEN
        assertTrue(dispatcher.getLog() instanceof ChannelActivityLogger);
        assertTrue(dispatcher.getErrorLog() instanceof ChannelErrorLogger);
        loggerVerification.run();
    }

    @Test
    void shouldCreateChannelErrorLoggerAndLogDebugMessageOnChannelError() {
        // GIVEN
        var channel = newChannelMock();
        var logging = mock(LoggingProvider.class);
        var logger = mock(System.Logger.class);
        when(logger.isLoggable(System.Logger.Level.DEBUG)).thenReturn(true);
        when(logging.getLog(InboundMessageDispatcher.class)).thenReturn(logger);
        var errorLogger = mock(ChannelErrorLogger.class);
        when(errorLogger.isLoggable(System.Logger.Level.DEBUG)).thenReturn(true);
        when(logging.getLog(ChannelErrorLogger.class)).thenReturn(errorLogger);
        var dispatcher = new InboundMessageDispatcher(channel, logging);
        var handler = mock(ResponseHandler.class);
        dispatcher.enqueue(handler);
        var throwable = mock(Throwable.class);

        // WHEN
        dispatcher.handleChannelError(throwable);

        // THEN
        assertTrue(dispatcher.getLog() instanceof ChannelActivityLogger);
        assertTrue(dispatcher.getErrorLog() instanceof ChannelErrorLogger);
        verify(errorLogger)
                .log(
                        eq(System.Logger.Level.DEBUG),
                        eq((ResourceBundle) null),
                        contains(throwable.getClass().toString()),
                        eq((String) null));
    }

    private static void verifyFailure(ResponseHandler handler) {
        verifyFailure(handler, FAILURE_CODE, FAILURE_MESSAGE, null);
    }

    @SuppressWarnings("SameParameterValue")
    private static void verifyFailure(
            ResponseHandler handler, String code, String message, Class<? extends BoltFailureException> exceptionCls) {
        var captor = ArgumentCaptor.forClass(BoltFailureException.class);
        verify(handler).onFailure(captor.capture());
        var value = captor.getValue();
        assertEquals(code, value.code());
        assertEquals(message, value.getMessage());
        if (exceptionCls != null) {
            assertEquals(exceptionCls, value.getClass());
        }
    }

    private static InboundMessageDispatcher newDispatcher() {
        return newDispatcher(newChannelMock());
    }

    private static InboundMessageDispatcher newDispatcher(Channel channel) {
        return new InboundMessageDispatcher(channel, NoopLoggingProvider.INSTANCE);
    }

    @SuppressWarnings("unchecked")
    private static Channel newChannelMock() {
        var channel = mock(Channel.class);
        when(channel.id()).thenReturn(DefaultChannelId.newInstance());
        var channelConfig = mock(ChannelConfig.class);
        when(channel.config()).thenReturn(channelConfig);
        Attribute<Object> attribute = mock(Attribute.class);
        when(channel.attr(any())).thenReturn(attribute);
        return channel;
    }

    private static ResponseHandler newAutoReadManagingResponseHandler() {
        var handler = mock(ResponseHandler.class);
        when(handler.canManageAutoRead()).thenReturn(true);
        return handler;
    }
}
