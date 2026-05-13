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
package org.neo4j.bolt.connection.netty.impl.messaging.common;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import org.neo4j.bolt.connection.GqlError;
import org.neo4j.bolt.connection.GqlStatusError;
import org.neo4j.bolt.connection.codec.ReadInput;
import org.neo4j.bolt.connection.codec.network.ValueDecoder;
import org.neo4j.bolt.connection.netty.impl.messaging.MessageFormat;
import org.neo4j.bolt.connection.netty.impl.messaging.ResponseMessageHandler;
import org.neo4j.bolt.connection.netty.impl.messaging.response.FailureMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.response.IgnoredMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.response.RecordMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.response.SuccessMessage;
import org.neo4j.bolt.connection.values.ValueFactory;

public class CommonMessageReader implements MessageFormat.Reader {
    protected final ValueDecoder reader;
    protected final ValueFactory valueFactory;

    public CommonMessageReader(ValueDecoder reader, ValueFactory valueFactory) {
        this.reader = reader;
        this.valueFactory = Objects.requireNonNull(valueFactory);
    }

    @Override
    public void read(ResponseMessageHandler handler, ReadInput input) throws IOException {
        reader.unpackStructHeader(input);
        var type = reader.unpackStructSignature(input);
        switch (type) {
            case SuccessMessage.SIGNATURE -> unpackSuccessMessage(handler, input);
            case FailureMessage.SIGNATURE -> unpackFailureMessage(handler, input);
            case IgnoredMessage.SIGNATURE -> unpackIgnoredMessage(handler);
            case RecordMessage.SIGNATURE -> unpackRecordMessage(handler, input);
            default -> throw new IOException("Unknown message type: " + type);
        }
    }

    private void unpackSuccessMessage(ResponseMessageHandler output, ReadInput input) throws IOException {
        var map = reader.unpackMap(input);
        output.handleSuccessMessage(map);
    }

    protected void unpackFailureMessage(ResponseMessageHandler output, ReadInput input) throws IOException {
        var params = reader.unpackMap(input);
        var code = params.get("code").asString();
        var message = params.get("message").asString();
        var diagnosticRecord = Map.ofEntries(
                Map.entry("CURRENT_SCHEMA", valueFactory.value("/")),
                Map.entry("OPERATION", valueFactory.value("")),
                Map.entry("OPERATION_CODE", valueFactory.value("0")));
        var gqlError = new GqlError(
                GqlStatusError.UNKNOWN.getStatus(),
                GqlStatusError.UNKNOWN.getStatusDescription(message),
                code,
                message,
                diagnosticRecord,
                null);
        output.handleFailureMessage(gqlError);
    }

    private void unpackIgnoredMessage(ResponseMessageHandler output) {
        output.handleIgnoredMessage();
    }

    private void unpackRecordMessage(ResponseMessageHandler output, ReadInput input) throws IOException {
        var fields = reader.unpackList(input);
        output.handleRecordMessage(fields);
    }
}
