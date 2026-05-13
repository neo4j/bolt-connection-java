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

import org.neo4j.bolt.connection.codec.network.ValueDecoderFactory;
import org.neo4j.bolt.connection.netty.impl.messaging.MessageFormat;
import org.neo4j.bolt.connection.netty.impl.messaging.common.CommonMessageReader;
import org.neo4j.bolt.connection.values.ValueFactory;

/**
 * Bolt message format v4.4
 */
public class MessageFormatV44 implements MessageFormat {
    private boolean dateTimeUtcEnabled;

    @Override
    public MessageFormat.Writer newWriter(ValueFactory valueFactory) {
        return new MessageWriterV44(dateTimeUtcEnabled, valueFactory);
    }

    @Override
    public MessageFormat.Reader newReader(ValueFactory valueFactory) {
        return new CommonMessageReader(
                ValueDecoderFactory.create(BoltProtocolV44.VERSION, valueFactory, dateTimeUtcEnabled), valueFactory);
    }

    @Override
    public void enableDateTimeUtc() {
        dateTimeUtcEnabled = true;
    }
}
