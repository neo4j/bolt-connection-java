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
package org.neo4j.bolt.connection.netty.impl.messaging.v5;

import org.neo4j.bolt.connection.netty.impl.messaging.ValueUnpacker;
import org.neo4j.bolt.connection.netty.impl.messaging.common.CommonMessageReader;
import org.neo4j.bolt.connection.netty.impl.packstream.PackInput;
import org.neo4j.bolt.connection.values.ValueFactory;

public class MessageReaderV5 extends CommonMessageReader {
    public MessageReaderV5(PackInput input, ValueFactory valueFactory) {
        super(new ValueUnpackerV5(input, valueFactory), valueFactory);
    }

    protected MessageReaderV5(ValueUnpacker unpacker, ValueFactory valueFactory) {
        super(unpacker, valueFactory);
    }
}
