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

import static org.neo4j.bolt.connection.netty.impl.util.Preconditions.checkArgument;

import java.io.IOException;
import org.neo4j.bolt.connection.netty.impl.messaging.Message;
import org.neo4j.bolt.connection.netty.impl.messaging.MessageEncoder;
import org.neo4j.bolt.connection.netty.impl.messaging.ValuePacker;
import org.neo4j.bolt.connection.netty.impl.messaging.request.BeginMessage;
import org.neo4j.bolt.connection.values.ValueFactory;

public class BeginMessageEncoder implements MessageEncoder {
    @Override
    public void encode(Message message, ValuePacker packer, ValueFactory valueFactory) throws IOException {
        checkArgument(message, BeginMessage.class);
        var beginMessage = (BeginMessage) message;
        packer.packStructHeader(1, beginMessage.signature());
        packer.pack(beginMessage.metadata());
    }
}
