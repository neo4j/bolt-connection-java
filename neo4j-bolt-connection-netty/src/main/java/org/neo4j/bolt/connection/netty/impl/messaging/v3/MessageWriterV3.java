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
package org.neo4j.bolt.connection.netty.impl.messaging.v3;

import java.util.Map;
import org.neo4j.bolt.connection.netty.impl.messaging.AbstractMessageWriter;
import org.neo4j.bolt.connection.netty.impl.messaging.MessageEncoder;
import org.neo4j.bolt.connection.netty.impl.messaging.common.CommonValuePacker;
import org.neo4j.bolt.connection.netty.impl.messaging.encode.BeginMessageEncoder;
import org.neo4j.bolt.connection.netty.impl.messaging.encode.CommitMessageEncoder;
import org.neo4j.bolt.connection.netty.impl.messaging.encode.DiscardAllMessageEncoder;
import org.neo4j.bolt.connection.netty.impl.messaging.encode.GoodbyeMessageEncoder;
import org.neo4j.bolt.connection.netty.impl.messaging.encode.HelloMessageEncoder;
import org.neo4j.bolt.connection.netty.impl.messaging.encode.PullAllMessageEncoder;
import org.neo4j.bolt.connection.netty.impl.messaging.encode.ResetMessageEncoder;
import org.neo4j.bolt.connection.netty.impl.messaging.encode.RollbackMessageEncoder;
import org.neo4j.bolt.connection.netty.impl.messaging.encode.RunWithMetadataMessageEncoder;
import org.neo4j.bolt.connection.netty.impl.messaging.request.BeginMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.CommitMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.DiscardAllMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.GoodbyeMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.HelloMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.PullAllMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.ResetMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.RollbackMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.RunWithMetadataMessage;
import org.neo4j.bolt.connection.netty.impl.packstream.PackOutput;
import org.neo4j.bolt.connection.values.ValueFactory;

public class MessageWriterV3 extends AbstractMessageWriter {
    public MessageWriterV3(PackOutput output, ValueFactory valueFactory) {
        super(new CommonValuePacker(output, false), buildEncoders(), valueFactory);
    }

    private static Map<Byte, MessageEncoder> buildEncoders() {
        return Map.of(
                HelloMessage.SIGNATURE, new HelloMessageEncoder(),
                GoodbyeMessage.SIGNATURE, new GoodbyeMessageEncoder(),
                RunWithMetadataMessage.SIGNATURE, new RunWithMetadataMessageEncoder(),
                DiscardAllMessage.SIGNATURE, new DiscardAllMessageEncoder(),
                PullAllMessage.SIGNATURE, new PullAllMessageEncoder(),
                BeginMessage.SIGNATURE, new BeginMessageEncoder(),
                CommitMessage.SIGNATURE, new CommitMessageEncoder(),
                RollbackMessage.SIGNATURE, new RollbackMessageEncoder(),
                ResetMessage.SIGNATURE, new ResetMessageEncoder());
    }
}
