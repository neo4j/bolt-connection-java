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

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.neo4j.bolt.connection.codec.WriteOutput;
import org.neo4j.bolt.connection.codec.network.ValueEncoder;
import org.neo4j.bolt.connection.codec.network.ValueEncoderFactory;
import org.neo4j.bolt.connection.netty.impl.messaging.AbstractMessageWriter;
import org.neo4j.bolt.connection.netty.impl.messaging.MessageEncoder;
import org.neo4j.bolt.connection.netty.impl.messaging.encode.DiscardAllMessageEncoder;
import org.neo4j.bolt.connection.netty.impl.messaging.encode.PullAllMessageEncoder;
import org.neo4j.bolt.connection.netty.impl.messaging.encode.ResetMessageEncoder;
import org.neo4j.bolt.connection.netty.impl.messaging.request.DiscardAllMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.PullAllMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.request.ResetMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.response.FailureMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.response.IgnoredMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.response.RecordMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.response.SuccessMessage;
import org.neo4j.bolt.connection.netty.impl.messaging.v3.MessageFormatV3;
import org.neo4j.bolt.connection.netty.impl.messaging.v44.BoltProtocolV44;
import org.neo4j.bolt.connection.test.values.TestNode;
import org.neo4j.bolt.connection.test.values.TestPath;
import org.neo4j.bolt.connection.test.values.TestRelationship;
import org.neo4j.bolt.connection.test.values.TestValue;
import org.neo4j.bolt.connection.test.values.TestValueFactory;
import org.neo4j.bolt.connection.test.values.impl.Entity;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.bolt.connection.values.ValueFactory;

/**
 * This class provides the missing server side packing methods to serialize Node, Relationship and Path. It also allows writing of server side messages like
 * SUCCESS, FAILURE, IGNORED and RECORD.
 */
public class KnowledgeableMessageFormat extends MessageFormatV3 {
    private final boolean elementIdEnabled;
    private boolean dateTimeUtcEnabled;

    public KnowledgeableMessageFormat(boolean elementIdEnabled) {
        this.elementIdEnabled = elementIdEnabled;
    }

    @Override
    public Writer newWriter(ValueFactory valueFactory) {
        return new KnowledgeableMessageWriter(elementIdEnabled, dateTimeUtcEnabled, (TestValueFactory) valueFactory);
    }

    @Override
    public void enableDateTimeUtc() {
        dateTimeUtcEnabled = true;
    }

    private static class KnowledgeableMessageWriter extends AbstractMessageWriter {
        KnowledgeableMessageWriter(boolean enableElementId, boolean dateTimeUtcEnabled, TestValueFactory valueFactory) {
            super(new KnowledgeableValueEncoder(enableElementId, dateTimeUtcEnabled), buildEncoders(), valueFactory);
        }

        static Map<Byte, MessageEncoder> buildEncoders() {
            Map<Byte, MessageEncoder> result = new HashMap<>(10);
            // request message encoders
            result.put(DiscardAllMessage.SIGNATURE, new DiscardAllMessageEncoder());
            result.put(PullAllMessage.SIGNATURE, new PullAllMessageEncoder());
            result.put(ResetMessage.SIGNATURE, new ResetMessageEncoder());
            // response message encoders
            result.put(FailureMessage.SIGNATURE, new FailureMessageEncoder());
            result.put(IgnoredMessage.SIGNATURE, new IgnoredMessageEncoder());
            result.put(RecordMessage.SIGNATURE, new RecordMessageEncoder());
            result.put(SuccessMessage.SIGNATURE, new SuccessMessageEncoder());
            return result;
        }
    }

    private static class KnowledgeableValueEncoder implements ValueEncoder {
        private final ValueEncoder delegate;
        private final boolean elementIdEnabled;

        KnowledgeableValueEncoder(boolean elementIdEnabled, boolean dateTimeUtcEnabled) {
            this.elementIdEnabled = elementIdEnabled;
            this.delegate = ValueEncoderFactory.create(BoltProtocolV44.VERSION, dateTimeUtcEnabled);
        }

        @Override
        public void encodeStructHeader(int size, byte signature, WriteOutput<?> output) throws IOException {
            delegate.encodeStructHeader(size, signature, output);
        }

        @Override
        public void packListHeader(int size, WriteOutput<?> output) throws IOException {
            delegate.packListHeader(size, output);
        }

        @Override
        public void encode(String string, WriteOutput<?> output) throws IOException {
            delegate.encode(string, output);
        }

        @Override
        public void encode(Value value, WriteOutput<?> output) throws IOException {
            switch (value.boltValueType()) {
                case LIST -> {
                    delegate.packListHeader(value.size(), output);
                    for (var item : value.boltValues()) {
                        encode(item, output);
                    }
                }
                case NODE -> {
                    var node = ((TestValue) value).asNode();
                    packNode(node, output);
                }
                case RELATIONSHIP -> {
                    var rel = ((TestValue) value).asRelationship();
                    packRelationship(rel, output);
                }
                case PATH -> {
                    var path = ((TestValue) value).asPath();
                    packPath(path, output);
                }
                default -> delegate.encode(value, output);
            }
        }

        @Override
        public void encode(Map<String, Value> map, WriteOutput<?> output) throws IOException {
            delegate.encode(map, output);
        }

        private void packRelationship(TestRelationship rel, WriteOutput<?> output) throws IOException {
            var valueFactory = TestValueFactory.INSTANCE;
            delegate.encodeStructHeader(elementIdEnabled ? 8 : 5, (byte) 'R', output);
            delegate.encode(valueFactory.value(rel.id()), output);
            delegate.encode(valueFactory.value(rel.startNodeId()), output);
            delegate.encode(valueFactory.value(rel.endNodeId()), output);

            delegate.encode(rel.typeString(), output);

            packProperties(rel, output);

            if (elementIdEnabled) {
                delegate.encode(rel.elementId(), output);
                delegate.encode(rel.startNodeElementId(), output);
                delegate.encode(rel.endNodeElementId(), output);
            }
        }

        private void packNode(TestNode node, WriteOutput<?> output) throws IOException {
            var valueFactory = TestValueFactory.INSTANCE;
            delegate.encodeStructHeader(elementIdEnabled ? 4 : 3, (byte) 'N', output);
            delegate.encode(valueFactory.value(node.id()), output);

            var labels = node.labels();
            delegate.encode(valueFactory.value(labels), output);

            packProperties(node, output);

            if (elementIdEnabled) {
                delegate.encode(node.elementId(), output);
            }
        }

        private void packProperties(Entity entity, WriteOutput<?> output) throws IOException {
            var valueFactory = TestValueFactory.INSTANCE;
            delegate.encode(valueFactory.value(entity.asMap(valueFactory::value)), output);
        }

        private void packPath(TestPath path, WriteOutput<?> output) throws IOException {
            var valueFactory = TestValueFactory.INSTANCE;
            delegate.encodeStructHeader(3, (byte) 'P', output);

            // Unique nodes
            Map<TestNode, Integer> nodeIdx = new LinkedHashMap<>(path.length() + 1);
            for (var node : path.nodes()) {
                if (!nodeIdx.containsKey(node)) {
                    nodeIdx.put(node, nodeIdx.size());
                }
            }
            delegate.packListHeader(nodeIdx.size(), output);
            for (var node : nodeIdx.keySet()) {
                packNode(node, output);
            }

            // Unique rels
            Map<TestRelationship, Integer> relIdx = new LinkedHashMap<>(path.length());
            for (var rel : path.relationships()) {
                if (!relIdx.containsKey(rel)) {
                    relIdx.put(rel, relIdx.size() + 1);
                }
            }
            delegate.packListHeader(relIdx.size(), output);
            for (var rel : relIdx.keySet()) {
                delegate.encodeStructHeader(elementIdEnabled ? 4 : 3, (byte) 'r', output);
                delegate.encode(valueFactory.value(rel.id()), output);
                delegate.encode(rel.typeString(), output);
                packProperties(rel, output);
                if (elementIdEnabled) {
                    delegate.encode(rel.elementId(), output);
                }
            }

            // Sequence
            delegate.packListHeader(path.length() * 2, output);
            for (var seg : path) {
                var rel = seg.relationship();
                var relEndId = rel.endNodeId();
                var segEndId = seg.end().id();
                var size = relEndId == segEndId ? relIdx.get(rel) : -relIdx.get(rel);
                delegate.encode(valueFactory.value(size), output);
                delegate.encode(valueFactory.value(nodeIdx.get(seg.end())), output);
            }
        }
    }
}
