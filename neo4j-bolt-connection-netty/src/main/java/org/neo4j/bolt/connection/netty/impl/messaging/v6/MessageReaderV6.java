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
package org.neo4j.bolt.connection.netty.impl.messaging.v6;

import java.util.Map;
import org.neo4j.bolt.connection.GqlError;
import org.neo4j.bolt.connection.netty.impl.messaging.v57.MessageReaderV57;
import org.neo4j.bolt.connection.netty.impl.packstream.PackInput;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.bolt.connection.values.ValueFactory;

final class MessageReaderV6 extends MessageReaderV57 {
    public MessageReaderV6(PackInput input, ValueFactory valueFactory) {
        super(new ValueUnpackerV6(input, valueFactory), valueFactory);
    }

    @Override
    protected GqlError unpackGqlError(Map<String, Value> params) {
        return super.unpackGqlError(params);
    }
}
