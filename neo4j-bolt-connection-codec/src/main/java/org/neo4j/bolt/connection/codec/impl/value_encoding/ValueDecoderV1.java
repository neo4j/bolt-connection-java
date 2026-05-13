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
package org.neo4j.bolt.connection.codec.impl.value_encoding;

import java.io.IOException;
import java.util.Objects;
import org.neo4j.bolt.connection.codec.impl.ValueUnpackerV61;
import org.neo4j.bolt.connection.codec.value_encoding.EncodingVersion;
import org.neo4j.bolt.connection.codec.value_encoding.ValueDecoder;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.bolt.connection.values.ValueFactory;

final class ValueDecoderV1 implements ValueDecoder {
    private final ValueFactory valueFactory;

    public ValueDecoderV1(ValueFactory valueFactory) {
        this.valueFactory = Objects.requireNonNull(valueFactory);
    }

    @Override
    public Value decode(byte[] bytes) throws IOException {
        return new ValueUnpackerV61(new NonChunkedPackInput(bytes), valueFactory).unpack();
    }

    @Override
    public EncodingVersion serializationVersion() {
        return EncodingVersion.V1_0;
    }
}
