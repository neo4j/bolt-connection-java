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
import org.neo4j.bolt.connection.codec.WriteOutputs;
import org.neo4j.bolt.connection.codec.impl.ValuePackerV61;
import org.neo4j.bolt.connection.codec.impl.network.WriteOutputAdapter;
import org.neo4j.bolt.connection.codec.value_encoding.ValueEncoder;
import org.neo4j.bolt.connection.codec.value_encoding.ValueEncodingSchemeVersion;
import org.neo4j.bolt.connection.values.Value;

final class ValueEncoderV1 implements ValueEncoder {

    @Override
    public Encoded encode(Value value) throws IOException {
        var output = WriteOutputs.bytes();
        var packer = new ValuePackerV61(new WriteOutputAdapter(output));
        packer.pack(value);
        return new Encoded(output.output(), ValueEncodingSchemeVersion.V1_0);
    }
}
