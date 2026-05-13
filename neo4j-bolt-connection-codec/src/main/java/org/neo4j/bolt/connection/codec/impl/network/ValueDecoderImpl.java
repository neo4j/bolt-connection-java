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
package org.neo4j.bolt.connection.codec.impl.network;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.neo4j.bolt.connection.codec.ReadInput;
import org.neo4j.bolt.connection.codec.impl.CommonValueUnpacker;
import org.neo4j.bolt.connection.codec.impl.ValueUnpacker;
import org.neo4j.bolt.connection.codec.network.ValueDecoder;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.bolt.connection.values.ValueFactory;

public class ValueDecoderImpl implements ValueDecoder {
    protected final ValueFactory valueFactory;
    private final boolean dateTimeUtcEnabled;

    public ValueDecoderImpl(ValueFactory valueFactory, boolean dateTimeUtcEnabled) {
        this.valueFactory = Objects.requireNonNull(valueFactory);
        this.dateTimeUtcEnabled = dateTimeUtcEnabled;
    }

    @Override
    public long unpackStructHeader(ReadInput input) throws IOException {
        return valueUnpacker(input).unpackStructHeader();
    }

    @Override
    public int unpackStructSignature(ReadInput input) throws IOException {
        return valueUnpacker(input).unpackStructSignature();
    }

    @Override
    public Map<String, Value> unpackMap(ReadInput input) throws IOException {
        return valueUnpacker(input).unpackMap();
    }

    @Override
    public List<Value> unpackList(ReadInput input) throws IOException {
        return valueUnpacker(input).unpackList();
    }

    protected ValueUnpacker valueUnpacker(ReadInput input) {
        return new CommonValueUnpacker(new ReadInputAdapter(input), dateTimeUtcEnabled, valueFactory);
    }
}
