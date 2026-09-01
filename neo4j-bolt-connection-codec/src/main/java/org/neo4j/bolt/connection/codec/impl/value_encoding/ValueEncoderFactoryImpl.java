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

import java.util.Objects;
import java.util.ServiceLoader;
import org.neo4j.bolt.connection.codec.value_encoding.ValueEncoder;
import org.neo4j.bolt.connection.codec.value_encoding.ValueEncoderFactory;
import org.neo4j.bolt.connection.codec.value_encoding.ValueEncodingSchemeVersion;
import org.neo4j.bolt.connection.exception.BoltClientException;

public final class ValueEncoderFactoryImpl implements ValueEncoderFactory {
    /**
     * Creates a new instance of this factory.
     * <p>
     * It is used by {@link ServiceLoader}.
     */
    public ValueEncoderFactoryImpl() {}

    @Override
    public ValueEncoder create(ValueEncodingSchemeVersion encodingVersion) {
        Objects.requireNonNull(encodingVersion);
        if (ValueEncodingSchemeVersion.V1_0.equals(encodingVersion)) {
            return new ValueEncoderV1();
        } else {
            throw new BoltClientException("Unsupported serialization version: " + encodingVersion);
        }
    }
}
