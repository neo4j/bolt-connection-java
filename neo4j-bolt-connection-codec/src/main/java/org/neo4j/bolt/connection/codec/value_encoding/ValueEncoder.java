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
package org.neo4j.bolt.connection.codec.value_encoding;

import java.io.IOException;
import java.util.Objects;
import org.neo4j.bolt.connection.values.Value;

/**
 * An encoder implementing Bolt Value Encoding Scheme.
 * @since 12.1.0
 */
public interface ValueEncoder {
    /**
     * Encodes the given Neo4j Value.
     * @param value the value to encode
     * @return the encoded value
     * @throws IOException if I/O error occurs
     */
    Encoded encode(Value value) throws IOException;

    /**
     * An encoded value.
     * @param bytes the encoded bytes
     * @param baseVersion the base version of Bolt Value Encoding Scheme
     */
    record Encoded(byte[] bytes, ValueEncodingSchemeVersion baseVersion) {
        public Encoded {
            Objects.requireNonNull(bytes);
            Objects.requireNonNull(baseVersion);
        }
    }
}
