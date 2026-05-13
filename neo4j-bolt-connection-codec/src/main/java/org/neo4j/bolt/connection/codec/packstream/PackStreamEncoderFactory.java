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
package org.neo4j.bolt.connection.codec.packstream;

import java.util.Set;
import org.neo4j.bolt.connection.codec.packstream.struct.PackStreamStructureEncoder;

/**
 * A factory for {@link PackStreamEncoder}.
 * @since 12.1.0
 */
public interface PackStreamEncoderFactory {
    /**
     * Creates a new instance of {@link PackStreamEncoder}.
     * @return a new instance of encoder
     */
    default PackStreamEncoder create() {
        return create(Set.of());
    }

    /**
     * Creates a new instance of {@link PackStreamEncoder} that also has the given {@link PackStreamStructureEncoder} instances registered.
     * @param structureEncoders the structure encoders
     * @return the new instance of encoder
     */
    PackStreamEncoder create(Set<PackStreamStructureEncoder<?>> structureEncoders);
}
