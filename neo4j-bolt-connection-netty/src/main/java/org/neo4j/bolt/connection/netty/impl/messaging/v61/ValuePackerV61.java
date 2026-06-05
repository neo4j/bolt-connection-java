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
package org.neo4j.bolt.connection.netty.impl.messaging.v61;

import java.io.IOException;
import java.util.UUID;
import org.neo4j.bolt.connection.netty.impl.messaging.v6.ValuePackerV6;
import org.neo4j.bolt.connection.netty.impl.packstream.PackOutput;

final class ValuePackerV61 extends ValuePackerV6 {
    public ValuePackerV61(PackOutput output) {
        super(output);
    }

    @Override
    protected void packUUID(UUID uuid) throws IOException {
        packer.pack(uuid);
    }
}
