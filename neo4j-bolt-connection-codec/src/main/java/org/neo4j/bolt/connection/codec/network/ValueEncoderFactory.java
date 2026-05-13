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
package org.neo4j.bolt.connection.codec.network;

import java.util.Objects;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.codec.impl.network.BoltProtocolVersions;
import org.neo4j.bolt.connection.codec.impl.network.ValueEncoderImpl;
import org.neo4j.bolt.connection.codec.impl.network.ValueEncoderImplV6;
import org.neo4j.bolt.connection.codec.impl.network.ValueEncoderImplV61;

public final class ValueEncoderFactory {
    private ValueEncoderFactory() {}

    public static ValueEncoder create(BoltProtocolVersion version, boolean dateTimeUtcEnabled) {
        Objects.requireNonNull(version);
        if (BoltProtocolVersions.V3_0_TO_V4_2.contains(version)) {
            return new ValueEncoderImpl(false);
        } else if (BoltProtocolVersions.V4_3_TO_V4_4.contains(version)) {
            return new ValueEncoderImpl(dateTimeUtcEnabled);
        } else if (BoltProtocolVersions.V5_0_TO_V5_8.contains(version)) {
            return new ValueEncoderImpl(true);
        } else if (BoltProtocolVersions.V6_0.equals(version)) {
            return new ValueEncoderImplV6();
        } else if (BoltProtocolVersions.V6_1.equals(version)) {
            return new ValueEncoderImplV61();
        }
        throw new IllegalArgumentException("Unknown version: " + version);
    }

    public static ValueEncoder create(BoltProtocolVersion version) {
        return create(version, true);
    }
}
