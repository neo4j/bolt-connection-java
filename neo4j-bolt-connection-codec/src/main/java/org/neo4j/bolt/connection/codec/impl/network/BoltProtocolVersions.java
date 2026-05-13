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

import java.util.Set;
import org.neo4j.bolt.connection.BoltProtocolVersion;

public final class BoltProtocolVersions {
    public static final BoltProtocolVersion V3_0 = new BoltProtocolVersion(3, 0);
    public static final BoltProtocolVersion V4_0 = new BoltProtocolVersion(4, 0);
    public static final BoltProtocolVersion V4_1 = new BoltProtocolVersion(4, 1);
    public static final BoltProtocolVersion V4_2 = new BoltProtocolVersion(4, 2);
    public static final BoltProtocolVersion V4_3 = new BoltProtocolVersion(4, 3);
    public static final BoltProtocolVersion V4_4 = new BoltProtocolVersion(4, 4);
    public static final BoltProtocolVersion V5_0 = new BoltProtocolVersion(5, 0);
    public static final BoltProtocolVersion V5_1 = new BoltProtocolVersion(5, 1);
    public static final BoltProtocolVersion V5_2 = new BoltProtocolVersion(5, 2);
    public static final BoltProtocolVersion V5_3 = new BoltProtocolVersion(5, 3);
    public static final BoltProtocolVersion V5_4 = new BoltProtocolVersion(5, 4);
    public static final BoltProtocolVersion V5_5 = new BoltProtocolVersion(5, 5);
    public static final BoltProtocolVersion V5_6 = new BoltProtocolVersion(5, 6);
    public static final BoltProtocolVersion V5_7 = new BoltProtocolVersion(5, 7);
    public static final BoltProtocolVersion V5_8 = new BoltProtocolVersion(5, 8);
    public static final BoltProtocolVersion V6_0 = new BoltProtocolVersion(6, 0);
    public static final BoltProtocolVersion V6_1 = new BoltProtocolVersion(6, 1);

    public static final Set<BoltProtocolVersion> V3_0_TO_V4_2 = Set.of(V3_0, V4_0, V4_1, V4_2);
    public static final Set<BoltProtocolVersion> V4_3_TO_V4_4 = Set.of(V4_3, V4_4);
    public static final Set<BoltProtocolVersion> V5_0_TO_V5_8 = Set.of(V5_0, V5_1, V5_2, V5_3, V5_4, V5_6, V5_7, V5_8);
}
