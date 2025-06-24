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
package org.neo4j.bolt.connection.netty.impl.messaging.request;

/**
 * PULL_ALL request message
 * <p>
 * Sent by clients to pull the entirety of the remaining stream down.
 */
public class PullAllMessage implements RequestMessage {
    public static final byte SIGNATURE = 0x3F;
    private static final String NAME = "PULL_ALL";

    public static final PullAllMessage PULL_ALL = new PullAllMessage();

    private PullAllMessage() {}

    @Override
    public byte signature() {
        return SIGNATURE;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String toString() {
        return NAME;
    }
}
