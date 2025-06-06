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
/**
 * The Neo4j Bolt Connection Netty implementation module.
 */
@SuppressWarnings("requires-automatic")
module org.neo4j.bolt.connection.netty {
    provides org.neo4j.bolt.connection.BoltConnectionProviderFactory with
            org.neo4j.bolt.connection.netty.NettyBoltConnectionProviderFactory;

    exports org.neo4j.bolt.connection.netty;

    requires org.neo4j.bolt.connection;
    requires io.netty.common;
    requires io.netty.handler;
    requires io.netty.transport;
    requires io.netty.buffer;
    requires io.netty.codec;
    requires io.netty.resolver;
}
