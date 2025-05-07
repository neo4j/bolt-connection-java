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
package org.neo4j.bolt.connection;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.neo4j.bolt.connection.message.Message;

public interface BoltConnection {
    default CompletionStage<Void> writeAndFlush(ResponseHandler handler, Message messages) {
        return writeAndFlush(handler, List.of(messages));
    }

    CompletionStage<Void> writeAndFlush(ResponseHandler handler, List<Message> messages);

    default CompletionStage<Void> write(Message messages) {
        return write(List.of(messages));
    }

    CompletionStage<Void> write(List<Message> messages);

    CompletionStage<Void> forceClose(String reason);

    CompletionStage<Void> close();

    // ----- STATE UPDATES -----

    CompletionStage<Void> setReadTimeout(Duration duration);

    // ----- MUTABLE DATA -----

    BoltConnectionState state();

    CompletionStage<AuthInfo> authInfo();

    // ----- IMMUTABLE DATA -----

    String serverAgent();

    BoltServerAddress serverAddress();

    BoltProtocolVersion protocolVersion();

    boolean telemetrySupported();

    boolean serverSideRoutingEnabled();

    Optional<Duration> defaultReadTimeout();
}
