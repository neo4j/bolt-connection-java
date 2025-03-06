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
package org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.encode;

import static org.neo4j.driver.internal.bolt.basicimpl.impl.util.Preconditions.checkArgument;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.neo4j.driver.internal.bolt.api.values.Value;
import org.neo4j.driver.internal.bolt.api.values.ValueFactory;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.Message;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.MessageEncoder;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.ValuePacker;
import org.neo4j.driver.internal.bolt.basicimpl.impl.messaging.request.RouteMessage;

/**
 * Encodes the ROUTE message to the stream
 */
public class RouteV44MessageEncoder implements MessageEncoder {
    @Override
    public void encode(Message message, ValuePacker packer, ValueFactory valueFactory) throws IOException {
        checkArgument(message, RouteMessage.class);
        var routeMessage = (RouteMessage) message;
        packer.packStructHeader(3, message.signature());
        packer.pack(routeMessage.routingContext());
        packer.pack(valueFactory.value(routeMessage.bookmarks()));

        Map<String, Value> params;
        if (routeMessage.impersonatedUser() != null && routeMessage.databaseName() == null) {
            params = Collections.singletonMap("imp_user", valueFactory.value(routeMessage.impersonatedUser()));
        } else if (routeMessage.databaseName() != null) {
            params = Collections.singletonMap("db", valueFactory.value(routeMessage.databaseName()));
        } else {
            params = Collections.emptyMap();
        }
        packer.pack(params);
    }
}
