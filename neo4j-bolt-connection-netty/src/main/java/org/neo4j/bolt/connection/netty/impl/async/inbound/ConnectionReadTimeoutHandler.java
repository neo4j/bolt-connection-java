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
package org.neo4j.bolt.connection.netty.impl.async.inbound;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.ReadTimeoutHandler;
import java.util.concurrent.TimeUnit;
import org.neo4j.bolt.connection.exception.BoltConnectionReadTimeoutException;

public class ConnectionReadTimeoutHandler extends ReadTimeoutHandler {
    private boolean triggered;

    public ConnectionReadTimeoutHandler(long timeout, TimeUnit unit) {
        super(timeout, unit);
    }

    @Override
    protected void readTimedOut(ChannelHandlerContext ctx) {
        if (!triggered) {
            ctx.fireExceptionCaught(
                    new BoltConnectionReadTimeoutException(
                            "Connection read timed out due to it taking longer than the server-supplied timeout value via configuration hint."));
            ctx.close();
            triggered = true;
        }
    }
}
