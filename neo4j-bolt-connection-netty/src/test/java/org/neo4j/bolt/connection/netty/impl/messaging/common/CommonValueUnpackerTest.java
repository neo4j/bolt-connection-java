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
package org.neo4j.bolt.connection.netty.impl.messaging.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;

final class CommonValueUnpackerTest {
    @Test
    void shouldMakeZonedDateTimeUsingUtcBaseline() {
        var unpacker = new CommonValueUnpacker(mock(), true, mock());
        var epochSecondLocal = 1761442200L;
        var nano = 0;
        var zoneId = ZoneId.of("Europe/Stockholm");

        var date = unpacker.newZonedDateTimeUsingUtcBaseline(epochSecondLocal, nano, zoneId);

        assertEquals(ZonedDateTime.parse("2025-10-26T02:30:00+01:00[Europe/Stockholm]"), date);
    }
}
