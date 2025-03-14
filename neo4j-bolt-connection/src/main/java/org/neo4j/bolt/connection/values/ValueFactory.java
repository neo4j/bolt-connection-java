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
package org.neo4j.bolt.connection.values;

import java.time.DateTimeException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface ValueFactory {
    Value value(Object value);

    Node node(long id, String elementId, Collection<String> labels, Map<String, Value> properties);

    Relationship relationship(
            long id,
            String elementId,
            long start,
            String startElementId,
            long end,
            String endElementId,
            String type,
            Map<String, Value> properties);

    Segment segment(Node start, Relationship relationship, Node end);

    Path path(List<Segment> segments, List<Node> nodes, List<Relationship> relationships);

    Value isoDuration(long months, long days, long seconds, int nanoseconds);

    Value point(int srid, double x, double y);

    Value point(int srid, double x, double y, double z);

    Value unsupportedDateTimeValue(DateTimeException e);
}
