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
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface ValueFactory {
    Value value(Object value);

    default Value value(boolean value) {
        return value((Object) value);
    }

    default Value value(long value) {
        return value((Object) value);
    }

    default Value value(double value) {
        return value((Object) value);
    }

    default Value value(byte[] values) {
        return value((Object) values);
    }

    default Value value(String value) {
        return value((Object) value);
    }

    default Value value(Map<String, Value> stringToValue) {
        return value((Object) stringToValue);
    }

    default Value value(Value[] values) {
        return value((Object) values);
    }

    default Value value(Node node) {
        return value((Object) node);
    }

    default Value value(Relationship relationship) {
        return value((Object) relationship);
    }

    default Value value(Path path) {
        return value((Object) path);
    }

    default Value value(LocalDate localDate) {
        return value((Object) localDate);
    }

    default Value value(OffsetTime offsetTime) {
        return value((Object) offsetTime);
    }

    default Value value(LocalTime localTime) {
        return value((Object) localTime);
    }

    default Value value(LocalDateTime localDateTime) {
        return value((Object) localDateTime);
    }

    default Value value(OffsetDateTime offsetDateTime) {
        return value((Object) offsetDateTime);
    }

    default Value value(ZonedDateTime zonedDateTime) {
        return value((Object) zonedDateTime);
    }

    default Value value(Period period) {
        return value((Object) period);
    }

    default Value value(Duration duration) {
        return value((Object) duration);
    }

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
