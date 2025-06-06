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
package org.neo4j.bolt.connection.query_api.impl;

import java.time.LocalDate;
import java.time.OffsetTime;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.neo4j.bolt.connection.values.Type;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.bolt.connection.values.ValueFactory;

enum CypherTypes {
    // spotless:off
    Null(
        Type.NULL,
        (v, i) -> v.value((Object) null),
        (i) -> null
    ),

    List(
        Type.LIST,
        null, // manually handled in JSON converter
        (v) -> v.values()
    ),

    Map(
        Type.MAP,
        null, // manually handled in JSON converter
        (v) -> v.asMap(Function.identity())
    ),

    Boolean(
        Type.BOOLEAN,
        (v, i) -> v.value(java.lang.Boolean.parseBoolean(i)),
        Value::asBoolean
    ),

    Integer(
        Type.INTEGER,
        (v, i) -> v.value(java.lang.Long.parseLong(i)),
        Value::asLong
    ),

    Float(
        Type.FLOAT,
        (v, i) -> v.value(Double.parseDouble(i)),
        Value::asDouble
    ),

    String(
        Type.STRING,
        ValueFactory::value,
        Value::asString
    ),

    Base64(Type.BYTES,
        (v, i) -> v.value(java.util.Base64.getDecoder().decode(i)),
        v -> java.util.Base64.getEncoder().encodeToString(v.asByteArray())
    ),

    Date(
        Type.DATE,
        (v, i) -> v.value(LocalDate.parse(i, DateTimeFormatter.ISO_LOCAL_DATE)),
        v -> DateTimeFormatter.ISO_LOCAL_DATE.format(v.asLocalDate())
    ),

    Time(
        Type.TIME,
        (v, i) -> v.value(OffsetTime.parse(i, DateTimeFormatter.ISO_OFFSET_TIME)),
        v -> DateTimeFormatter.ISO_OFFSET_TIME.format(v.asOffsetTime())
    ),

    LocalTime(
        Type.LOCAL_TIME,
        (v, i) -> v.value(java.time.LocalTime.parse(i, DateTimeFormatter.ISO_LOCAL_TIME)),
        v -> DateTimeFormatter.ISO_LOCAL_TIME.format(v.asLocalTime())
    ),

    DateTime(
            Type.DATE_TIME,
            (v, i) -> v.value(java.time.ZonedDateTime.parse(i, DateTimeFormatter.ISO_ZONED_DATE_TIME)),
            v -> DateTimeFormatter.ISO_ZONED_DATE_TIME.format(v.asZonedDateTime())
    ),

    OffsetDateTime(
            Type.DATE_TIME,
            (v, i) -> v.value(java.time.OffsetDateTime.parse(i, DateTimeFormatter.ISO_OFFSET_DATE_TIME)),
            v -> DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(v.asZonedDateTime())
    ),

    ZonedDateTime(
            Type.DATE_TIME,
            (v, i) -> v.value(java.time.ZonedDateTime.parse(i, DateTimeFormatter.ISO_ZONED_DATE_TIME)),
            v -> DateTimeFormatter.ISO_ZONED_DATE_TIME.format(v.asZonedDateTime())
    ),

    LocalDateTime(
            Type.LOCAL_DATE_TIME,
            (v, i) -> v.value(java.time.LocalDateTime.parse(i, DateTimeFormatter.ISO_LOCAL_DATE_TIME)),
            v -> DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(v.asLocalDateTime())
    ),

    Duration(
        Type.DURATION,
        CypherTypes::parseDuration,
        (v) -> v.asIsoDuration().toString()
    ),

    Point(
        Type.POINT,
        CypherTypes::parsePoint,
        CypherTypes::writePoint
    ),

    Node(
        Type.NODE,
        null, // handled in DriverValueProvider
        CypherTypes::unsupported
    ),

    Relationship(
        Type.RELATIONSHIP,
        null, // handled in DriverValueProvider
        CypherTypes::unsupported
    ),

    Path(
        Type.PATH,
        null, // handled in DriverValueProvider
        CypherTypes::unsupported);

    // spotless:on
    private final BiFunction<ValueFactory, String, Value> reader;
    private final Function<Value, Object> writer;
    private final Type type;

    CypherTypes(Type type, BiFunction<ValueFactory, String, Value> reader, Function<Value, Object> writer) {
        this.type = type;
        this.reader = reader;
        this.writer = writer;
    }

    public static CypherTypes typeFromValue(Value value) {
        var valueType = value.type();
        for (CypherTypes cypherType : values()) {
            if (cypherType.type == valueType) {
                return cypherType;
            }
        }

        throw new IllegalArgumentException("no Cypher type found representing " + value.type());
    }

    /**
     * {@return optional reader if this type can be read directly}
     */
    public BiFunction<ValueFactory, String, Value> getReader() {
        return reader;
    }

    /**
     * {@return optional writer if this type can be written directly}
     */
    public Function<Value, Object> getWriter() {
        return writer;
    }

    private static final Pattern WKT_PATTERN =
            Pattern.compile("SRID=(\\d+);\\s*POINT\\s?Z?\\s?\\(\\s*(\\S+)\\s+(\\S+)\\s*(\\S*)\\)");

    private static Value parsePoint(ValueFactory valueFactory, String input) {
        Matcher matcher = WKT_PATTERN.matcher(input);

        if (!matcher.matches()) {
            throw new IllegalArgumentException("Illegal pattern");
        }

        int srid = java.lang.Integer.parseInt(matcher.group(1));
        double x = Double.parseDouble(matcher.group(2));
        double y = Double.parseDouble(matcher.group(3));
        String z = matcher.group(4);
        if (z != null && !z.trim().isEmpty()) {
            return valueFactory.point(srid, x, y, Double.parseDouble(z));
        } else {
            return valueFactory.point(srid, x, y);
        }
    }

    private static String writePoint(Value value) {
        if (value.type() == Type.POINT) {
            var point = value.asPoint();
            var srid = point.srid();
            String pointArguments;
            if (Double.isNaN(point.z())) {
                pointArguments = java.lang.String.format(Locale.US, "%f %f", point.x(), point.y());
            } else {
                pointArguments = java.lang.String.format(Locale.US, "%f %f %f", point.x(), point.y(), point.z());
            }

            return "SRID=%d;POINT (%s)".formatted(srid, pointArguments);
        } else {
            throw new IllegalArgumentException("Not a point to convert");
        }
    }

    private static Object unsupported(Value value) {
        throw new IllegalArgumentException("Node value type is not supported");
    }

    private static Value parseDuration(ValueFactory valueFactory, String input) {
        var parts = input.split("T", 2);

        var months = 0L;
        var days = 0L;
        var seconds = 0L;
        var nanos = 0;

        if (parts.length == 2) {
            try {
                var period = Period.parse(parts[0]);
                months = period.getMonths();
                days = period.getDays();
            } catch (DateTimeParseException ignored) {
            }

            try {
                var duration = java.time.Duration.parse("PT" + parts[1]);
                seconds = duration.getSeconds();
                nanos = duration.getNano();
            } catch (DateTimeParseException ignored) {
            }

        } else if (parts.length == 1) {
            if (input.startsWith("P") && !input.contains("T")) {
                try {
                    var period = Period.parse(parts[0]);
                    var yearsInMonths = period.getYears() * 12;
                    months = period.getMonths() + yearsInMonths;
                    days = period.getDays();
                } catch (DateTimeParseException e) {
                    var duration = java.time.Duration.parse(parts[0]);
                    seconds = duration.getSeconds();
                    nanos = duration.getNano();
                }
            }
        }
        return valueFactory.isoDuration(months, days, seconds, nanos);
    }
}
