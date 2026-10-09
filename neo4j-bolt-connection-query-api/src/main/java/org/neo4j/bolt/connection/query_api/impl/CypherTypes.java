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

import java.io.Serializable;
import java.lang.reflect.Array;
import java.time.LocalDate;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.neo4j.bolt.connection.values.Type;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.bolt.connection.values.ValueFactory;
import org.neo4j.bolt.connection.values.ValueUtils;

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
        (v) -> v.boltValues()
    ),

    Map(
        Type.MAP,
        null, // manually handled in JSON converter
        Value::asBoltMap
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
        ValueUtils::parseDuration,
        (v) -> ValueUtils.renderDuration(v.asBoltIsoDuration())
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
        CypherTypes::unsupported),

    Vector(
            Type.VECTOR,
            null, // handled in DriverValueProvider
            CypherTypes::writeVector,
            VndNeo4jQueryVersion.V1_1);

    // spotless:on
    private final BiFunction<ValueFactory, String, Value> reader;
    private final Function<Value, Object> writer;
    private final Type type;
    private final VndNeo4jQueryVersion minVndNeo4jQueryVersion;

    CypherTypes(Type type, BiFunction<ValueFactory, String, Value> reader, Function<Value, Object> writer) {
        this(type, reader, writer, VndNeo4jQueryVersion.V1_0);
    }

    CypherTypes(
            Type type,
            BiFunction<ValueFactory, String, Value> reader,
            Function<Value, Object> writer,
            VndNeo4jQueryVersion minVndNeo4jQueryVersion) {
        this.type = type;
        this.reader = reader;
        this.writer = writer;
        this.minVndNeo4jQueryVersion = minVndNeo4jQueryVersion;
    }

    public static CypherTypes typeFromValue(Value value) {
        var valueType = value.boltValueType();
        for (CypherTypes cypherType : values()) {
            if (cypherType.type == valueType) {
                return cypherType;
            }
        }

        throw new IllegalArgumentException("no Cypher type found representing " + value.boltValueType());
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

    public VndNeo4jQueryVersion getMinVndNeo4jQueryVersion() {
        return minVndNeo4jQueryVersion;
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
        if (value.boltValueType() == Type.POINT) {
            var point = value.asBoltPoint();
            var srid = point.srid();
            return Double.isNaN(point.z())
                    ? "SRID=%d;POINT (%f %f)".formatted(srid, point.x(), point.y())
                    : "SRID=%d;POINT Z (%f %f %f)".formatted(srid, point.x(), point.y(), point.z());
        } else {
            throw new IllegalArgumentException("Not a point to convert");
        }
    }

    private static Map<String, Serializable> writeVector(Value value) {
        if (value.boltValueType() == Type.VECTOR) {
            var vector = value.asBoltVector();
            var elements = vector.elements();
            var length = Array.getLength(elements);
            String coordinatesType;
            var coordinates = new String[length];
            var elementType = vector.elementType();
            if (elementType.equals(long.class) || elementType.isAssignableFrom(Long.class)) {
                coordinatesType = "INT64";
            } else if (elementType.equals(int.class) || elementType.equals(Integer.class)) {
                coordinatesType = "INT32";
            } else if (elementType.equals(double.class) || elementType.equals(Double.class)) {
                coordinatesType = "FLOAT64";
            } else if (elementType.equals(float.class) || elementType.equals(Float.class)) {
                coordinatesType = "FLOAT32";
            } else if (elementType.equals(short.class) || elementType.equals(Short.class)) {
                coordinatesType = "INT16";
            } else if (elementType.equals(byte.class) || elementType.equals(Byte.class)) {
                coordinatesType = "INT8";
            } else {
                throw new IllegalArgumentException("Unsupported vector element type: " + elementType);
            }
            for (var i = 0; i < length; i++) {
                coordinates[i] = java.lang.String.valueOf(Array.get(elements, i));
            }
            return java.util.Map.of("coordinatesType", coordinatesType, "coordinates", coordinates);
        } else {
            throw new IllegalArgumentException("Not a vector to convert");
        }
    }

    private static Object unsupported(Value value) {
        throw new IllegalArgumentException("Node value type is not supported");
    }
}
