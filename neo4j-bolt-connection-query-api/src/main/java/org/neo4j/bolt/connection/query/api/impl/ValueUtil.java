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
package org.neo4j.bolt.connection.query.api.impl;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.neo4j.bolt.connection.exception.BoltProtocolException;
import org.neo4j.bolt.connection.values.Node;
import org.neo4j.bolt.connection.values.Relationship;
import org.neo4j.bolt.connection.values.Segment;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.bolt.connection.values.ValueFactory;

final class ValueUtil {
    private static final Pattern POINT_PATTERN = Pattern.compile("^SRID=(\\d+);POINT Z?\\s?\\(([-\\d.\\s]+)\\)$");

    static JsonObject asJsonObject(Value value) {
        return switch (value.type()) {
            case ANY -> throw new IllegalArgumentException("Any value type is not supported");
            case BOOLEAN -> {
                var jsonObject = new JsonObject();
                jsonObject.addProperty("$type", "Boolean");
                jsonObject.addProperty("_value", value.asBoolean());
                yield jsonObject;
            }
            case BYTES -> {
                var jsonObject = new JsonObject();
                jsonObject.addProperty("$type", "Base64");
                jsonObject.addProperty("_value", Base64.getEncoder().encodeToString(value.asByteArray()));
                yield jsonObject;
            }
            case STRING -> {
                var jsonObject = new JsonObject();
                jsonObject.addProperty("$type", "String");
                jsonObject.addProperty("_value", value.asString());
                yield jsonObject;
            }
            case NUMBER -> throw new IllegalArgumentException("Number value type is not supported");
            case INTEGER -> {
                var jsonObject = new JsonObject();
                jsonObject.addProperty("$type", "Integer");
                jsonObject.addProperty("_value", value.asLong());
                yield jsonObject;
            }
            case FLOAT -> {
                var jsonObject = new JsonObject();
                jsonObject.addProperty("$type", "Float");
                jsonObject.addProperty("_value", value.asDouble());
                yield jsonObject;
            }
            case LIST -> {
                var jsonObject = new JsonObject();
                jsonObject.addProperty("$type", "List");
                var jsonArray = new JsonArray();
                for (var val : value.values()) {
                    jsonArray.add(asJsonObject(val));
                }
                jsonObject.add("_value", jsonArray);
                yield jsonObject;
            }
            case MAP -> {
                var jsonObject = new JsonObject();
                jsonObject.addProperty("$type", "Map");
                var entriesObject = new JsonObject();
                for (var entry : value.asMap(Function.identity()).entrySet()) {
                    var key = entry.getKey();
                    var val = entry.getValue();
                    entriesObject.add(key, asJsonObject(val));
                }
                jsonObject.add("_value", entriesObject);
                yield jsonObject;
            }
            case NODE -> throw new IllegalArgumentException("Node value type is not supported");
            case RELATIONSHIP -> throw new IllegalArgumentException("Relationship value type is not supported");
            case PATH -> throw new IllegalArgumentException("Path value type is not supported");
            case POINT -> {
                var jsonObject = new JsonObject();
                jsonObject.addProperty("$type", "Point");
                var point = value.asPoint();
                var pointAsString = Double.isNaN(point.z())
                        ? String.format(Locale.US, "SRID=%d;POINT (%f %f)", point.srid(), point.x(), point.y())
                        : String.format(
                                Locale.US, "SRID=%d;POINT (%f %f %f)", point.srid(), point.x(), point.y(), point.z());
                jsonObject.addProperty("_value", pointAsString);
                yield jsonObject;
            }
            case DATE -> {
                var jsonObject = new JsonObject();
                jsonObject.addProperty("$type", "Date");
                jsonObject.addProperty("_value", value.asLocalDate().toString());
                yield jsonObject;
            }
            case TIME -> {
                var jsonObject = new JsonObject();
                jsonObject.addProperty("$type", "Time");
                jsonObject.addProperty("_value", value.asOffsetTime().toString());
                yield jsonObject;
            }
            case LOCAL_TIME -> {
                var jsonObject = new JsonObject();
                jsonObject.addProperty("$type", "LocalTime");
                jsonObject.addProperty("_value", value.asLocalTime().toString());
                yield jsonObject;
            }
            case LOCAL_DATE_TIME -> {
                var jsonObject = new JsonObject();
                jsonObject.addProperty("$type", "LocalDateTime");
                jsonObject.addProperty("_value", value.asLocalDateTime().toString());
                yield jsonObject;
            }
            case DATE_TIME -> {
                var jsonObject = new JsonObject();
                jsonObject.addProperty("$type", "ZonedDateTime");
                jsonObject.addProperty("_value", value.asZonedDateTime().toString());
                yield jsonObject;
            }
            case DURATION -> {
                var jsonObject = new JsonObject();
                jsonObject.addProperty("$type", "Duration");
                jsonObject.addProperty("_value", value.asIsoDuration().toString());
                yield jsonObject;
            }
            case NULL -> {
                var jsonObject = new JsonObject();
                jsonObject.addProperty("$type", "Null");
                jsonObject.add("_value", JsonNull.INSTANCE);
                yield jsonObject;
            }
        };
    }

    static Value asValue(JsonObject jsonObject, ValueFactory valueFactory) {
        var type = jsonObject.get("$type").getAsString();
        var valueElem = jsonObject.get("_value");
        return switch (type) {
            case "Boolean" -> valueFactory.value(valueElem.getAsBoolean());
            case "Base64" -> valueFactory.value(Base64.getDecoder().decode(valueElem.getAsString()));
            case "String" -> valueFactory.value(valueElem.getAsString());
            case "Integer" -> valueFactory.value(valueElem.getAsLong());
            case "Float" -> valueFactory.value(valueElem.getAsDouble());
            case "List" -> {
                var values = valueElem.getAsJsonArray().asList().stream()
                        .map(JsonElement::getAsJsonObject)
                        .map(json -> asValue(json, valueFactory))
                        .collect(Collectors.toList());
                yield valueFactory.value(values);
            }
            case "Map" -> {
                var values = valueElem.getAsJsonObject().entrySet().stream()
                        .map(entry -> Map.entry(
                                entry.getKey(), asValue(entry.getValue().getAsJsonObject(), valueFactory)))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                yield valueFactory.value(values);
            }
            case "Point" -> {
                var stringValue = valueElem.getAsString();
                var matcher = POINT_PATTERN.matcher(stringValue);
                if (matcher.matches()) {
                    var srid = Integer.parseInt(matcher.group(1));
                    var coordinates = matcher.group(2).split(" ");
                    if (coordinates.length == 2) {
                        yield valueFactory.point(
                                srid, Double.parseDouble(coordinates[0]), Double.parseDouble(coordinates[1]));
                    } else if (coordinates.length == 3) {
                        yield valueFactory.point(
                                srid,
                                Double.parseDouble(coordinates[0]),
                                Double.parseDouble(coordinates[1]),
                                Double.parseDouble(coordinates[2]));
                    }
                }
                throw new BoltProtocolException("Invalid point value: " + stringValue);
            }
            case "Date" -> {
                var stringValue = valueElem.getAsString();
                yield valueFactory.value(LocalDate.parse(stringValue));
            }
            case "Time" -> {
                var stringValue = valueElem.getAsString();
                yield valueFactory.value(OffsetTime.parse(stringValue));
            }
            case "LocalTime" -> {
                var stringValue = valueElem.getAsString();
                yield valueFactory.value(LocalTime.parse(stringValue));
            }
            case "LocalDateTime" -> {
                var stringValue = valueElem.getAsString();
                yield valueFactory.value(LocalDateTime.parse(stringValue));
            }
            case "OffsetDateTime" -> {
                var stringValue = valueElem.getAsString();
                yield valueFactory.value(OffsetDateTime.parse(stringValue));
            }
            case "ZonedDateTime" -> {
                var stringValue = valueElem.getAsString();
                yield valueFactory.value(ZonedDateTime.parse(stringValue));
            }
            case "Duration" -> {
                var stringValue = valueElem.getAsString();
                var parts = stringValue.split("T", 2);

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
                        var duration = Duration.parse("PT" + parts[1]);
                        seconds = duration.getSeconds();
                        nanos = duration.getNano();
                    } catch (DateTimeParseException ignored) {
                    }

                } else if (parts.length == 1) {
                    if (stringValue.startsWith("P") && !stringValue.contains("T")) {
                        try {
                            var period = Period.parse(parts[0]);
                            months = period.getMonths();
                            days = period.getDays();
                        } catch (DateTimeParseException e) {
                            var duration = Duration.parse(parts[0]);
                            seconds = duration.getSeconds();
                            nanos = duration.getNano();
                        }
                    }
                }
                yield valueFactory.isoDuration(months, days, seconds, nanos);
            }
            case "Null" -> valueFactory.value((Object) null);
            case "Node" -> valueFactory.value(asNode(valueElem.getAsJsonObject(), valueFactory));
            case "Relationship" -> valueFactory.value(asRelationship(valueElem.getAsJsonObject(), valueFactory));
            case "Path" -> {
                List<Segment> segments = new ArrayList<>();
                List<Node> nodes = new ArrayList<>();
                List<Relationship> relationships = new ArrayList<>();

                Node start = null;
                Relationship relationship = null;

                var list = valueElem.getAsJsonArray().asList();
                for (var jsonElement : list) {
                    var elementObject = jsonElement.getAsJsonObject();
                    var elementType = elementObject.get("$type").getAsString();
                    var eleventValue = elementObject.get("_value").getAsJsonObject();
                    switch (elementType) {
                        case "Node" -> {
                            var node = asNode(eleventValue, valueFactory);

                            if (start != null) {
                                segments.add(valueFactory.segment(start, relationship, node));
                            }
                            start = node;

                            nodes.add(node);
                        }
                        case "Relationship" -> {
                            relationship = asRelationship(eleventValue, valueFactory);
                            relationships.add(relationship);
                        }
                    }
                }
                yield valueFactory.value(valueFactory.path(segments, nodes, relationships));
            }
            default -> throw new IllegalStateException("Unexpected value: " + type);
        };
    }

    private static Node asNode(JsonObject node, ValueFactory valueFactory) {
        var elementId = node.get("_element_id").getAsString();
        var labels = node.get("_labels").getAsJsonArray().asList().stream()
                .map(JsonElement::getAsString)
                .toList();
        var properties = node.get("_properties").getAsJsonObject().entrySet().stream()
                .map(entry -> {
                    var key = entry.getKey();
                    var value = asValue(entry.getValue().getAsJsonObject(), valueFactory);
                    return Map.entry(key, value);
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return valueFactory.node(0, elementId, labels, properties);
    }

    private static Relationship asRelationship(JsonObject relationship, ValueFactory valueFactory) {
        var elementId = relationship.get("_element_id").getAsString();
        var startElementId = relationship.get("_start_node_element_id").getAsString();
        var endElementId = relationship.get("_end_node_element_id").getAsString();
        var relationshipType = relationship.get("_type").getAsString();
        var properties = relationship.get("_properties").getAsJsonObject().entrySet().stream()
                .map(entry -> {
                    var key = entry.getKey();
                    var value = asValue(entry.getValue().getAsJsonObject(), valueFactory);
                    return Map.entry(key, value);
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return valueFactory.relationship(
                0, elementId, 0, startElementId, 0, endElementId, relationshipType, properties);
    }

    private ValueUtil() {}
}
