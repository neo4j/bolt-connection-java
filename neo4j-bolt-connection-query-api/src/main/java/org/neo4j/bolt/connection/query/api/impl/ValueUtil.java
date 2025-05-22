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

import com.fasterxml.jackson.jr.ob.JSON;
import java.io.IOException;
import java.util.Base64;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.neo4j.bolt.connection.exception.BoltException;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.bolt.connection.values.ValueFactory;

final class ValueUtil {
    private static final Pattern POINT_PATTERN = Pattern.compile("^SRID=(\\d+);POINT Z?\\s?\\(([-\\d.\\s]+)\\)$");

    private static String asJsonObject(Value value) throws IOException {
        var json = JSON.std.composeString();
        return switch (value.type()) {
            case ANY -> throw new IllegalArgumentException("Any value type is not supported");
            case BOOLEAN -> {
                var jsonObject = json.startObject();
                jsonObject.put("$type", "Boolean");
                jsonObject.put("_value", value.asBoolean());
                yield jsonObject.end().finish();
            }
            case BYTES -> {
                var jsonObject = json.startObject();
                jsonObject.put("$type", "Base64");
                jsonObject.put("_value", Base64.getEncoder().encodeToString(value.asByteArray()));
                yield jsonObject.end().finish();
            }
            case STRING -> {
                var jsonObject = json.startObject();
                jsonObject.put("$type", "String");
                jsonObject.put("_value", value.asString());
                yield jsonObject.end().finish();
            }
            case NUMBER -> throw new IllegalArgumentException("Number value type is not supported");
            case INTEGER -> {
                var jsonObject = json.startObject();
                jsonObject.put("$type", "Integer");
                jsonObject.put("_value", value.asLong());
                yield jsonObject.end().finish();
            }
            case FLOAT -> {
                var jsonObject = json.startObject();
                jsonObject.put("$type", "Float");
                jsonObject.put("_value", value.asDouble());
                yield jsonObject.end().finish();
            }
            case LIST -> {
                var jsonObject = json.startObject();
                jsonObject.put("$type", "List");
                var jsonArray = jsonObject.startArrayField("_value");
                for (var val : value.values()) {
                    jsonArray.add(asJsonObject(val));
                }
                var finishedArray = jsonArray.end();
                yield finishedArray.end().finish();
            }
            case MAP -> {
                var jsonObject = json.startObject();
                jsonObject.put("$type", "Map");
                var entriesObject = jsonObject.startObjectField("_value");
                for (var entry : value.asMap(Function.identity()).entrySet()) {
                    var key = entry.getKey();
                    var val = entry.getValue();
                    entriesObject.put(key, asJsonObject(val));
                }
                yield jsonObject.end().finish();
            }
            case NODE -> throw new IllegalArgumentException("Node value type is not supported");
            case RELATIONSHIP -> throw new IllegalArgumentException("Relationship value type is not supported");
            case PATH -> throw new IllegalArgumentException("Path value type is not supported");
            case POINT -> {
                var jsonObject = json.startObject();
                jsonObject.put("$type", "Point");
                var point = value.asPoint();
                var pointAsString = Double.isNaN(point.z())
                        ? String.format(Locale.US, "SRID=%d;POINT (%f %f)", point.srid(), point.x(), point.y())
                        : String.format(
                                Locale.US, "SRID=%d;POINT (%f %f %f)", point.srid(), point.x(), point.y(), point.z());
                jsonObject.put("_value", pointAsString);
                yield jsonObject.end().finish();
            }
            case DATE -> {
                var jsonObject = json.startObject();
                jsonObject.put("$type", "Date");
                jsonObject.put("_value", value.asLocalDate().toString());
                yield jsonObject.end().finish();
            }
            case TIME -> {
                var jsonObject = json.startObject();
                jsonObject.put("$type", "Time");
                jsonObject.put("_value", value.asOffsetTime().toString());
                yield jsonObject.end().finish();
            }
            case LOCAL_TIME -> {
                var jsonObject = json.startObject();
                jsonObject.put("$type", "LocalTime");
                jsonObject.put("_value", value.asLocalTime().toString());
                yield jsonObject.end().finish();
            }
            case LOCAL_DATE_TIME -> {
                var jsonObject = json.startObject();
                jsonObject.put("$type", "LocalDateTime");
                jsonObject.put("_value", value.asLocalDateTime().toString());
                yield jsonObject.end().finish();
            }
            case DATE_TIME -> {
                var jsonObject = json.startObject();
                jsonObject.put("$type", "ZonedDateTime");
                jsonObject.put("_value", value.asZonedDateTime().toString());
                yield jsonObject.end().finish();
            }
            case DURATION -> {
                var jsonObject = json.startObject();
                jsonObject.put("$type", "Duration");
                jsonObject.put("_value", value.asIsoDuration().toString());
                yield jsonObject.end().finish();
            }
            case NULL -> {
                var jsonObject = json.startObject();
                jsonObject.put("$type", "Null");
                jsonObject.put("_value", null);
                yield jsonObject.end().finish();
            }
        };
    }

    private static Value asValue(Map<String, Object> jsonObject, ValueFactory valueFactory) throws IOException {
        var type = (String) jsonObject.get("$type");
        var valueElem = (String) jsonObject.get("_value");
        return switch (type) {
            case "Boolean" -> valueFactory.value(Boolean.valueOf(valueElem));
            case "Base64" -> valueFactory.value(Base64.getDecoder().decode(valueElem));
            case "String" -> valueFactory.value(valueElem);
            case "Integer" -> valueFactory.value((Long.valueOf(valueElem)));
            case "Float" -> valueFactory.value(Double.valueOf(valueElem));
            case "List" -> {
                var values = JSON.std.listFrom(valueElem).stream()
                        .map(json -> {
                            try {
                                return asValue((Map<String, Object>) json, valueFactory);
                            } catch (IOException e) {
                                throw new BoltException("kaputt", e);
                            }
                        })
                        .collect(Collectors.toList());
                yield valueFactory.value(values);
            }
                //            case "Map" -> {
                //                var values = valueElem.getAsJsonObject().entrySet().stream()
                //                        .map(entry -> Map.entry(
                //                                entry.getKey(), asValue(entry.getValue().getAsJsonObject(),
                // valueFactory)))
                //                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                //                yield valueFactory.value(values);
                //            }
                //            case "Point" -> {
                //                var stringValue = valueElem.getAsString();
                //                var matcher = POINT_PATTERN.matcher(stringValue);
                //                if (matcher.matches()) {
                //                    var srid = Integer.parseInt(matcher.group(1));
                //                    var coordinates = matcher.group(2).split(" ");
                //                    if (coordinates.length == 2) {
                //                        yield valueFactory.point(
                //                                srid, Double.parseDouble(coordinates[0]),
                // Double.parseDouble(coordinates[1]));
                //                    } else if (coordinates.length == 3) {
                //                        yield valueFactory.point(
                //                                srid,
                //                                Double.parseDouble(coordinates[0]),
                //                                Double.parseDouble(coordinates[1]),
                //                                Double.parseDouble(coordinates[2]));
                //                    }
                //                }
                //                throw new BoltProtocolException("Invalid point value: " + stringValue);
                //            }
                //            case "Date" -> {
                //                var stringValue = valueElem.getAsString();
                //                yield valueFactory.value(LocalDate.parse(stringValue));
                //            }
                //            case "Time" -> {
                //                var stringValue = valueElem.getAsString();
                //                yield valueFactory.value(OffsetTime.parse(stringValue));
                //            }
                //            case "LocalTime" -> {
                //                var stringValue = valueElem.getAsString();
                //                yield valueFactory.value(LocalTime.parse(stringValue));
                //            }
                //            case "LocalDateTime" -> {
                //                var stringValue = valueElem.getAsString();
                //                yield valueFactory.value(LocalDateTime.parse(stringValue));
                //            }
                //            case "OffsetDateTime" -> {
                //                var stringValue = valueElem.getAsString();
                //                yield valueFactory.value(OffsetDateTime.parse(stringValue));
                //            }
                //            case "ZonedDateTime" -> {
                //                var stringValue = valueElem.getAsString();
                //                yield valueFactory.value(ZonedDateTime.parse(stringValue));
                //            }
                //            case "Duration" -> {
                //                var stringValue = valueElem.getAsString();
                //                var parts = stringValue.split("T", 2);
                //
                //                var months = 0L;
                //                var days = 0L;
                //                var seconds = 0L;
                //                var nanos = 0;
                //
                //                if (parts.length == 2) {
                //                    try {
                //                        var period = Period.parse(parts[0]);
                //                        months = period.getMonths();
                //                        days = period.getDays();
                //                    } catch (DateTimeParseException ignored) {
                //                    }
                //
                //                    try {
                //                        var duration = Duration.parse("PT" + parts[1]);
                //                        seconds = duration.getSeconds();
                //                        nanos = duration.getNano();
                //                    } catch (DateTimeParseException ignored) {
                //                    }
                //
                //                } else if (parts.length == 1) {
                //                    if (stringValue.startsWith("P") && !stringValue.contains("T")) {
                //                        try {
                //                            var period = Period.parse(parts[0]);
                //                            months = period.getMonths();
                //                            days = period.getDays();
                //                        } catch (DateTimeParseException e) {
                //                            var duration = Duration.parse(parts[0]);
                //                            seconds = duration.getSeconds();
                //                            nanos = duration.getNano();
                //                        }
                //                    }
                //                }
                //                yield valueFactory.isoDuration(months, days, seconds, nanos);
                //            }
                //            case "Null" -> valueFactory.value((Object) null);
                //            case "Node" -> valueFactory.value(asNode(valueElem.getAsJsonObject(), valueFactory));
                //            case "Relationship" -> valueFactory.value(asRelationship(valueElem.getAsJsonObject(),
                // valueFactory));
                //            case "Path" -> {
                //                List<Segment> segments = new ArrayList<>();
                //                List<Node> nodes = new ArrayList<>();
                //                List<Relationship> relationships = new ArrayList<>();
                //
                //                Node start = null;
                //                Relationship relationship = null;
                //
                //                var list = valueElem.getAsJsonArray().asList();
                //                for (var jsonElement : list) {
                //                    var elementObject = jsonElement.getAsJsonObject();
                //                    var elementType = elementObject.get("$type").getAsString();
                //                    var eleventValue = elementObject.get("_value").getAsJsonObject();
                //                    switch (elementType) {
                //                        case "Node" -> {
                //                            var node = asNode(eleventValue, valueFactory);
                //
                //                            if (start != null) {
                //                                segments.add(valueFactory.segment(start, relationship, node));
                //                            }
                //                            start = node;
                //
                //                            nodes.add(node);
                //                        }
                //                        case "Relationship" -> {
                //                            relationship = asRelationship(eleventValue, valueFactory);
                //                            relationships.add(relationship);
                //                        }
                //                    }
                //                }
                //                yield valueFactory.value(valueFactory.path(segments, nodes, relationships));
                //            }
            default -> throw new IllegalStateException("Unexpected value: " + type);
        };
    }

    //    private static Node asNode(JsonObject node, ValueFactory valueFactory) {
    //        var elementId = node.get("_element_id").getAsString();
    //        var labels = node.get("_labels").getAsJsonArray().asList().stream()
    //                .map(JsonElement::getAsString)
    //                .toList();
    //        var properties = node.get("_properties").getAsJsonObject().entrySet().stream()
    //                .map(entry -> {
    //                    var key = entry.getKey();
    //                    var value = asValue(entry.getValue().getAsJsonObject(), valueFactory);
    //                    return Map.entry(key, value);
    //                })
    //                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    //        return valueFactory.node(0, elementId, labels, properties);
    //    }
    //
    //    private static Relationship asRelationship(JsonObject relationship, ValueFactory valueFactory) {
    //        var elementId = relationship.get("_element_id").getAsString();
    //        var startElementId = relationship.get("_start_node_element_id").getAsString();
    //        var endElementId = relationship.get("_end_node_element_id").getAsString();
    //        var relationshipType = relationship.get("_type").getAsString();
    //        var properties = relationship.get("_properties").getAsJsonObject().entrySet().stream()
    //                .map(entry -> {
    //                    var key = entry.getKey();
    //                    var value = asValue(entry.getValue().getAsJsonObject(), valueFactory);
    //                    return Map.entry(key, value);
    //                })
    //                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    //        return valueFactory.relationship(
    //                0, elementId, 0, startElementId, 0, endElementId, relationshipType, properties);
    //    }

    private ValueUtil() {}
}
