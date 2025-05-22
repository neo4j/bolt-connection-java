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

import static java.lang.String.format;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.jr.ob.api.ReaderWriterProvider;
import com.fasterxml.jackson.jr.ob.api.ValueReader;
import com.fasterxml.jackson.jr.ob.api.ValueWriter;
import com.fasterxml.jackson.jr.ob.impl.JSONReader;
import com.fasterxml.jackson.jr.ob.impl.JSONWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.bolt.connection.values.ValueFactory;

/**
 * @author Gerrit Meier
 */
public class DriverValueProvider extends ReaderWriterProvider {

    private final ValueFactory valueFactory;

    public DriverValueProvider(ValueFactory valueFactory) {
        this.valueFactory = valueFactory;
    }

    @Override
    public ValueReader findValueReader(JSONReader readContext, Class<?> type) {
        if (Value.class.isAssignableFrom(type)) {
            return new ValueValueReader(Value.class, valueFactory);
        }
        return super.findValueReader(readContext, type);
    }

    @Override
    public ValueWriter findValueWriter(JSONWriter writeContext, Class<?> type) {
        if (Value.class.isAssignableFrom(type)) {
            return new ValueValueWriter();
        }
        return super.findValueWriter(writeContext, type);
    }

    static class ValueValueWriter implements ValueWriter {

        @Override
        public void writeValue(JSONWriter context, JsonGenerator g, Object value) throws IOException {
            CypherTypes cypherType = CypherTypes.typeFromValue((Value) value);
            g.writeStartObject();
            g.writeFieldName(Fieldnames.CYPHER_TYPE);
            context.writeValue(cypherType.name());
            g.writeFieldName(Fieldnames.CYPHER_VALUE);
            context.writeValue(fromValue(cypherType, (Value) value));
            g.writeEndObject();
        }

        private Object fromValue(CypherTypes cypherType, Value value) {
            Function<Value, Object> writer = cypherType.getWriter();
            if (writer == null) {
                throw new IllegalArgumentException("could not obtain writer for " + cypherType);
            }
            return writer.apply(value);
        }

        @Override
        public Class<?> valueType() {
            return Value.class;
        }
    }

    static class ValueValueReader extends ValueReader {

        private final ValueFactory valueFactory;

        protected ValueValueReader(Class<?> valueType, ValueFactory valueFactory) {
            super(valueType);
            this.valueFactory = valueFactory;
        }

        @Override
        public Object read(JSONReader reader, JsonParser p) throws IOException {
            String name = p.nextFieldName();

            if (name.equals(Fieldnames.CYPHER_TYPE)) {
                String typeString = p.nextTextValue();
                JsonToken nextToken = p.nextToken();

                if (nextToken.equals(JsonToken.FIELD_NAME) && p.currentName().equals(Fieldnames.CYPHER_VALUE)) {
                    p.nextToken();
                    if (typeString.equals(CypherTypes.List.name())) {
                        Value listValue = valueFactory.value(reader.readListOf(Value.class));
                        p.nextToken();
                        return listValue;
                    } else if (typeString.equals(CypherTypes.Map.name())) {
                        Map<String, Value> value1 = reader.readMapOf(Value.class);
                        p.nextToken();
                        return valueFactory.value(value1);
                    } else if (typeString.equals(CypherTypes.Boolean.name())) {
                        Value boolValue = valueFactory.value(p.getBooleanValue());
                        p.nextToken();
                        return boolValue;
                    } else if (typeString.equals(CypherTypes.Null.name())) {
                        if (p.currentToken().equals(JsonToken.VALUE_NULL)) {
                            p.nextToken();
                            return valueFactory.value((Object) null);
                        } else {
                            throw new JsonParseException("Expected 'null' value");
                        }
                    } else if (typeString.equals(CypherTypes.Node.name())) {
                        var node = reader.readBean(DeserializedNode.class);
                        p.nextToken();
                        return valueFactory.value(valueFactory.node(
                                node.getId(), node.get_element_id(), node.get_labels(), node.get_properties()));
                    } else if (typeString.equals(CypherTypes.Relationship.name())) {
                        var relationship = reader.readBean(DeserializedRelationship.class);
                        p.nextToken();
                        return valueFactory.value(valueFactory.relationship(
                                relationship.getId(),
                                relationship.get_element_id(),
                                relationship.getStartId(),
                                relationship.get_start_node_element_id(),
                                relationship.getEndId(),
                                relationship.get_end_node_element_id(),
                                relationship.get_type(),
                                relationship.get_properties()));
                    } else {
                        BiFunction<ValueFactory, String, Value> parser =
                                CypherTypes.valueOf(typeString).getReader();

                        if (parser != null) {
                            String stringValue = p.getValueAsString();
                            p.nextToken();
                            return parser.apply(valueFactory, stringValue);
                        } else {
                            throw new JsonParseException(format("Type %s is not a valid parameter type.", typeString));
                        }
                    }
                } else {
                    throw new JsonParseException(format("Expecting field %s", Fieldnames.CYPHER_VALUE));
                }
            } else {
                throw new JsonParseException("Expected a typed value.");
            }
        }
    }

    public static class DeserializedNode {
        private String _element_id;

        private List<String> _labels;

        private Map<String, Value> _properties;

        public String get_element_id() {
            return _element_id;
        }

        public void set_element_id(String _element_id) {
            this._element_id = _element_id;
        }

        // Legacy needs
        public long getId() {
            return Long.parseLong(get_element_id().split(":")[2]);
        }

        public List<String> get_labels() {
            return _labels;
        }

        public void set_labels(List<String> _labels) {
            this._labels = _labels;
        }

        public Map<String, Value> get_properties() {
            return _properties;
        }

        public void set_properties(Map<String, Value> _properties) {
            this._properties = _properties;
        }
    }

    public static class DeserializedRelationship {
        private String _element_id;

        private String _start_node_element_id;

        private String _end_node_element_id;

        private String _type;

        private Map<String, Value> _properties;

        public String get_element_id() {
            return _element_id;
        }

        public void set_element_id(String _element_id) {
            this._element_id = _element_id;
        }

        public String get_end_node_element_id() {
            return _end_node_element_id;
        }

        public void set_end_node_element_id(String _end_node_element_id) {
            this._end_node_element_id = _end_node_element_id;
        }

        public Map<String, Value> get_properties() {
            return _properties;
        }

        public void set_properties(Map<String, Value> _properties) {
            this._properties = _properties;
        }

        public String get_start_node_element_id() {
            return _start_node_element_id;
        }

        public void set_start_node_element_id(String _start_node_element_id) {
            this._start_node_element_id = _start_node_element_id;
        }

        public String get_type() {
            return _type;
        }

        public void set_type(String _type) {
            this._type = _type;
        }

        // Legacy needs
        public long getId() {
            return Long.parseLong(get_element_id().split(":")[2]);
        }

        public long getStartId() {
            return Long.parseLong(get_start_node_element_id().split(":")[2]);
        }

        public long getEndId() {
            return Long.parseLong(get_end_node_element_id().split(":")[2]);
        }
    }
}
