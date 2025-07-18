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
package org.neo4j.bolt.connection.test.values.impl;

import java.util.Map;
import java.util.function.Function;
import org.neo4j.bolt.connection.values.Type;
import org.neo4j.bolt.connection.values.Value;

public class MapValue extends ValueAdapter {
    private final Map<String, Value> val;

    public MapValue(Map<String, Value> val) {
        if (val == null) {
            throw new IllegalArgumentException("Cannot construct MapValue from null");
        }
        this.val = val;
    }

    @Override
    public boolean isEmpty() {
        return val.isEmpty();
    }

    @Override
    public Map<String, Value> asBoltMap() {
        return Extract.map(val, Function.identity());
    }

    @Override
    public int size() {
        return val.size();
    }

    @Override
    public boolean containsKey(String key) {
        return val.containsKey(key);
    }

    @Override
    public Iterable<String> keys() {
        return val.keySet();
    }

    @Override
    public Iterable<Value> boltValues() {
        return val.values();
    }

    @Override
    public Value getBoltValue(String key) {
        var value = val.get(key);
        return value == null ? NullValue.NULL : value;
    }

    @Override
    public String toString() {
        return "MapValue{" + "val=" + val + '}';
    }

    @Override
    public Type boltValueType() {
        return Type.MAP;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        var values = (MapValue) o;
        return val.equals(values.val);
    }

    @Override
    public int hashCode() {
        return val.hashCode();
    }
}
