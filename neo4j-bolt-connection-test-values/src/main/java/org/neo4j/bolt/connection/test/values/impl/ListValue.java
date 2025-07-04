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

import java.util.List;
import org.neo4j.bolt.connection.values.Type;
import org.neo4j.bolt.connection.values.Value;

public class ListValue extends ValueAdapter {
    private final List<? extends Value> values;

    public ListValue(List<? extends Value> values) {
        if (values == null) {
            throw new IllegalArgumentException("Cannot construct ListValue from null");
        }
        this.values = values;
    }

    @Override
    public boolean isEmpty() {
        return values.isEmpty();
    }

    @Override
    public int size() {
        return values.size();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<Value> boltValues() {
        return (Iterable<Value>) values;
    }

    @Override
    public Type boltValueType() {
        return Type.LIST;
    }

    @Override
    public String toString() {
        return values.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        var otherValues = (ListValue) o;
        return values.equals(otherValues.values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }
}
