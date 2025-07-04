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

import java.lang.reflect.InvocationTargetException;
import java.time.DateTimeException;
import java.time.ZonedDateTime;
import org.neo4j.bolt.connection.values.Type;

public class UnsupportedDateTimeValue extends ValueAdapter {
    final DateTimeException exception;

    public UnsupportedDateTimeValue(DateTimeException exception) {
        this.exception = exception;
    }

    @Override
    public ZonedDateTime asZonedDateTime() {
        throw instantiateDateTimeException();
    }

    @Override
    public Type boltValueType() {
        return Type.DATE_TIME;
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    @Override
    public String toString() {
        return "Unsupported datetime value.";
    }

    private DateTimeException instantiateDateTimeException() {
        DateTimeException newException;
        try {
            newException = exception
                    .getClass()
                    .getDeclaredConstructor(String.class, Throwable.class)
                    .newInstance(exception.getMessage(), exception);
        } catch (NoSuchMethodException
                | InvocationTargetException
                | InstantiationException
                | IllegalAccessException e) {
            newException = new DateTimeException(exception.getMessage(), exception);
        }
        return newException;
    }
}
