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

import java.time.Duration;
import java.time.Period;
import java.util.Objects;
import org.neo4j.bolt.connection.values.IsoDuration;
import org.neo4j.bolt.connection.values.ValueUtils;

public class InternalBoltIsoDuration implements IsoDuration {

    private final long months;
    private final long days;
    private final long seconds;
    private final int nanoseconds;

    public InternalBoltIsoDuration(Period period) {
        this(period.toTotalMonths(), period.getDays(), Duration.ZERO);
    }

    public InternalBoltIsoDuration(Duration duration) {
        this(0, 0, duration);
    }

    public InternalBoltIsoDuration(long months, long days, long seconds, int nanoseconds) {
        this(months, days, Duration.ofSeconds(seconds, nanoseconds));
    }

    InternalBoltIsoDuration(long months, long days, Duration duration) {
        this.months = months;
        this.days = days;
        this.seconds = duration.getSeconds(); // normalized value of seconds
        this.nanoseconds = duration.getNano(); // normalized value of nanoseconds in [0, 999_999_999]
    }

    @Override
    public long months() {
        return months;
    }

    @Override
    public long days() {
        return days;
    }

    @Override
    public long seconds() {
        return seconds;
    }

    @Override
    public int nanoseconds() {
        return nanoseconds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        var that = (InternalBoltIsoDuration) o;
        return months == that.months && days == that.days && seconds == that.seconds && nanoseconds == that.nanoseconds;
    }

    @Override
    public int hashCode() {
        return Objects.hash(months, days, seconds, nanoseconds);
    }

    @Override
    public String toString() {
        return ValueUtils.renderDuration(this);
    }
}
