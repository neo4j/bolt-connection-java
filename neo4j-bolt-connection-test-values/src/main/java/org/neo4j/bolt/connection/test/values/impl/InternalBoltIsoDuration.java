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

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.NANOS;
import static java.time.temporal.ChronoUnit.SECONDS;

import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalUnit;
import java.util.List;
import java.util.Objects;
import org.neo4j.bolt.connection.values.IsoDuration;

public class InternalBoltIsoDuration implements IsoDuration {
    private static final long NANOS_PER_SECOND = 1_000_000_000;
    private static final List<TemporalUnit> SUPPORTED_UNITS = List.of(MONTHS, DAYS, SECONDS, NANOS);

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
        var sb = new StringBuilder();
        sb.append('P');
        sb.append(months).append('M');
        sb.append(days).append('D');
        sb.append('T');
        if (seconds < 0 && nanoseconds > 0) {
            if (seconds == -1) {
                sb.append("-0");
            } else {
                sb.append(seconds + 1);
            }
        } else {
            sb.append(seconds);
        }
        if (nanoseconds > 0) {
            var pos = sb.length();
            // append nanoseconds as a 10-digit string with leading '1' that is later replaced by a '.'
            if (seconds < 0) {
                sb.append(2 * NANOS_PER_SECOND - nanoseconds);
            } else {
                sb.append(NANOS_PER_SECOND + nanoseconds);
            }
            sb.setCharAt(pos, '.'); // replace '1' with '.'
        }
        sb.append('S');
        return sb.toString();
    }
}
