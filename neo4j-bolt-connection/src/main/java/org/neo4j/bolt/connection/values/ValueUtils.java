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
package org.neo4j.bolt.connection.values;

import java.time.Period;
import java.time.format.DateTimeParseException;

/**
 * The methods in this utility class are mainly aimed at Bolt connection developers or for use in the Neo4j Java or JDBC Drivers.
 * Both drivers are maintained by Neo4j inline with the Bolt Connection API, so we will upgrade and change the methods in this
 * class as we see fit and won't give any API guarantees.
 */
public final class ValueUtils {

    private static final long NANOS_PER_SECOND = 1_000_000_000;

    private ValueUtils() {}

    /**
     * Parses a {@link String} into a {@link Value} wrapping an {@link IsoDuration}.
     *
     * @param valueFactory needed for creating the underlying value
     * @param input        the input to parse
     * @return the new value
     */
    public static Value parseDuration(ValueFactory valueFactory, String input) {
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

    /**
     * Renders an {@link IsoDuration} into a commonly agreed upon {@link String}
     *
     * @param duration the duration to render
     * @return a string representation or {@literal null} if {@literal duration} is {@literal null}
     */
    public static String renderDuration(IsoDuration duration) {

        if (duration == null) {
            return null;
        }

        var months = duration.months();
        var days = duration.days();
        var seconds = duration.seconds();
        var nanoseconds = duration.nanoseconds();

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
