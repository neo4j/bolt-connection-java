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
package org.neo4j.bolt.connection.codec.value_encoding;

import java.util.Objects;
import org.neo4j.bolt.connection.BoltProtocolVersion;

/**
 * A version of Bolt Value Encoding Scheme.
 * @since 12.1.0
 */
public final class ValueEncodingSchemeVersion implements Comparable<ValueEncodingSchemeVersion> {
    /**
     * Version 1.0.
     */
    public static final ValueEncodingSchemeVersion V1_0 = new ValueEncodingSchemeVersion(1, 0);

    private final int majorVersion;
    private final int minorVersion;

    /**
     * Creates a new version instance.
     * @param majorVersion the major version
     * @param minorVersion the minor version
     */
    public ValueEncodingSchemeVersion(int majorVersion, int minorVersion) {
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
    }

    /**
     * Returns the major version.
     * @return the major version
     */
    public long majorVersion() {
        return majorVersion;
    }
    /**
     * Returns the minor version.
     * @return the minor version
     */
    public long minorVersion() {
        return minorVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(minorVersion, majorVersion);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof BoltProtocolVersion other)) {
            return false;
        } else {
            return this.majorVersion() == other.getMajorVersion() && this.minorVersion() == other.getMinorVersion();
        }
    }

    @Override
    public int compareTo(ValueEncodingSchemeVersion other) {
        var result = Integer.compare(majorVersion, other.majorVersion);

        if (result == 0) {
            return Integer.compare(minorVersion, other.minorVersion);
        }

        return result;
    }
}
