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
package org.neo4j.bolt.connection.routed;

import java.net.URI;
import org.neo4j.bolt.connection.BoltConnectionParameters;
import org.neo4j.bolt.connection.BoltConnectionSource;

/**
 * A factory for {@link BoltConnectionSource} instances used by {@link RoutedBoltConnectionSource}.
 * @since 4.0.0
 */
@FunctionalInterface
public interface BoltConnectionSourceFactory {
    /**
     * Creates a {@link BoltConnectionSource} for a specofic {@link URI} provided by the {@link RoutedBoltConnectionSource}.
     * <p>
     * As {@link URI} instances provided by the {@link RoutedBoltConnectionSource} use {@code bolt} schemes,
     * {@link BoltConnectionSource} instances MUST support such schemes.
     * @param uri the URI
     * @param expectedHostname the expected hostname for verification purposes
     * @return a new {@link BoltConnectionSource}
     */
    BoltConnectionSource<BoltConnectionParameters> create(URI uri, String expectedHostname);
}
