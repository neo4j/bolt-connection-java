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

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.MockitoAnnotations.openMocks;

import java.net.URI;
import java.time.Clock;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.neo4j.bolt.connection.BoltConnectionParameters;
import org.neo4j.bolt.connection.BoltConnectionSource;
import org.neo4j.bolt.connection.DefaultDomainNameResolver;
import org.neo4j.bolt.connection.LoggingProvider;
import org.neo4j.bolt.connection.observation.ObservationProvider;

class RoutedBoltConnectionSourceTest {
    RoutedBoltConnectionSource source;

    @Mock
    BoltConnectionSourceFactory boltConnectionSourceFactory;

    @Mock
    BoltConnectionSource<BoltConnectionParameters> upstream;

    @Mock
    ObservationProvider observationProvider;

    @Mock
    LoggingProvider loggingProvider;

    @BeforeEach
    void beforeEach() {
        openMocks(this);
    }

    @Test
    void shouldTimeout() {
        // given
        given(boltConnectionSourceFactory.create(any(), any())).willReturn(upstream);
        given(upstream.getConnection(any())).willReturn(new CompletableFuture<>());
        source = new RoutedBoltConnectionSource(
                boltConnectionSourceFactory,
                Set::of,
                DefaultDomainNameResolver.getInstance(),
                1000,
                null,
                Clock.systemUTC(),
                loggingProvider,
                URI.create("neo4j://localhost:7687"),
                1,
                List.of(),
                observationProvider);

        // when
        var connectionStage = source.getConnection();

        // then
        var completionException = assertThrows(
                CompletionException.class,
                () -> connectionStage.toCompletableFuture().join());
        assertInstanceOf(TimeoutException.class, completionException.getCause());
    }
}
