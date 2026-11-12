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
package org.neo4j.bolt.connection.routed.impl;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.neo4j.bolt.connection.AccessMode;
import org.neo4j.bolt.connection.BoltConnection;
import org.neo4j.bolt.connection.routed.RoutedBoltConnectionSource;
import org.neo4j.bolt.connection.routed.impl.cluster.RoutingTableHandler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.never;
import static org.mockito.MockitoAnnotations.openMocks;

class RoutedBoltConnectionTest {
    @Mock
    RoutedBoltConnectionSource source;

    @Mock
    BoltConnection delegate;

    @Mock
    RoutingTableHandler routingTableHandler;

    @BeforeEach
    void beforeEach() {
        openMocks(this);
    }

    @Test
    void shouldCloseDelegateConnectionBeforeDecrementingInUse() {
        // GIVEN
        var connection = new RoutedBoltConnection(delegate, routingTableHandler, AccessMode.WRITE, source);
        var delegateCloseFuture = new CompletableFuture<Void>();
        given(delegate.close()).willReturn(delegateCloseFuture);

        // WHEN
        var closeFuture = connection.close();
        then(delegate).should().close();
        then(source).should(never()).decrementInUseCount(any());
        delegateCloseFuture.complete(null);
        closeFuture.toCompletableFuture().join();

        // THEN
        then(source).should().decrementInUseCount(any());
    }
}
