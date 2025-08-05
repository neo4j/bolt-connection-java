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
package org.neo4j.bolt.connection.routed.impl.cluster;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.connection.AccessMode.READ;
import static org.neo4j.bolt.connection.AccessMode.WRITE;
import static org.neo4j.bolt.connection.BoltServerAddress.LOCAL_DEFAULT;
import static org.neo4j.bolt.connection.DatabaseName.defaultDatabase;
import static org.neo4j.bolt.connection.routed.impl.util.ClusterCompositionUtil.A;
import static org.neo4j.bolt.connection.routed.impl.util.ClusterCompositionUtil.B;
import static org.neo4j.bolt.connection.routed.impl.util.ClusterCompositionUtil.C;
import static org.neo4j.bolt.connection.routed.impl.util.ClusterCompositionUtil.D;
import static org.neo4j.bolt.connection.routed.impl.util.ClusterCompositionUtil.E;
import static org.neo4j.bolt.connection.routed.impl.util.ClusterCompositionUtil.F;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.bolt.connection.AccessMode;
import org.neo4j.bolt.connection.BoltConnection;
import org.neo4j.bolt.connection.BoltConnectionParameters;
import org.neo4j.bolt.connection.BoltConnectionSource;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.ClusterComposition;
import org.neo4j.bolt.connection.DatabaseName;
import org.neo4j.bolt.connection.RoutedBoltConnectionParameters;
import org.neo4j.bolt.connection.exception.BoltServiceUnavailableException;
import org.neo4j.bolt.connection.observation.ImmutableObservation;
import org.neo4j.bolt.connection.observation.Observation;
import org.neo4j.bolt.connection.routed.ClusterCompositionLookupResult;
import org.neo4j.bolt.connection.routed.Rediscovery;
import org.neo4j.bolt.connection.routed.RoutingTable;
import org.neo4j.bolt.connection.routed.impl.NoopLoggingProvider;
import org.neo4j.bolt.connection.routed.impl.util.FakeClock;

class RoutingTableHandlerTest {
    public static final long STALE_ROUTING_TABLE_PURGE_DELAY_MS = SECONDS.toMillis(30);
    RoutedBoltConnectionParameters parameters = RoutedBoltConnectionParameters.builder()
            .withAccessMode(AccessMode.READ)
            .withMinVersion(new BoltProtocolVersion(4, 1))
            .build();

    @Test
    void shouldRemoveAddressFromRoutingTableOnConnectionFailure() {
        RoutingTable routingTable = new ClusterRoutingTable(defaultDatabase(), new FakeClock());
        routingTable.update(
                new ClusterComposition(42, asOrderedSet(A, B, C), asOrderedSet(A, C, E), asOrderedSet(B, D, F), null));

        var handler = newRoutingTableHandler(routingTable, newRediscoveryMock(), newConnectionPoolMock());

        handler.onConnectionFailure(B);

        assertArrayEquals(new BoltServerAddress[] {A, C}, routingTable.readers().toArray());
        assertArrayEquals(
                new BoltServerAddress[] {A, C, E}, routingTable.writers().toArray());
        assertArrayEquals(new BoltServerAddress[] {D, F}, routingTable.routers().toArray());

        handler.onConnectionFailure(A);

        assertArrayEquals(new BoltServerAddress[] {C}, routingTable.readers().toArray());
        assertArrayEquals(new BoltServerAddress[] {C, E}, routingTable.writers().toArray());
        assertArrayEquals(new BoltServerAddress[] {D, F}, routingTable.routers().toArray());
    }

    @Test
    void acquireShouldUpdateRoutingTableWhenKnownRoutingTableIsStale() {
        var initialRouter = new BoltServerAddress("initialRouter", 1);
        var reader1 = new BoltServerAddress("reader-1", 2);
        var reader2 = new BoltServerAddress("reader-1", 3);
        var writer1 = new BoltServerAddress("writer-1", 4);
        var router1 = new BoltServerAddress("router-1", 5);

        var connectionPool = newConnectionPoolMock();
        var routingTable = new ClusterRoutingTable(defaultDatabase(), new FakeClock(), initialRouter);

        Set<BoltServerAddress> readers = new LinkedHashSet<>(asList(reader1, reader2));
        Set<BoltServerAddress> writers = new LinkedHashSet<>(singletonList(writer1));
        Set<BoltServerAddress> routers = new LinkedHashSet<>(singletonList(router1));
        var clusterComposition = new ClusterComposition(42, readers, writers, routers, null);
        Rediscovery rediscovery = Mockito.mock(RediscoveryImpl.class);
        when(rediscovery.lookupClusterComposition(any(), any(), any(), any()))
                .thenReturn(completedFuture(new ClusterCompositionLookupResult(clusterComposition)));

        var handler = newRoutingTableHandler(routingTable, rediscovery, connectionPool);
        assertNotNull(handler.ensureRoutingTable(parameters, mock(Observation.class))
                .toCompletableFuture()
                .join());

        verify(rediscovery).lookupClusterComposition(any(), any(), any(), any());
        assertArrayEquals(
                new BoltServerAddress[] {reader1, reader2},
                routingTable.readers().toArray());
        assertArrayEquals(
                new BoltServerAddress[] {writer1}, routingTable.writers().toArray());
        assertArrayEquals(
                new BoltServerAddress[] {router1}, routingTable.routers().toArray());
    }

    @Test
    void shouldRediscoverOnReadWhenRoutingTableIsStaleForReads() {
        testRediscoveryWhenStale(READ);
    }

    @Test
    void shouldRediscoverOnWriteWhenRoutingTableIsStaleForWrites() {
        testRediscoveryWhenStale(WRITE);
    }

    @Test
    void shouldNotRediscoverOnReadWhenRoutingTableIsStaleForWritesButNotReads() {
        testNoRediscoveryWhenNotStale(WRITE, READ);
    }

    @Test
    void shouldNotRediscoverOnWriteWhenRoutingTableIsStaleForReadsButNotWrites() {
        testNoRediscoveryWhenNotStale(READ, WRITE);
    }

    @Test
    void shouldRetainAllFetchedAddressesInConnectionPoolAfterFetchingOfRoutingTable() {
        RoutingTable routingTable = new ClusterRoutingTable(defaultDatabase(), new FakeClock());
        routingTable.update(new ClusterComposition(42, asOrderedSet(), asOrderedSet(B, C), asOrderedSet(D, E), null));

        var connectionPool = newConnectionPoolMock();

        var rediscovery = newRediscoveryMock();
        when(rediscovery.lookupClusterComposition(any(), any(), any(), any()))
                .thenReturn(completedFuture(new ClusterCompositionLookupResult(
                        new ClusterComposition(42, asOrderedSet(A, B), asOrderedSet(B, C), asOrderedSet(A, C), null))));

        var registry = new RoutingTableRegistry() {
            @Override
            public CompletionStage<RoutingTableHandler> ensureRoutingTable(
                    CompletableFuture<DatabaseName> databaseNameFuture,
                    RoutedBoltConnectionParameters parameters,
                    ImmutableObservation parentObservation) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Set<BoltServerAddress> allServers() {
                return routingTable.servers();
            }

            @Override
            public void remove(DatabaseName databaseName) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void removeAged() {}

            @Override
            public Optional<RoutingTableHandler> getRoutingTableHandler(DatabaseName databaseName) {
                return Optional.empty();
            }
        };

        var addressesToRetainRef = new AtomicReference<Set<BoltServerAddress>>();
        var handler =
                newRoutingTableHandler(routingTable, rediscovery, connectionPool, registry, addressesToRetainRef::set);

        var actual = handler.ensureRoutingTable(parameters, mock(Observation.class))
                .toCompletableFuture()
                .join();
        assertEquals(routingTable, actual);

        assertEquals(Set.of(A, B, C), addressesToRetainRef.get());
    }

    @Test
    void shouldRemoveRoutingTableHandlerIfFailedToLookup() {
        // Given
        RoutingTable routingTable = new ClusterRoutingTable(defaultDatabase(), new FakeClock());

        var rediscovery = newRediscoveryMock();
        when(rediscovery.lookupClusterComposition(any(), any(), any(), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Bang!")));

        var connectionPool = newConnectionPoolMock();
        var registry = newRoutingTableRegistryMock();
        // When

        var handler = newRoutingTableHandler(routingTable, rediscovery, connectionPool, registry);
        assertThrows(RuntimeException.class, () -> handler.ensureRoutingTable(parameters, mock(Observation.class))
                .toCompletableFuture()
                .join());

        // Then
        verify(registry).remove(defaultDatabase());
    }

    private void testRediscoveryWhenStale(AccessMode mode) {
        Function<BoltServerAddress, BoltConnectionSource<BoltConnectionParameters>> connectionSourceGetter =
                requestedAddress -> {
                    @SuppressWarnings("unchecked")
                    BoltConnectionSource<BoltConnectionParameters> boltConnectionSource =
                            mock(BoltConnectionSource.class);
                    var connection = mock(BoltConnection.class);
                    given(boltConnectionSource.getConnection(any())).willReturn(completedFuture(connection));
                    return boltConnectionSource;
                };

        var routingTable = newStaleRoutingTableMock(mode);
        var rediscovery = newRediscoveryMock();

        var handler = newRoutingTableHandler(routingTable, rediscovery, connectionSourceGetter);
        var actual = handler.ensureRoutingTable(
                        RoutedBoltConnectionParameters.builder()
                                .withAccessMode(mode)
                                .withMinVersion(new BoltProtocolVersion(4, 1))
                                .build(),
                        mock(Observation.class))
                .toCompletableFuture()
                .join();
        assertEquals(routingTable, actual);

        verify(routingTable).isStaleFor(mode);
        verify(rediscovery).lookupClusterComposition(any(), any(), any(), any());
    }

    private void testNoRediscoveryWhenNotStale(AccessMode staleMode, AccessMode notStaleMode) {
        Function<BoltServerAddress, BoltConnectionSource<BoltConnectionParameters>> connectionSourceGetter =
                requestedAddress -> {
                    @SuppressWarnings("unchecked")
                    BoltConnectionSource<BoltConnectionParameters> boltConnectionSource =
                            mock(BoltConnectionSource.class);
                    var connection = mock(BoltConnection.class);
                    given(boltConnectionSource.getConnection(any())).willReturn(completedFuture(connection));
                    return boltConnectionSource;
                };

        var routingTable = newStaleRoutingTableMock(staleMode);
        var rediscovery = newRediscoveryMock();

        var handler = newRoutingTableHandler(routingTable, rediscovery, connectionSourceGetter);

        assertNotNull(handler.ensureRoutingTable(
                        RoutedBoltConnectionParameters.builder()
                                .withAccessMode(notStaleMode)
                                .withMinVersion(new BoltProtocolVersion(4, 1))
                                .build(),
                        mock(Observation.class))
                .toCompletableFuture()
                .join());
        verify(routingTable).isStaleFor(notStaleMode);
        verify(rediscovery, never()).lookupClusterComposition(any(), any(), any(), any());
    }

    private static RoutingTable newStaleRoutingTableMock(AccessMode mode) {
        var routingTable = Mockito.mock(RoutingTable.class);
        when(routingTable.isStaleFor(mode)).thenReturn(true);

        var addresses = singletonList(LOCAL_DEFAULT);
        when(routingTable.readers()).thenReturn(addresses);
        when(routingTable.writers()).thenReturn(addresses);
        when(routingTable.database()).thenReturn(defaultDatabase());

        return routingTable;
    }

    private static RoutingTableRegistry newRoutingTableRegistryMock() {
        return Mockito.mock(RoutingTableRegistry.class);
    }

    private static Rediscovery newRediscoveryMock() {
        Rediscovery rediscovery = Mockito.mock(RediscoveryImpl.class);
        Set<BoltServerAddress> noServers = Collections.emptySet();
        var clusterComposition = new ClusterComposition(1, noServers, noServers, noServers, null);
        when(rediscovery.lookupClusterComposition(any(), any(), any(), any()))
                .thenReturn(completedFuture(new ClusterCompositionLookupResult(clusterComposition)));
        return rediscovery;
    }

    private static Function<BoltServerAddress, BoltConnectionSource<BoltConnectionParameters>> newConnectionPoolMock() {
        return newConnectionPoolMockWithFailures(emptySet());
    }

    private static Function<BoltServerAddress, BoltConnectionSource<BoltConnectionParameters>>
            newConnectionPoolMockWithFailures(Set<BoltServerAddress> unavailableAddresses) {
        return requestedAddress -> {
            @SuppressWarnings("unchecked")
            BoltConnectionSource<BoltConnectionParameters> boltConnectionSource = mock(BoltConnectionSource.class);
            if (unavailableAddresses.contains(requestedAddress)) {
                given(boltConnectionSource.getConnection(any()))
                        .willReturn(CompletableFuture.failedFuture(
                                new BoltServiceUnavailableException(requestedAddress + " is unavailable!")));
                return boltConnectionSource;
            }
            var connection = mock(BoltConnection.class);
            when(connection.serverAddress()).thenReturn(requestedAddress);
            given(boltConnectionSource.getConnection(any())).willReturn(completedFuture(connection));
            return boltConnectionSource;
        };
    }

    private static RoutingTableHandler newRoutingTableHandler(
            RoutingTable routingTable,
            Rediscovery rediscovery,
            Function<BoltServerAddress, BoltConnectionSource<BoltConnectionParameters>> connectionSourceGetter) {
        return new RoutingTableHandlerImpl(
                routingTable,
                rediscovery,
                connectionSourceGetter,
                newRoutingTableRegistryMock(),
                NoopLoggingProvider.INSTANCE,
                STALE_ROUTING_TABLE_PURGE_DELAY_MS,
                ignored -> {});
    }

    private static RoutingTableHandler newRoutingTableHandler(
            RoutingTable routingTable,
            Rediscovery rediscovery,
            Function<BoltServerAddress, BoltConnectionSource<BoltConnectionParameters>> connectionSourceGetter,
            RoutingTableRegistry routingTableRegistry) {
        return newRoutingTableHandler(
                routingTable, rediscovery, connectionSourceGetter, routingTableRegistry, ignored -> {});
    }

    private static RoutingTableHandler newRoutingTableHandler(
            RoutingTable routingTable,
            Rediscovery rediscovery,
            Function<BoltServerAddress, BoltConnectionSource<BoltConnectionParameters>> connectionSourceGetter,
            RoutingTableRegistry routingTableRegistry,
            Consumer<Set<BoltServerAddress>> addressesToRetainConsumer) {
        return new RoutingTableHandlerImpl(
                routingTable,
                rediscovery,
                connectionSourceGetter,
                routingTableRegistry,
                NoopLoggingProvider.INSTANCE,
                STALE_ROUTING_TABLE_PURGE_DELAY_MS,
                addressesToRetainConsumer);
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> Set<T> asOrderedSet(T... elements) {
        return new LinkedHashSet<>(Arrays.asList(elements));
    }
}
