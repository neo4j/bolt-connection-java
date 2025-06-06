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
package org.neo4j.bolt.connection.query_api.impl;

import com.fasterxml.jackson.jr.ob.JSON;
import java.net.URI;
import java.net.http.HttpClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class HttpContextTest {

    @ParameterizedTest
    @ValueSource(
            strings = {
                "http://localhost",
                "http://localhost/subpath",
                "http://localhost/a/b/c",
                "http://localhost:7474",
                "http://localhost:7474/subpath",
                "http://localhost/",
                "http://localhost/subpath/",
                "http://localhost/a/b/c/",
            })
    void shouldNormalizeBase(URI uri) {

        var httpContext = new HttpContext(HttpClient.newHttpClient(), uri, JSON.std, new String[] {});
        var expected = uri.toString() + (uri.toString().endsWith("/") ? "" : "/") + "db/foo/query/v2";
        Assertions.assertEquals(expected, httpContext.queryUrl("foo").toString());
    }

    @Test
    void txUrlShouldWork() {
        var httpContext = new HttpContext(
                HttpClient.newHttpClient(), URI.create("http://localhost:7474"), JSON.std, new String[] {});
        Assertions.assertEquals(
                "http://localhost:7474/db/ads/query/v2/tx",
                httpContext.txUrl("ads").toString());
    }
}
