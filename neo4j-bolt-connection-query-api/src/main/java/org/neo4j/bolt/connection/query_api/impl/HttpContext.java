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
import java.util.Objects;

public record HttpContext(HttpClient httpClient, URI baseUri, JSON json, String[] headers) {

    public HttpContext {
        var path = baseUri.getPath();
        if (path.endsWith("/")) {
            baseUri = baseUri.resolve(path.substring(0, path.length() - 1));
        }
    }

    public HttpContext(HttpClient httpClient, URI baseUri, JSON json, String authHeader, String userAgent) {
        this(httpClient, baseUri, json, headers(authHeader, userAgent));
    }

    private static String[] headers(String authHeader, String userAgent) {
        var headers = new String[userAgent != null ? 8 : 6];
        headers[0] = "Content-Type";
        headers[1] = "application/vnd.neo4j.query";
        headers[2] = "Accept";
        headers[3] = "application/vnd.neo4j.query";
        headers[4] = "Authorization";
        headers[5] = Objects.requireNonNull(authHeader);
        if (userAgent != null) {
            headers[6] = "User-Agent";
            headers[7] = userAgent;
        }
        return headers;
    }

    URI queryUrl(String databaseName) {
        return URI.create("%s/db/%s/query/v2".formatted(baseUri, databaseName)).normalize();
    }

    URI txUrl(String databaseName) {
        return URI.create("%s/tx".formatted(queryUrl(databaseName)));
    }

    URI txUrl(TransactionInfo tx) {
        return URI.create("%s/%s".formatted(txUrl(tx.databaseName()), tx.id()));
    }

    URI commitUrl(TransactionInfo tx) {
        return URI.create("%s/commit".formatted(txUrl(tx)));
    }
}
