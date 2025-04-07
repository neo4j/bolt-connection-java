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
package org.neo4j.bolt.connection.query.api.impl;

import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

final class HttpUtil {
    static String mapToString(HttpRequest request) {
        var sb = new StringBuilder();
        sb.append("HTTP Request:\n");
        sb.append("  Method: ").append(request.method()).append("\n");
        sb.append("  URI: ").append(request.uri()).append("\n");
        sb.append("  Headers:\n");

        request.headers().map().forEach((k, v) -> sb.append("    ")
                .append(k)
                .append(": ")
                .append(String.join(", ", v))
                .append("\n"));

        if (request.bodyPublisher().isPresent()) {
            sb.append("  Body: [BodyPublisher present, raw body not printable]\n");
        } else {
            sb.append("  Body: <none>\n");
        }
        return sb.toString();
    }

    static <T> String mapToString(HttpResponse<T> response) {
        var sb = new StringBuilder();
        sb.append("HTTP Response:\n");
        sb.append("  Status Code: ").append(response.statusCode()).append("\n");
        sb.append("  Headers:\n");

        for (var entry : response.headers().map().entrySet()) {
            sb.append("    ")
                    .append(entry.getKey())
                    .append(": ")
                    .append(String.join(", ", entry.getValue()))
                    .append("\n");
        }

        T body = response.body();
        if (body != null) {
            sb.append("  Body:\n");
            sb.append("    ").append(body.toString().replace("\n", "\n    ")).append("\n");
        } else {
            sb.append("  Body: <null>\n");
        }

        return sb.toString();
    }

    private HttpUtil() {}
}
