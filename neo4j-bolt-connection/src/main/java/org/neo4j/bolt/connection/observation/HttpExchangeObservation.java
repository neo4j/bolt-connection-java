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
package org.neo4j.bolt.connection.observation;

import java.util.List;
import java.util.Map;

/**
 * An observation of HTTP exchange.
 *
 * @since 7.0.0
 */
public interface HttpExchangeObservation extends Observation {
    /**
     * Sets request headers.
     * @param headers the headers
     * @return this observation
     */
    HttpExchangeObservation onHeaders(Map<String, List<String>> headers);

    /**
     * Sets {@link Response}.
     * @param response the response
     * @return this observation
     */
    HttpExchangeObservation onResponse(Response response);

    @Override
    HttpExchangeObservation error(Throwable error);

    /**
     * A response data.
     */
    interface Response {
        /**
         * Returns response status code.
         * @return the status code
         */
        int statusCode();

        /**
         * Returns response headers.
         * @return the headers
         */
        Map<String, List<String>> headers();

        /**
         * Returns HTTP version.
         * @return the HTTP version
         */
        String httpVersion();
    }
}
