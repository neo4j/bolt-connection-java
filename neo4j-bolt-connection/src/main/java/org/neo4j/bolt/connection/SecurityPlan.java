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
package org.neo4j.bolt.connection;

import javax.net.ssl.SSLContext;

/**
 * A SecurityPlan consists of encryption and trust details.
 * @since 1.0.0
 */
public sealed interface SecurityPlan permits SecurityPlanImpl {
    /**
     * Returns {@link SSLContext} that must be used.
     * @return the {@link SSLContext}, must not be {@code null}
     */
    SSLContext sslContext();

    /**
     * Indicates if hostname verification must be done.
     * @return {@code true} if enabled, {@code false} if disabled
     */
    boolean verifyHostname();

    /**
     * An optional hostname that is used for hostname verification when the {@link #verifyHostname()} option is enabled.
     * <p>
     * When {@code null} value is used, the hostname provided in the connection {@link java.net.URI} is used for
     * verification. Therefore, this option should only be used if a diferrent name should be used for verification.
     * For example, when the {@link java.net.URI} contains an IP address, but a domain name should be used for
     * verification purposes.
     * @since 4.0.0
     * @return the expected hostname
     */
    String expectedHostname();
}
