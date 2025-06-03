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

import java.io.IOException;
import java.security.GeneralSecurityException;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import org.neo4j.bolt.connection.ssl.RevocationCheckingStrategy;
import org.neo4j.bolt.connection.ssl.SSLContexts;
import org.neo4j.bolt.connection.ssl.TrustManagerFactories;

public final class SecurityPlans {
    public static SecurityPlan encrypted(SSLContext sslContext, boolean requiresHostnameVerification) {
        return encrypted(sslContext, requiresHostnameVerification, null);
    }

    public static SecurityPlan encrypted(
            SSLContext sslContext, boolean requiresHostnameVerification, String hostnameForVerification) {
        return new SecurityPlanImpl(sslContext, requiresHostnameVerification, hostnameForVerification);
    }

    public static SecurityPlan encryptedForAnyCertificate() throws GeneralSecurityException {
        var sslContext = SSLContexts.forAnyCertificate(new KeyManager[0]);
        return encrypted(sslContext, false);
    }

    public static SecurityPlan encryptedForSystemCASignedCertificates() throws GeneralSecurityException, IOException {
        var trustManagerFactory = TrustManagerFactories.forSystemCertificates(RevocationCheckingStrategy.NO_CHECKS);
        var sslContext = SSLContexts.forTrustManagers(new KeyManager[0], trustManagerFactory.getTrustManagers());
        return encrypted(sslContext, true);
    }

    private SecurityPlans() {}
}
