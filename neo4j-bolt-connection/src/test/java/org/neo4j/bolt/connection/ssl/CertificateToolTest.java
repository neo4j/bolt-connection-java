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
package org.neo4j.bolt.connection.ssl;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemWriter;
import org.junit.jupiter.api.Test;

class CertificateToolTest {
    private static final String DEFAULT_HOST_NAME = "localhost";
    private static final String DEFAULT_ENCRYPTION = "RSA";
    private static final Provider PROVIDER = new BouncyCastleProvider();
    private static final String BEGIN_CERT = "-----BEGIN CERTIFICATE-----";
    private static final String END_CERT = "-----END CERTIFICATE-----";

    static {
        // adds the Bouncy castle provider to java security
        Security.addProvider(PROVIDER);
    }

    @Test
    void shouldLoadMultipleCertsIntoKeyStore() throws Throwable {
        // Given
        var certFile = File.createTempFile("3random", ".cer");
        certFile.deleteOnExit();

        var cert1 = generateSelfSignedCertificate();
        var cert2 = generateSelfSignedCertificate();
        var cert3 = generateSelfSignedCertificate();

        saveX509Cert(new Certificate[] {cert1, cert2, cert3}, certFile);

        var keyStore = KeyStore.getInstance("JKS");
        keyStore.load(null, null);

        // When
        CertificateTool.loadX509Cert(Collections.singletonList(certFile), keyStore);

        // Then
        var aliases = keyStore.aliases();
        assertTrue(aliases.hasMoreElements());
        assertTrue(aliases.nextElement().startsWith("neo4j.javadriver.trustedcert"));
        assertTrue(aliases.hasMoreElements());
        assertTrue(aliases.nextElement().startsWith("neo4j.javadriver.trustedcert"));
        assertTrue(aliases.hasMoreElements());
        assertTrue(aliases.nextElement().startsWith("neo4j.javadriver.trustedcert"));
        assertFalse(aliases.hasMoreElements());
    }

    private static KeyPair generateKeyPair() throws NoSuchAlgorithmException {
        var keyPairGenerator = KeyPairGenerator.getInstance(DEFAULT_ENCRYPTION);
        keyPairGenerator.initialize(2048, new SecureRandom());
        return keyPairGenerator.generateKeyPair();
    }

    private static X509Certificate generateCert(
            X500Name issuer, X500Name subject, KeyPair issuerKeys, PublicKey publicKey, GeneralName... generalNames)
            throws GeneralSecurityException, OperatorCreationException, CertIOException {
        // Create x509 certificate
        var startDate = new Date(System.currentTimeMillis());
        var endDate = new Date(System.currentTimeMillis() + 365L * 24L * 60L * 60L * 1000L);
        var serialNum = BigInteger.valueOf(System.currentTimeMillis());
        X509v3CertificateBuilder certBuilder =
                new JcaX509v3CertificateBuilder(issuer, serialNum, startDate, endDate, subject, publicKey);

        // Subject alternative name (part of SNI extension, used for hostname verification)
        Set<GeneralName> names = new HashSet<>();
        names.add(new GeneralName(GeneralName.dNSName, DEFAULT_HOST_NAME));
        names.addAll(Arrays.asList(generalNames));
        var subjectAlternativeName = new GeneralNames(names.toArray(new GeneralName[0]));
        certBuilder.addExtension(Extension.subjectAlternativeName, false, subjectAlternativeName);
        certBuilder.addExtension(Extension.basicConstraints, false, new BasicConstraints(true));

        // Get the certificate back
        var signer = new JcaContentSignerBuilder("SHA512WithRSAEncryption").build(issuerKeys.getPrivate());
        var certHolder = certBuilder.build(signer);
        var certificate = new JcaX509CertificateConverter().setProvider("BC").getCertificate(certHolder);

        certificate.verify(issuerKeys.getPublic());
        return certificate;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void writePem(String type, byte[] encodedContent, File path) throws IOException {
        if (path.getParentFile() != null && path.getParentFile().exists()) {
            path.getParentFile().mkdirs();
        }
        try (var writer = new PemWriter(new FileWriter(path))) {
            writer.writeObject(new PemObject(type, encodedContent));
            writer.flush();
        }
    }

    private static void saveX509Cert(Certificate cert, File certFile) throws GeneralSecurityException, IOException {
        saveX509Cert(new Certificate[] {cert}, certFile);
    }

    private static void saveX509Cert(Certificate[] certs, File certFile) throws GeneralSecurityException, IOException {
        try (var writer = new BufferedWriter(new FileWriter(certFile))) {
            for (var cert : certs) {
                var certStr =
                        Base64.getEncoder().encodeToString(cert.getEncoded()).replaceAll("(.{64})", "$1\n");

                writer.write(BEGIN_CERT);
                writer.newLine();

                writer.write(certStr);
                writer.newLine();

                writer.write(END_CERT);
                writer.newLine();
            }
        }
    }

    private static X509Certificate generateSelfSignedCertificate()
            throws GeneralSecurityException, OperatorCreationException, CertIOException {
        return new SelfSignedCertificateGenerator().certificate;
    }

    private static class SelfSignedCertificateGenerator {
        private final KeyPair keyPair;
        private final X509Certificate certificate;

        public SelfSignedCertificateGenerator(GeneralName... generalNames)
                throws GeneralSecurityException, OperatorCreationException, CertIOException {
            // Create the public/private rsa key pair
            keyPair = generateKeyPair();

            // Create x509 certificate
            certificate = generateCert(
                    new X500Name("CN=" + DEFAULT_HOST_NAME),
                    new X500Name("CN=" + DEFAULT_HOST_NAME),
                    keyPair,
                    keyPair.getPublic(),
                    generalNames);
        }

        public void savePrivateKey(File saveTo) throws IOException {
            writePem("PRIVATE KEY", keyPair.getPrivate().getEncoded(), saveTo);
        }

        public void saveSelfSignedCertificate(File saveTo) throws CertificateEncodingException, IOException {
            writePem("CERTIFICATE", certificate.getEncoded(), saveTo);
        }

        public X509Certificate sign(PKCS10CertificationRequest csr, PublicKey csrPublicKey, GeneralName... generalNames)
                throws GeneralSecurityException, OperatorCreationException, CertIOException {
            return generateCert(
                    X500Name.getInstance(
                            this.certificate.getSubjectX500Principal().getEncoded()),
                    csr.getSubject(),
                    keyPair,
                    csrPublicKey,
                    generalNames);
        }
    }
}
