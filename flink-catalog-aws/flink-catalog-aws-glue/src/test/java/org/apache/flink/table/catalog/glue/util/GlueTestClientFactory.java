/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.catalog.glue.util;

import org.apache.flink.table.catalog.glue.operator.FakeGlueClient;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;

/**
 * Provides the {@link GlueClient} backing the catalog test suites: an in-memory {@link
 * FakeGlueClient} by default (used by CI), or a client against <b>real AWS Glue</b> when
 * credentials are supplied, so the same tests can be executed against the real service.
 *
 * <p>Real mode is enabled by either of:
 *
 * <ul>
 *   <li>{@code IT_CASE_GLUE_CATALOG_ACCESS_KEY} / {@code IT_CASE_GLUE_CATALOG_SECRET_KEY} (and
 *       optionally {@code IT_CASE_GLUE_CATALOG_SESSION_TOKEN}) - explicit credentials
 *   <li>{@code IT_CASE_GLUE_CATALOG_USE_DEFAULT_CREDENTIALS=true} - the AWS default credential
 *       provider chain (SSO, instance profiles, credential_process)
 * </ul>
 *
 * <p>{@code IT_CASE_GLUE_CATALOG_REGION} selects the region (default {@code us-east-1}). Real mode
 * is never active in CI, which sets none of these variables. When running in real mode, register
 * {@link RealGlueCleanupExtension} on the test class so databases created by tests are removed from
 * the account afterwards.
 */
public final class GlueTestClientFactory {

    private static final String ACCESS_KEY = System.getenv("IT_CASE_GLUE_CATALOG_ACCESS_KEY");
    private static final String SECRET_KEY = System.getenv("IT_CASE_GLUE_CATALOG_SECRET_KEY");
    private static final String SESSION_TOKEN = System.getenv("IT_CASE_GLUE_CATALOG_SESSION_TOKEN");

    /** Region used for real-Glue runs. */
    public static final String REGION =
            System.getenv().getOrDefault("IT_CASE_GLUE_CATALOG_REGION", "us-east-1");

    /** Whether the suite is running against real AWS Glue instead of the in-memory fake. */
    public static final boolean REAL_GLUE =
            Boolean.parseBoolean(
                            System.getenv()
                                    .getOrDefault(
                                            "IT_CASE_GLUE_CATALOG_USE_DEFAULT_CREDENTIALS",
                                            "false"))
                    || (ACCESS_KEY != null && !ACCESS_KEY.isBlank());

    private GlueTestClientFactory() {}

    /**
     * Returns a database name unique to this test execution. Real Glue deletes databases
     * asynchronously, so reusing a fixed name across tests races the previous test's in-flight
     * deletion; unique names avoid that (and are harmless for the in-memory fake).
     */
    public static String uniqueName(String prefix) {
        return prefix + "_" + Long.toHexString(System.nanoTime());
    }

    /**
     * Creates the client for a test: a freshly reset {@link FakeGlueClient}, or a real Glue client
     * when real mode is enabled.
     */
    public static GlueClient createClient() {
        if (!REAL_GLUE) {
            FakeGlueClient.reset();
            return new FakeGlueClient();
        }
        GlueClientBuilder builder = GlueClient.builder().region(Region.of(REGION));
        if (ACCESS_KEY != null && !ACCESS_KEY.isBlank()) {
            AwsCredentials credentials =
                    (SESSION_TOKEN != null && !SESSION_TOKEN.isBlank())
                            ? AwsSessionCredentials.create(ACCESS_KEY, SECRET_KEY, SESSION_TOKEN)
                            : AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY);
            builder.credentialsProvider(StaticCredentialsProvider.create(credentials));
        }
        return builder.build();
    }
}
