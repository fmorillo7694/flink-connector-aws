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

package org.apache.flink.table.catalog.glue.test;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetTableRequest;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * End-to-end test for the Glue catalog against <b>real AWS Glue</b>.
 *
 * <p>This test exercises the complete user path: {@code CREATE CATALOG ... WITH ('type'='glue')}
 * discovers {@code GlueCatalogFactory} via SPI, the factory builds its own {@code GlueClient} from
 * the default credential/region chain, and all catalog operations flow through Flink SQL DDL and
 * the planner down to the real Glue service.
 *
 * <p>Following the convention of the other AWS end-to-end tests in this repository, the test is
 * gated on explicit credentials and skips cleanly when they are absent:
 *
 * <ul>
 *   <li>{@code IT_CASE_GLUE_CATALOG_ACCESS_KEY} - AWS access key id (required)
 *   <li>{@code IT_CASE_GLUE_CATALOG_SECRET_KEY} - AWS secret access key (required)
 *   <li>{@code IT_CASE_GLUE_CATALOG_SESSION_TOKEN} - session token (optional, for temporary
 *       credentials)
 *   <li>{@code IT_CASE_GLUE_CATALOG_REGION} - region (optional, defaults to {@code us-east-1})
 * </ul>
 *
 * <p>Credentials are handed to the factory-built client through the AWS SDK system properties
 * ({@code aws.accessKeyId}, {@code aws.secretAccessKey}, {@code aws.sessionToken}, {@code
 * aws.region}), which the default provider chain resolves - no test-only code path exists in the
 * factory.
 *
 * <p>Unlike the moto-based integration tests in the catalog module, this test also covers the two
 * behaviours only real Glue exhibits: column-name lowercasing with case restoration through the
 * catalog, and {@code dropDatabase} (whose emptiness check requires the UDF API that moto does not
 * implement).
 *
 * <p>Run with: {@code mvn verify -Prun-aws-end-to-end-tests} with the environment variables above.
 */
@Tag("requires-aws-credentials")
class GlueCatalogEndToEndITCase {

    private static final String ACCESS_KEY = System.getenv("IT_CASE_GLUE_CATALOG_ACCESS_KEY");
    private static final String SECRET_KEY = System.getenv("IT_CASE_GLUE_CATALOG_SECRET_KEY");
    private static final String SESSION_TOKEN = System.getenv("IT_CASE_GLUE_CATALOG_SESSION_TOKEN");
    private static final String REGION =
            System.getenv().getOrDefault("IT_CASE_GLUE_CATALOG_REGION", "us-east-1");

    /**
     * Alternative gate for environments where extracting key material is undesirable (SSO, instance
     * profiles, credential_process): when {@code true}, the test runs with the AWS default
     * credential provider chain instead of explicit keys.
     */
    private static final boolean USE_DEFAULT_CREDENTIALS =
            Boolean.parseBoolean(
                    System.getenv()
                            .getOrDefault("IT_CASE_GLUE_CATALOG_USE_DEFAULT_CREDENTIALS", "false"));

    private static final String CATALOG_NAME = "glue_e2e";
    private static final String DB_NAME =
            "flink_e2e_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    private static final String DATA_DB_NAME = DB_NAME + "_data";

    private static TableEnvironment tEnv;
    private static GlueClient rawGlueClient;

    @BeforeAll
    static void setUp() {
        assumeThat(USE_DEFAULT_CREDENTIALS || (ACCESS_KEY != null && !ACCESS_KEY.isBlank()))
                .as("Credentials not configured, skipping test...")
                .isTrue();

        GlueClientBuilder rawClientBuilder = GlueClient.builder().region(Region.of(REGION));
        if (!USE_DEFAULT_CREDENTIALS) {
            assumeThat(SECRET_KEY).as("Secret key not configured, skipping test...").isNotBlank();

            // Route the factory-built GlueClient's default provider chain to the test
            // credentials.
            System.setProperty("aws.accessKeyId", ACCESS_KEY);
            System.setProperty("aws.secretAccessKey", SECRET_KEY);
            if (SESSION_TOKEN != null && !SESSION_TOKEN.isBlank()) {
                System.setProperty("aws.sessionToken", SESSION_TOKEN);
            }

            // Independent client for out-of-band verification and cleanup.
            AwsCredentials rawCredentials =
                    (SESSION_TOKEN != null && !SESSION_TOKEN.isBlank())
                            ? AwsSessionCredentials.create(ACCESS_KEY, SECRET_KEY, SESSION_TOKEN)
                            : AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY);
            rawClientBuilder.credentialsProvider(StaticCredentialsProvider.create(rawCredentials));
        }
        System.setProperty("aws.region", REGION);
        rawGlueClient = rawClientBuilder.build();

        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());
        tEnv.executeSql(
                "CREATE CATALOG "
                        + CATALOG_NAME
                        + " WITH ('type' = 'glue', 'region' = '"
                        + REGION
                        + "', 'default-database' = 'default')");
        tEnv.executeSql("USE CATALOG " + CATALOG_NAME);
    }

    @AfterEach
    void cleanUpTables() {
        if (rawGlueClient == null) {
            return;
        }
        for (String db : new String[] {DB_NAME, DATA_DB_NAME}) {
            try {
                rawGlueClient
                        .getTables(b -> b.databaseName(db))
                        .tableList()
                        .forEach(
                                t ->
                                        rawGlueClient.deleteTable(
                                                b -> b.databaseName(db).name(t.name())));
            } catch (EntityNotFoundException ignored) {
                // database already gone
            }
        }
    }

    @AfterAll
    static void tearDown() {
        if (rawGlueClient != null) {
            for (String db : new String[] {DB_NAME, DATA_DB_NAME}) {
                try {
                    rawGlueClient.deleteDatabase(b -> b.name(db));
                } catch (EntityNotFoundException ignored) {
                    // already dropped by the test
                }
            }
            rawGlueClient.close();
        }
        System.clearProperty("aws.accessKeyId");
        System.clearProperty("aws.secretAccessKey");
        System.clearProperty("aws.sessionToken");
        System.clearProperty("aws.region");
    }

    @Test
    void testDatabaseAndTableLifecycleEndToEnd() {
        tEnv.executeSql(
                "CREATE DATABASE IF NOT EXISTS "
                        + DB_NAME
                        + " COMMENT 'Flink Glue catalog e2e test database'");
        assertThat(sql("SHOW DATABASES")).extracting(r -> r.getField(0)).contains(DB_NAME);

        // All object names are fully qualified so the test never depends on a pre-existing
        // 'default' database in the target account.
        tEnv.executeSql(
                "CREATE TABLE "
                        + DB_NAME
                        + ".orders_e2e ("
                        + "  orderId STRING,"
                        + "  orderTotal DOUBLE,"
                        + "  orderTime TIMESTAMP(3)"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = 'arn:aws:kinesis:"
                        + REGION
                        + ":000000000000:stream/e2e-orders',"
                        + "  'format' = 'json'"
                        + ")");
        assertThat(sql("SHOW TABLES FROM " + DB_NAME))
                .extracting(r -> r.getField(0))
                .contains("orders_e2e");

        // Case restoration through the catalog: real Glue lowercases physical column names,
        // the catalog must restore the original camelCase on read.
        List<String> describedColumns =
                sql("DESCRIBE " + DB_NAME + ".orders_e2e").stream()
                        .map(r -> String.valueOf(r.getField(0)))
                        .collect(Collectors.toList());
        assertThat(describedColumns).containsExactly("orderId", "orderTotal", "orderTime");

        // Out-of-band wire verification: the table exists in real Glue, and Glue stored the
        // physical column names lowercased (the behaviour emulators do not reproduce).
        List<Column> glueColumns =
                rawGlueClient
                        .getTable(
                                GetTableRequest.builder()
                                        .databaseName(DB_NAME)
                                        .name("orders_e2e")
                                        .build())
                        .table()
                        .storageDescriptor()
                        .columns();
        assertThat(glueColumns)
                .extracting(Column::name)
                .containsExactly("orderid", "ordertotal", "ordertime");

        // Full teardown through SQL - including dropDatabase, whose emptiness check
        // (listFunctions) cannot run against moto and is only covered here.
        tEnv.executeSql("DROP TABLE " + DB_NAME + ".orders_e2e");
        assertThat(sql("SHOW TABLES FROM " + DB_NAME))
                .extracting(r -> r.getField(0))
                .doesNotContain("orders_e2e");

        tEnv.executeSql("DROP DATABASE " + DB_NAME);
        assertThat(sql("SHOW DATABASES")).extracting(r -> r.getField(0)).doesNotContain(DB_NAME);
    }

    @Test
    void testDataRoundTripThroughCatalogResolvedTables(@TempDir Path sinkDir) throws Exception {
        // Batch mode so the filesystem sink finalizes its files on job completion
        // (a streaming filesystem sink only commits files on checkpoints).
        TableEnvironment batchEnv =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        batchEnv.executeSql(
                "CREATE CATALOG "
                        + CATALOG_NAME
                        + " WITH ('type' = 'glue', 'region' = '"
                        + REGION
                        + "', 'default-database' = 'default')");
        batchEnv.executeSql("USE CATALOG " + CATALOG_NAME);
        batchEnv.executeSql("CREATE DATABASE IF NOT EXISTS " + DATA_DB_NAME);

        // Both tables are REGISTERED IN REAL GLUE; execution resolves connector, format and
        // schema from what Glue stored - proving property and type fidelity through the wire.
        batchEnv.executeSql(
                "CREATE TABLE "
                        + DATA_DB_NAME
                        + ".orders_src ("
                        + "  orderId STRING,"
                        + "  orderTotal DOUBLE,"
                        + "  orderTime TIMESTAMP(3)"
                        + ") WITH ("
                        + "  'connector' = 'datagen',"
                        + "  'number-of-rows' = '100',"
                        + "  'fields.orderId.length' = '12'"
                        + ")");
        batchEnv.executeSql(
                "CREATE TABLE "
                        + DATA_DB_NAME
                        + ".orders_copy ("
                        + "  orderId STRING,"
                        + "  orderTotal DOUBLE,"
                        + "  orderTime TIMESTAMP(3)"
                        + ") WITH ("
                        + "  'connector' = 'filesystem',"
                        + "  'path' = '"
                        + sinkDir.toUri()
                        + "',"
                        + "  'format' = 'json'"
                        + ")");

        // WRITE: a real Flink job from the catalog-resolved datagen source into the
        // catalog-resolved filesystem sink.
        batchEnv.executeSql(
                        "INSERT INTO "
                                + DATA_DB_NAME
                                + ".orders_copy SELECT * FROM "
                                + DATA_DB_NAME
                                + ".orders_src")
                .await();

        // READ: back through the catalog, projecting and filtering on restored camelCase
        // columns to prove runtime column resolution, not just DESCRIBE output.
        List<Row> rows = new ArrayList<>();
        batchEnv.executeSql(
                        "SELECT orderId, orderTotal FROM "
                                + DATA_DB_NAME
                                + ".orders_copy WHERE orderId IS NOT NULL"
                                + " AND orderTime IS NOT NULL")
                .collect()
                .forEachRemaining(rows::add);
        assertThat(rows).hasSize(100);
        assertThat(rows)
                .allSatisfy(
                        row -> {
                            assertThat((String) row.getField(0)).hasSize(12);
                            assertThat(row.getField(1)).isInstanceOf(Double.class);
                        });

        // Teardown through SQL DDL against real Glue.
        batchEnv.executeSql("DROP TABLE " + DATA_DB_NAME + ".orders_src");
        batchEnv.executeSql("DROP TABLE " + DATA_DB_NAME + ".orders_copy");
        batchEnv.executeSql("DROP DATABASE " + DATA_DB_NAME);
    }

    private static List<Row> sql(String statement) {
        List<Row> rows = new ArrayList<>();
        tEnv.executeSql(statement).collect().forEachRemaining(rows::add);
        return rows;
    }
}
