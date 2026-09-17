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

package org.apache.flink.table.catalog.glue;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;

import java.net.URI;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SQL-path integration test for the Glue catalog against a moto Glue emulator.
 *
 * <p>Unlike {@link GlueCatalogMotoITCase}, which drives {@link GlueCatalog} methods directly, this
 * test exercises the full user path: {@code CREATE CATALOG ... WITH ('type'='glue')} discovers
 * {@code GlueCatalogFactory} via SPI, the factory builds its own {@code GlueClient}, and all
 * catalog operations flow through Flink SQL DDL and the planner down to the Glue wire protocol.
 *
 * <p>The factory-built client is pointed at moto through the AWS SDK's service-specific endpoint
 * system property ({@code aws.endpointUrlGlue}) and system-property credentials - no
 * endpoint-override code path exists in the factory, and none is needed.
 */
@Testcontainers
class GlueCatalogSqlMotoITCase {

    private static final int MOTO_PORT = 5000;

    @Container
    private static final GenericContainer<?> MOTO =
            new GenericContainer<>("motoserver/moto:5.0.28").withExposedPorts(MOTO_PORT);

    private static GlueClient seedClient;
    private static TableEnvironment tEnv;

    @BeforeAll
    static void setUp() {
        String endpoint =
                String.format("http://%s:%d", MOTO.getHost(), MOTO.getMappedPort(MOTO_PORT));

        // Route the factory-built GlueClient to moto via the SDK's service-specific endpoint
        // and system-property credentials (first in the default credentials chain).
        System.setProperty("aws.endpointUrlGlue", endpoint);
        System.setProperty("aws.accessKeyId", "testing");
        System.setProperty("aws.secretAccessKey", "testing");

        // Seed the default database (USE CATALOG validates it exists).
        seedClient =
                GlueClient.builder()
                        .endpointOverride(URI.create(endpoint))
                        .region(Region.US_EAST_1)
                        .credentialsProvider(
                                StaticCredentialsProvider.create(
                                        AwsBasicCredentials.create("testing", "testing")))
                        .build();
        seedClient.createDatabase(builder -> builder.databaseInput(db -> db.name("default")));

        tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.executeSql(
                "CREATE CATALOG glue_moto WITH ("
                        + "'type' = 'glue', "
                        + "'region' = 'us-east-1', "
                        + "'default-database' = 'default')");
        tEnv.executeSql("USE CATALOG glue_moto");
    }

    @AfterAll
    static void tearDown() {
        System.clearProperty("aws.endpointUrlGlue");
        System.clearProperty("aws.accessKeyId");
        System.clearProperty("aws.secretAccessKey");
        if (seedClient != null) {
            seedClient.close();
        }
    }

    private static List<Row> sql(String statement) {
        return CollectionUtil.iteratorToList(tEnv.executeSql(statement).collect());
    }

    @Test
    void testShowDatabasesThroughSql() {
        List<Row> databases = sql("SHOW DATABASES");

        assertThat(databases).extracting(row -> row.getField(0)).contains("default");
    }

    @Test
    void testDatabaseDdlThroughSql() {
        tEnv.executeSql("CREATE DATABASE sql_ddl_db COMMENT 'created via SQL DDL'");

        assertThat(sql("SHOW DATABASES")).extracting(row -> row.getField(0)).contains("sql_ddl_db");
    }

    @Test
    void testTableDdlRoundTripThroughSql() {
        tEnv.executeSql("CREATE DATABASE sql_table_db");
        tEnv.executeSql(
                "CREATE TABLE sql_table_db.orders ("
                        + "  user_id STRING,"
                        + "  order_total DOUBLE"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = 'arn:aws:kinesis:us-east-1:000000000000:stream/orders'"
                        + ")");

        assertThat(sql("SHOW TABLES IN sql_table_db"))
                .extracting(row -> row.getField(0))
                .contains("orders");

        // Schema must survive the wire round-trip through moto and come back through DESCRIBE.
        List<Row> columns = sql("DESCRIBE sql_table_db.orders");
        assertThat(columns).hasSize(2);
        assertThat(columns.get(0).getField(0)).isEqualTo("user_id");
        assertThat(columns.get(0).getField(1)).isEqualTo("STRING");
        assertThat(columns.get(1).getField(0)).isEqualTo("order_total");
        assertThat(columns.get(1).getField(1)).isEqualTo("DOUBLE");

        tEnv.executeSql("DROP TABLE sql_table_db.orders");
        assertThat(sql("SHOW TABLES IN sql_table_db"))
                .extracting(row -> row.getField(0))
                .doesNotContain("orders");
    }
}
