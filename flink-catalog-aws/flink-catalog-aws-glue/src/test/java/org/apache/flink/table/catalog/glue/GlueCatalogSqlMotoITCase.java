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
 * <p>The factory-built client is pointed at moto through the catalog's {@code aws.endpoint} option
 * (handled by the shared AWS client-creation path) and system-property credentials, so this test
 * also covers the factory's AWS option pass-through.
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

        // Credentials come from system properties (first in the default credentials chain);
        // the endpoint is routed to moto via the catalog's own 'aws.endpoint' option below.
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
                        + "'aws.endpoint' = '"
                        + endpoint
                        + "', "
                        + "'default-database' = 'default')");
        tEnv.executeSql("USE CATALOG glue_moto");
    }

    @AfterAll
    static void tearDown() {
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

    @Test
    void testSchemaFidelityRoundTripThroughSql() {
        // Note: watermark and computed-column DDL is exercised in the fake-backed and
        // real-AWS tiers instead. Validating any SQL expression makes the planner probe the
        // catalog's function APIs, and moto does not implement the Glue UDF API (HTTP 500).
        tEnv.executeSql("CREATE DATABASE sql_fidelity_db");
        tEnv.executeSql(
                "CREATE TABLE sql_fidelity_db.events ("
                        + "  userId STRING,"
                        + "  eventTime TIMESTAMP(3),"
                        + "  price DOUBLE,"
                        + "  kafkaOffset BIGINT METADATA FROM 'offset' VIRTUAL,"
                        + "  PRIMARY KEY (userId) NOT ENFORCED"
                        + ") WITH ("
                        + "  'connector' = 'kinesis',"
                        + "  'stream.arn' = 'arn:aws:kinesis:us-east-1:000000000000:stream/events'"
                        + ")");

        // Read the table back through the catalog: primary key and metadata columns must
        // survive the round-trip through Glue table parameters.
        String createTable =
                sql("SHOW CREATE TABLE sql_fidelity_db.events").get(0).getField(0).toString();

        assertThat(createTable)
                .contains("`kafkaOffset` BIGINT METADATA FROM 'offset' VIRTUAL")
                .contains("PRIMARY KEY (`userId`) NOT ENFORCED");

        // Column order must be preserved, including the interleaved metadata column.
        assertThat(sql("DESCRIBE sql_fidelity_db.events"))
                .extracting(row -> row.getField(0))
                .containsExactly("userId", "eventTime", "price", "kafkaOffset");
    }
}
