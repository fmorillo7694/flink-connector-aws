/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.catalog.glue;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedCatalogView;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.catalog.glue.util.GlueTestClientFactory;
import org.apache.flink.table.catalog.glue.util.RealGlueCleanupExtension;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.Table;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that schema features which AWS Glue columns cannot represent - computed columns, metadata
 * columns, watermarks, and primary keys - survive a full create/read round-trip, and that views do
 * not leak their columns into Glue partition keys.
 */
@ExtendWith(RealGlueCleanupExtension.class)
class GlueCatalogSchemaFidelityTest {

    private GlueClient glueClient;
    private GlueCatalog glueCatalog;
    private String databaseName;
    private String glueDatabaseName;

    @BeforeEach
    void setUp() throws Exception {
        glueClient = GlueTestClientFactory.createClient();
        glueCatalog = new GlueCatalog("test_catalog", "default", "us-east-1", glueClient);
        databaseName = GlueTestClientFactory.uniqueName("fidelitydb");
        glueDatabaseName = databaseName.toLowerCase();
        glueCatalog.createDatabase(
                databaseName, new CatalogDatabaseImpl(new HashMap<>(), "fidelity tests"), false);
    }

    @AfterEach
    void tearDown() {
        if (glueCatalog != null) {
            glueCatalog.close();
        }
    }

    @Test
    void testWatermarkPrimaryKeyAndNonPhysicalColumnsRoundTrip() throws Exception {
        String tableName = GlueTestClientFactory.uniqueName("fidelitytable");
        ObjectPath path = new ObjectPath(databaseName, tableName);

        List<Column> columns =
                Arrays.asList(
                        Column.physical("userId", DataTypes.STRING().notNull()),
                        Column.physical("eventTime", DataTypes.TIMESTAMP(3)),
                        Column.physical("price", DataTypes.DOUBLE()),
                        Column.computed(
                                "doublePrice", sqlExpression("`price` * 2", DataTypes.DOUBLE())),
                        Column.metadata("kafkaOffset", DataTypes.BIGINT(), "offset", true));
        ResolvedSchema resolvedSchema =
                new ResolvedSchema(
                        columns,
                        Collections.singletonList(
                                WatermarkSpec.of(
                                        "eventTime",
                                        sqlExpression(
                                                "`eventTime` - INTERVAL '5' SECOND",
                                                DataTypes.TIMESTAMP(3)))),
                        UniqueConstraint.primaryKey(
                                "PK_userId", Collections.singletonList("userId")));

        Map<String, String> options = new HashMap<>();
        options.put("connector", "kinesis");
        options.put("stream.arn", "arn:aws:kinesis:us-east-1:000000000000:stream/fidelity");

        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().fromResolvedSchema(resolvedSchema).build())
                        .comment("schema fidelity round-trip")
                        .partitionKeys(Collections.emptyList())
                        .options(options)
                        .build();
        glueCatalog.createTable(
                path, new ResolvedCatalogTable(catalogTable, resolvedSchema), false);

        CatalogBaseTable readBack = glueCatalog.getTable(path);
        Schema schema = readBack.getUnresolvedSchema();

        // Column order preserved, including the interleaved non-physical columns.
        assertThat(schema.getColumns())
                .extracting(Schema.UnresolvedColumn::getName)
                .containsExactly("userId", "eventTime", "price", "doublePrice", "kafkaOffset");

        // Computed and metadata columns restored with the right kinds.
        assertThat(schema.getColumns().get(3)).isInstanceOf(Schema.UnresolvedComputedColumn.class);
        assertThat(schema.getColumns().get(4)).isInstanceOf(Schema.UnresolvedMetadataColumn.class);
        Schema.UnresolvedMetadataColumn metadataColumn =
                (Schema.UnresolvedMetadataColumn) schema.getColumns().get(4);
        assertThat(metadataColumn.getMetadataKey()).isEqualTo("offset");
        assertThat(metadataColumn.isVirtual()).isTrue();

        // Watermark and primary key restored.
        assertThat(schema.getWatermarkSpecs()).hasSize(1);
        assertThat(schema.getWatermarkSpecs().get(0).getColumnName()).isEqualTo("eventTime");
        assertThat(schema.getPrimaryKey()).isPresent();
        assertThat(schema.getPrimaryKey().get().getColumnNames()).containsExactly("userId");

        // Internal flink.schema.* parameters must not leak into the exposed options.
        assertThat(readBack.getOptions().keySet())
                .noneMatch(key -> key.startsWith("flink.schema."));

        // The Glue table itself only carries the physical columns.
        Table glueTable = getRawGlueTable(tableName.toLowerCase());
        assertThat(glueTable.storageDescriptor().columns())
                .extracting(software.amazon.awssdk.services.glue.model.Column::name)
                .containsExactly("userid", "eventtime", "price");
    }

    @Test
    void testViewColumnsAreNotPersistedAsPartitionKeys() throws Exception {
        String viewName = GlueTestClientFactory.uniqueName("fidelityview");
        ObjectPath path = new ObjectPath(databaseName, viewName);

        ResolvedSchema resolvedSchema =
                ResolvedSchema.of(
                        Column.physical("userId", DataTypes.STRING()),
                        Column.physical("total", DataTypes.DOUBLE()));
        CatalogView catalogView =
                CatalogView.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        "a view",
                        "SELECT userId, total FROM src",
                        "SELECT userId, total FROM src",
                        Collections.emptyMap());
        glueCatalog.createTable(path, new ResolvedCatalogView(catalogView, resolvedSchema), false);

        // The stored Glue table must not have the view's columns duplicated as partition keys.
        Table glueTable = getRawGlueTable(viewName.toLowerCase());
        assertThat(glueTable.partitionKeys()).isEmpty();
        assertThat(glueTable.storageDescriptor().columns()).hasSize(2);

        // And the view must read back cleanly with its columns intact.
        CatalogBaseTable readBack = glueCatalog.getTable(path);
        assertThat(readBack.getTableKind()).isEqualTo(CatalogBaseTable.TableKind.VIEW);
        assertThat(readBack.getUnresolvedSchema().getColumns())
                .extracting(Schema.UnresolvedColumn::getName)
                .containsExactly("userId", "total");
    }

    private Table getRawGlueTable(String glueTableName) {
        return glueClient
                .getTable(
                        GetTableRequest.builder()
                                .databaseName(glueDatabaseName)
                                .name(glueTableName)
                                .build())
                .table();
    }

    /**
     * Returns a minimal {@link ResolvedExpression} whose serializable form is the given SQL string,
     * mirroring what the planner produces when resolving DDL.
     */
    private static ResolvedExpression sqlExpression(String sql, DataType outputDataType) {
        return new ResolvedExpression() {
            @Override
            public DataType getOutputDataType() {
                return outputDataType;
            }

            @Override
            public List<ResolvedExpression> getResolvedChildren() {
                return Collections.emptyList();
            }

            @Override
            public String asSerializableString() {
                return sql;
            }

            @Override
            public String asSummaryString() {
                return sql;
            }

            @Override
            public List<org.apache.flink.table.expressions.Expression> getChildren() {
                return Collections.emptyList();
            }

            @Override
            public <R> R accept(ExpressionVisitor<R> visitor) {
                return visitor.visit(this);
            }
        };
    }
}
