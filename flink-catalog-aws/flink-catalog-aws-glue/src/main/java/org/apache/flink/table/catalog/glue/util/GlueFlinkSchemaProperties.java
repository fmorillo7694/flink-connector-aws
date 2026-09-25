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

package org.apache.flink.table.catalog.glue.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;

/**
 * Persists the parts of a Flink schema that AWS Glue columns cannot represent - computed columns,
 * metadata columns, watermarks, and the primary key - as Glue table parameters, and restores them
 * when a table is read back.
 *
 * <p>Physical columns are stored as native Glue columns (so other engines can read the table),
 * while everything else round-trips through {@code flink.schema.*} parameters. Without this, {@code
 * CREATE TABLE} statements with watermarks or primary keys would silently lose them on read back,
 * and computed or metadata columns would be corrupted into physical columns.
 */
@Internal
public final class GlueFlinkSchemaProperties {

    /** Prefix shared by all schema-fidelity parameters written by this class. */
    public static final String SCHEMA_PARAMETER_PREFIX = "flink.schema.";

    private static final String COLUMN_PREFIX = SCHEMA_PARAMETER_PREFIX + "column.";
    private static final String COMPUTED_NAME_SUFFIX = ".computed.name";
    private static final String COMPUTED_EXPR_SUFFIX = ".computed.expr";
    private static final String METADATA_NAME_SUFFIX = ".metadata.name";
    private static final String METADATA_DATA_TYPE_SUFFIX = ".metadata.data-type";
    private static final String METADATA_KEY_SUFFIX = ".metadata.key";
    private static final String METADATA_VIRTUAL_SUFFIX = ".metadata.virtual";
    private static final String PHYSICAL_DATA_TYPE_SUFFIX = ".data-type";

    private static final String WATERMARK_PREFIX = SCHEMA_PARAMETER_PREFIX + "watermark.";
    private static final String WATERMARK_ROWTIME_SUFFIX = ".rowtime";
    private static final String WATERMARK_STRATEGY_EXPR_SUFFIX = ".strategy.expr";

    private static final String PRIMARY_KEY_NAME = SCHEMA_PARAMETER_PREFIX + "primary-key.name";
    private static final String PRIMARY_KEY_COLUMNS =
            SCHEMA_PARAMETER_PREFIX + "primary-key.columns";

    /**
     * Comma-joined names of physical columns declared NOT NULL. Glue type strings carry no
     * nullability, so without this the restored primary key would fail schema resolution ("Invalid
     * primary key: column is nullable").
     */
    private static final String NOT_NULL_COLUMNS = SCHEMA_PARAMETER_PREFIX + "not-null-columns";

    private static final GlueTypeConverter TYPE_CONVERTER = new GlueTypeConverter();

    private GlueFlinkSchemaProperties() {}

    /**
     * Serializes the non-physical parts of the given schema into the target parameter map.
     *
     * @param resolvedSchema the resolved Flink schema
     * @param targetParameters the mutable Glue table parameter map to write into
     * @throws CatalogException if an expression cannot be serialized to SQL
     */
    public static void serializeNonPhysicalSchema(
            ResolvedSchema resolvedSchema, Map<String, String> targetParameters) {
        List<Column> columns = resolvedSchema.getColumns();
        List<String> notNullColumns = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            if (column instanceof Column.PhysicalColumn) {
                if (!column.getDataType().getLogicalType().isNullable()) {
                    notNullColumns.add(column.getName());
                }
                // Glue's Hive-style type strings are lossy for some types (for example
                // TIMESTAMP(3) has no Glue representation and would come back as
                // TIMESTAMP(6)). When the Glue round-trip does not reproduce the declared
                // type, record the original so the read path can restore it exactly.
                LogicalType declared = column.getDataType().getLogicalType();
                LogicalType glueRoundTrip =
                        TYPE_CONVERTER
                                .toFlinkDataType(
                                        TYPE_CONVERTER.toGlueDataType(column.getDataType()))
                                .getLogicalType();
                if (!declared.copy(true).equals(glueRoundTrip.copy(true))) {
                    targetParameters.put(
                            COLUMN_PREFIX + i + PHYSICAL_DATA_TYPE_SUFFIX,
                            declared.asSerializableString());
                }
            }
            if (column instanceof Column.ComputedColumn) {
                Column.ComputedColumn computed = (Column.ComputedColumn) column;
                targetParameters.put(COLUMN_PREFIX + i + COMPUTED_NAME_SUFFIX, column.getName());
                targetParameters.put(
                        COLUMN_PREFIX + i + COMPUTED_EXPR_SUFFIX,
                        serializeExpression(column.getName(), computed.getExpression()));
            } else if (column instanceof Column.MetadataColumn) {
                Column.MetadataColumn metadata = (Column.MetadataColumn) column;
                final int position = i;
                targetParameters.put(COLUMN_PREFIX + i + METADATA_NAME_SUFFIX, column.getName());
                targetParameters.put(
                        COLUMN_PREFIX + i + METADATA_DATA_TYPE_SUFFIX,
                        column.getDataType().getLogicalType().asSerializableString());
                metadata.getMetadataKey()
                        .ifPresent(
                                key ->
                                        targetParameters.put(
                                                COLUMN_PREFIX + position + METADATA_KEY_SUFFIX,
                                                key));
                targetParameters.put(
                        COLUMN_PREFIX + i + METADATA_VIRTUAL_SUFFIX,
                        String.valueOf(metadata.isVirtual()));
            }
        }

        if (!notNullColumns.isEmpty()) {
            targetParameters.put(NOT_NULL_COLUMNS, String.join(",", notNullColumns));
        }

        List<WatermarkSpec> watermarkSpecs = resolvedSchema.getWatermarkSpecs();
        for (int i = 0; i < watermarkSpecs.size(); i++) {
            WatermarkSpec spec = watermarkSpecs.get(i);
            targetParameters.put(
                    WATERMARK_PREFIX + i + WATERMARK_ROWTIME_SUFFIX, spec.getRowtimeAttribute());
            targetParameters.put(
                    WATERMARK_PREFIX + i + WATERMARK_STRATEGY_EXPR_SUFFIX,
                    serializeExpression(spec.getRowtimeAttribute(), spec.getWatermarkExpression()));
        }

        resolvedSchema
                .getPrimaryKey()
                .ifPresent(
                        primaryKey -> {
                            targetParameters.put(PRIMARY_KEY_NAME, primaryKey.getName());
                            targetParameters.put(
                                    PRIMARY_KEY_COLUMNS, String.join(",", primaryKey.getColumns()));
                        });
    }

    /**
     * Returns whether the given Glue table parameter key was written by {@link
     * #serializeNonPhysicalSchema} and must therefore be hidden from the table options exposed to
     * users.
     *
     * @param parameterKey the Glue table parameter key
     * @return true when the key is an internal schema-fidelity parameter
     */
    public static boolean isSchemaParameter(String parameterKey) {
        return parameterKey.startsWith(SCHEMA_PARAMETER_PREFIX);
    }

    /**
     * Restores computed and metadata columns from the Glue table parameters, merging them with the
     * physical columns at their originally declared positions, then re-applies watermarks and the
     * primary key.
     *
     * @param parameters the Glue table parameters (may be null)
     * @param physicalColumnAdders ordered adders for the physical columns read from Glue
     * @param schemaBuilder the schema builder to populate
     */
    public static void applySchemaWithNonPhysicalColumns(
            Map<String, String> parameters,
            List<PhysicalColumnSpec> physicalColumns,
            Schema.Builder schemaBuilder) {

        // Rebuild non-physical columns keyed by their original position in the schema.
        TreeMap<Integer, Consumer<Schema.Builder>> nonPhysicalByPosition = new TreeMap<>();
        if (parameters != null) {
            for (Map.Entry<String, String> entry : parameters.entrySet()) {
                String key = entry.getKey();
                if (key.startsWith(COLUMN_PREFIX) && key.endsWith(COMPUTED_NAME_SUFFIX)) {
                    int position = parsePosition(key, COMPUTED_NAME_SUFFIX);
                    String name = entry.getValue();
                    String expression =
                            parameters.get(COLUMN_PREFIX + position + COMPUTED_EXPR_SUFFIX);
                    if (expression != null) {
                        nonPhysicalByPosition.put(
                                position, builder -> builder.columnByExpression(name, expression));
                    }
                } else if (key.startsWith(COLUMN_PREFIX) && key.endsWith(METADATA_NAME_SUFFIX)) {
                    int position = parsePosition(key, METADATA_NAME_SUFFIX);
                    String name = entry.getValue();
                    String dataType =
                            parameters.get(COLUMN_PREFIX + position + METADATA_DATA_TYPE_SUFFIX);
                    String metadataKey =
                            parameters.get(COLUMN_PREFIX + position + METADATA_KEY_SUFFIX);
                    boolean virtual =
                            Boolean.parseBoolean(
                                    parameters.get(
                                            COLUMN_PREFIX + position + METADATA_VIRTUAL_SUFFIX));
                    if (dataType != null) {
                        nonPhysicalByPosition.put(
                                position,
                                builder ->
                                        builder.columnByMetadata(
                                                name,
                                                DataTypes.of(dataType),
                                                metadataKey,
                                                virtual));
                    }
                }
            }
        }

        // Merge physical and non-physical columns back into the declared order.
        int totalColumns = physicalColumns.size() + nonPhysicalByPosition.size();
        int physicalIndex = 0;
        for (int position = 0; position < totalColumns; position++) {
            Consumer<Schema.Builder> nonPhysical = nonPhysicalByPosition.get(position);
            if (nonPhysical != null) {
                nonPhysical.accept(schemaBuilder);
            } else if (physicalIndex < physicalColumns.size()) {
                addPhysicalColumn(
                        parameters, position, physicalColumns.get(physicalIndex++), schemaBuilder);
            }
        }
        // Defensive: append any remaining physical columns (e.g. malformed position metadata).
        while (physicalIndex < physicalColumns.size()) {
            addPhysicalColumn(parameters, -1, physicalColumns.get(physicalIndex++), schemaBuilder);
        }

        if (parameters == null) {
            return;
        }

        // Restore watermarks in declared order.
        for (int i = 0; ; i++) {
            String rowtime = parameters.get(WATERMARK_PREFIX + i + WATERMARK_ROWTIME_SUFFIX);
            String expression =
                    parameters.get(WATERMARK_PREFIX + i + WATERMARK_STRATEGY_EXPR_SUFFIX);
            if (rowtime == null || expression == null) {
                break;
            }
            schemaBuilder.watermark(rowtime, expression);
        }

        // Restore the primary key.
        String primaryKeyColumns = parameters.get(PRIMARY_KEY_COLUMNS);
        if (primaryKeyColumns != null && !primaryKeyColumns.isEmpty()) {
            String[] columns = primaryKeyColumns.split(",", -1);
            String constraintName = parameters.get(PRIMARY_KEY_NAME);
            if (constraintName != null && !constraintName.isEmpty()) {
                schemaBuilder.primaryKeyNamed(constraintName, columns);
            } else {
                schemaBuilder.primaryKey(columns);
            }
        }
    }

    /**
     * Returns the names of physical columns that were declared NOT NULL, as recorded by {@link
     * #serializeNonPhysicalSchema}. Glue type strings carry no nullability, so the read path uses
     * this to restore the declared constraint.
     *
     * @param parameters the Glue table parameters (may be null)
     * @return the NOT NULL column names; empty when none were recorded
     */
    public static Set<String> getNotNullColumns(Map<String, String> parameters) {
        if (parameters == null || !parameters.containsKey(NOT_NULL_COLUMNS)) {
            return Collections.emptySet();
        }
        return new HashSet<>(Arrays.asList(parameters.get(NOT_NULL_COLUMNS).split(",", -1)));
    }

    private static void addPhysicalColumn(
            Map<String, String> parameters,
            int position,
            PhysicalColumnSpec spec,
            Schema.Builder schemaBuilder) {
        String declaredType =
                parameters == null || position < 0
                        ? null
                        : parameters.get(COLUMN_PREFIX + position + PHYSICAL_DATA_TYPE_SUFFIX);
        if (declaredType != null) {
            // The declared type could not be represented exactly in Glue; restore the
            // recorded original (it carries its own nullability).
            schemaBuilder.column(spec.name, DataTypes.of(declaredType));
        } else {
            schemaBuilder.column(spec.name, spec.dataType);
        }
    }

    /** The name and converted data type of a physical column read back from Glue. */
    public static final class PhysicalColumnSpec {
        private final String name;
        private final org.apache.flink.table.types.DataType dataType;

        public PhysicalColumnSpec(String name, org.apache.flink.table.types.DataType dataType) {
            this.name = name;
            this.dataType = dataType;
        }
    }

    private static int parsePosition(String key, String suffix) {
        String position = key.substring(COLUMN_PREFIX.length(), key.length() - suffix.length());
        return Integer.parseInt(position);
    }

    private static String serializeExpression(
            String columnOrAttributeName,
            org.apache.flink.table.expressions.ResolvedExpression expression) {
        try {
            return expression.asSerializableString();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Expression for '%s' cannot be persisted to AWS Glue because it is "
                                    + "not serializable to SQL: %s",
                            columnOrAttributeName, expression.asSummaryString()),
                    e);
        }
    }
}
