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

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Utility class for working with Glue tables, including transforming Glue-specific metadata into
 * Flink-compatible objects.
 */
public class GlueTableUtils {

    /** Logger for logging Glue table operations. */
    private static final Logger LOG = LoggerFactory.getLogger(GlueTableUtils.class);

    /** Glue type converter for type conversions between Flink and Glue types. */
    private final GlueTypeConverter glueTypeConverter;

    /**
     * Constructor to initialize GlueTableUtils with a GlueTypeConverter.
     *
     * @param glueTypeConverter The GlueTypeConverter instance for type mapping.
     */
    public GlueTableUtils(GlueTypeConverter glueTypeConverter) {
        this.glueTypeConverter = glueTypeConverter;
    }

    /**
     * Builds a Glue StorageDescriptor from the given table properties, columns, and location.
     *
     * @param tableProperties Table properties for the Glue table.
     * @param glueColumns Columns to be included in the StorageDescriptor.
     * @param tableLocation Location of the Glue table.
     * @return A newly built StorageDescriptor object.
     */
    public StorageDescriptor buildStorageDescriptor(
            Map<String, String> tableProperties, List<Column> glueColumns, String tableLocation) {

        return StorageDescriptor.builder().columns(glueColumns).location(tableLocation).build();
    }

    /**
     * Extracts the table location based on the table properties and the table path. First, it
     * checks for a location key from the connector registry. If no such key is found, it uses a
     * default path based on the table path.
     *
     * @param tableProperties Table properties containing the connector and location.
     * @param tablePath The Flink ObjectPath representing the table.
     * @return The location of the Glue table.
     */
    public String extractTableLocation(Map<String, String> tableProperties, ObjectPath tablePath) {
        String connectorType = tableProperties.get("connector");
        if (connectorType != null) {
            String locationKey = ConnectorRegistry.getLocationKey(connectorType);
            if (locationKey != null && tableProperties.containsKey(locationKey)) {
                String location = tableProperties.get(locationKey);
                return location;
            }
        }

        String defaultLocation =
                tablePath.getDatabaseName() + "/tables/" + tablePath.getObjectName();
        return defaultLocation;
    }

    /**
     * Converts a Flink column to a Glue column. The column's data type is converted using the
     * GlueTypeConverter.
     *
     * @param flinkColumn The Flink column to be converted.
     * @return The corresponding Glue column.
     */
    public Column mapFlinkColumnToGlueColumn(org.apache.flink.table.catalog.Column flinkColumn) {
        String glueType = glueTypeConverter.toGlueDataType(flinkColumn.getDataType());

        // AWS Glue lowercases column names on CreateTable/UpdateTable (verified empirically:
        // a column created as "userId" is stored and returned as "userid"). To preserve the
        // declared case, store the lowercased name explicitly and stash the original name in
        // the "originalName" column parameter, which the read path restores.
        String originalName = flinkColumn.getName();
        String glueName = originalName.toLowerCase();

        Column.Builder builder = Column.builder().name(glueName).type(glueType);
        if (!glueName.equals(originalName)) {
            builder.parameters(
                    Collections.singletonMap(
                            GlueCatalogConstants.ORIGINAL_COLUMN_NAME, originalName));
        }
        return builder.build();
    }

    /**
     * Converts a Glue table into a Flink schema. Each Glue column is mapped to a Flink column using
     * the GlueTypeConverter. Partition columns (stored at the Glue table level, not in the storage
     * descriptor) are appended after the data columns so that declared partition keys are part of
     * the Flink schema, as required by {@code CatalogTable}. Computed and metadata columns,
     * watermarks, and the primary key - which Glue columns cannot represent - are restored from the
     * {@code flink.schema.*} table parameters written by {@link GlueFlinkSchemaProperties}.
     *
     * @param glueTable The Glue table from which the schema will be derived.
     * @return A Flink schema constructed from the Glue table's columns.
     */
    public Schema getSchemaFromGlueTable(Table glueTable) {
        Schema.Builder schemaBuilder = Schema.newBuilder();

        List<GlueFlinkSchemaProperties.PhysicalColumnSpec> physicalColumns = new ArrayList<>();
        java.util.Set<String> notNullColumns =
                GlueFlinkSchemaProperties.getNotNullColumns(glueTable.parameters());
        List<Column> columns =
                glueTable.storageDescriptor() != null
                        ? glueTable.storageDescriptor().columns()
                        : Collections.emptyList();
        for (Column column : columns) {
            String columnName = getColumnName(column);
            DataType convertedType = glueTypeConverter.toFlinkDataType(column.type());
            // Glue type strings carry no nullability; restore the declared NOT NULL
            // constraints (required for primary key columns to resolve).
            DataType flinkDataType =
                    notNullColumns.contains(columnName) ? convertedType.notNull() : convertedType;
            physicalColumns.add(
                    new GlueFlinkSchemaProperties.PhysicalColumnSpec(columnName, flinkDataType));
        }

        // Partition columns live in Table.partitionKeys(), not in the storage descriptor.
        // Their declared case is restored from the table-level parameter (Glue rejects
        // column-level parameters on partition columns).
        if (glueTable.partitionKeys() != null && !glueTable.partitionKeys().isEmpty()) {
            List<String> partitionKeyNames = getPartitionKeyNames(glueTable);
            List<Column> partitionColumns = glueTable.partitionKeys();
            for (int i = 0; i < partitionColumns.size(); i++) {
                Column partitionColumn = partitionColumns.get(i);
                DataType convertedType = glueTypeConverter.toFlinkDataType(partitionColumn.type());
                String partitionKeyName = partitionKeyNames.get(i);
                DataType flinkDataType =
                        notNullColumns.contains(partitionKeyName)
                                ? convertedType.notNull()
                                : convertedType;
                physicalColumns.add(
                        new GlueFlinkSchemaProperties.PhysicalColumnSpec(
                                partitionKeyName, flinkDataType));
            }
        }

        // Merge in computed/metadata columns and re-apply watermarks and the primary key.
        GlueFlinkSchemaProperties.applySchemaWithNonPhysicalColumns(
                glueTable.parameters(), physicalColumns, schemaBuilder);

        return schemaBuilder.build();
    }

    /**
     * Returns the Flink-facing partition key names of a Glue table, in declared order. The original
     * (case-preserved) names come from the table-level {@link
     * GlueCatalogConstants#ORIGINAL_PARTITION_KEYS} parameter when present (written by this catalog
     * because Glue rejects column-level parameters on partition columns), falling back to the
     * per-column resolution for tables written by other writers or older versions.
     *
     * @param glueTable The Glue table.
     * @return Ordered partition key names to expose to Flink; empty when not partitioned.
     */
    public static List<String> getPartitionKeyNames(Table glueTable) {
        if (glueTable.partitionKeys() == null || glueTable.partitionKeys().isEmpty()) {
            return Collections.emptyList();
        }
        List<Column> partitionColumns = glueTable.partitionKeys();
        if (glueTable.parameters() != null
                && glueTable
                        .parameters()
                        .containsKey(GlueCatalogConstants.ORIGINAL_PARTITION_KEYS)) {
            String[] originalNames =
                    glueTable
                            .parameters()
                            .get(GlueCatalogConstants.ORIGINAL_PARTITION_KEYS)
                            .split(",", -1);
            if (originalNames.length == partitionColumns.size()) {
                return java.util.Arrays.asList(originalNames);
            }
            LOG.warn(
                    "Ignoring malformed {} parameter on table {}: {} entries for {} partition keys",
                    GlueCatalogConstants.ORIGINAL_PARTITION_KEYS,
                    glueTable.name(),
                    originalNames.length,
                    partitionColumns.size());
        }
        List<String> names = new java.util.ArrayList<>(partitionColumns.size());
        for (Column partitionColumn : partitionColumns) {
            names.add(getColumnName(partitionColumn));
        }
        return names;
    }

    /**
     * Returns the Flink-facing name of a Glue column: the original (case-preserved) name from the
     * "originalName" column parameter when present, otherwise the Glue-stored name. Glue lowercases
     * column names on write, so this parameter is how the declared case survives the round-trip.
     *
     * @param column The Glue column.
     * @return The column name to expose to Flink.
     */
    public static String getColumnName(Column column) {
        if (column.parameters() != null
                && column.parameters().containsKey(GlueCatalogConstants.ORIGINAL_COLUMN_NAME)) {
            return column.parameters().get(GlueCatalogConstants.ORIGINAL_COLUMN_NAME);
        }
        return column.name();
    }
}
