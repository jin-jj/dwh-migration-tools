/*
 * Copyright 2022-2025 Google LLC
 * Copyright 2013-2021 CompilerWorks
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
package com.google.edwmigration.dumper.application.dumper.connector.databricks.output;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import java.util.Collection;
import java.util.Locale;

/** Output archive contract and catalog/schema filter for the Databricks metadata connector. */
public interface DatabricksMetadataDumpFormat {

  String FORMAT_NAME = "databricks.dump.zip";

  String UNITY_CATALOG_CATALOGS_ENTRY_NAME = "catalogs.csv";
  String UNITY_CATALOG_SCHEMATA_ENTRY_NAME = "schemata.csv";
  String UNITY_CATALOG_TABLES_ENTRY_NAME = "tables.csv";
  String UNITY_CATALOG_COLUMNS_ENTRY_NAME = "columns.csv";
  String UNITY_CATALOG_VIEWS_ENTRY_NAME = "views.csv";
  String UNITY_CATALOG_TABLE_CONSTRAINTS_ENTRY_NAME = "table_constraints.csv";
  String UNITY_CATALOG_COLUMN_MASKS_ENTRY_NAME = "column_masks.csv";
  String UNITY_CATALOG_ROW_FILTERS_ENTRY_NAME = "row_filters.csv";

  String HIVE_METASTORE_CATALOGS_ENTRY_NAME = "catalogs-hms.csv";
  String HIVE_METASTORE_SCHEMATA_ENTRY_NAME = "schemata-hms.csv";
  String HIVE_METASTORE_TABLES_ENTRY_NAME = "tables-hms.csv";
  String HIVE_METASTORE_COLUMNS_ENTRY_NAME = "columns-hms.csv";
  String HIVE_METASTORE_VIEWS_ENTRY_NAME = "views-hms.csv";

  /**
   * Columns of {@code catalogs.csv} and {@code catalogs-hms.csv}. For Unity Catalog, the {@code
   * name} and {@code owner} of the REST {@code catalogs.list} catalogs.
   */
  enum CatalogsHeader {
    CatalogName,
    CatalogOwner
  }

  /**
   * Columns of {@code schemata.csv} and {@code schemata-hms.csv}. For Unity Catalog, the catalog
   * and {@code name} of the REST {@code schemas.list} schemas.
   */
  enum SchemataHeader {
    CatalogName,
    SchemaName
  }

  /**
   * Columns of {@code tables.csv} and {@code tables-hms.csv}. For Unity Catalog, the {@code
   * catalog_name}, {@code schema_name}, {@code name}, {@code table_type}, {@code
   * data_source_format}, {@code storage_location} and {@code properties} (as a JSON object) of the
   * REST {@code tables.list} tables.
   */
  enum TablesHeader {
    TableCatalog,
    TableSchema,
    TableName,
    TableType,
    DataSourceFormat,
    StoragePath,
    TableProperties
  }

  /**
   * Columns of {@code columns.csv} and {@code columns-hms.csv}. For Unity Catalog, the {@code
   * columns} of the REST {@code tables.list} tables: {@code OrdinalPosition} is the 0-based {@code
   * position}, {@code DataType} is the {@code type_name}, such as LONG for a bigint column, {@code
   * NumericPrecision} and {@code NumericScale} are set for DECIMAL columns only, {@code
   * CharacterMaximumLength} for CHAR and VARCHAR columns only, {@code DatetimePrecision} is empty,
   * and {@code ColumnDefault}, {@code IsIdentity}, {@code IdentityGeneration}, {@code
   * IdentityStart}, {@code IdentityIncrement}, {@code IsGenerated} and {@code GenerationExpression}
   * come from the {@code metadata} of {@code TypeJson}. The {@code hive_metastore} columns use the
   * same {@code DataType} names.
   */
  enum ColumnsHeader {
    TableCatalog,
    TableSchema,
    TableName,
    OrdinalPosition,
    ColumnName,
    DataType,
    FullDataType,
    IsNullable,
    ColumnDefault,
    CharacterMaximumLength,
    NumericPrecision,
    NumericScale,
    DatetimePrecision,
    Comment,
    PartitionIndex,
    IsIdentity,
    IdentityGeneration,
    IdentityStart,
    IdentityIncrement,
    IsGenerated,
    GenerationExpression,
    TypeJson
  }

  /**
   * Columns of {@code views.csv} and {@code views-hms.csv}. For Unity Catalog, the REST {@code
   * tables.list} tables that have a {@code view_definition}.
   */
  enum ViewsHeader {
    TableCatalog,
    TableSchema,
    TableName,
    ViewDefinition
  }

  /**
   * Columns of {@code table_constraints.csv}: a row for each PRIMARY KEY, FOREIGN KEY and UNIQUE
   * constraint of the {@code table_constraints} of the REST {@code tables.get} tables. {@code
   * ConstraintColumns} and {@code ReferencedColumns} are JSON arrays of column names, the
   * referenced table and columns are set for foreign keys only, and {@code Rely} is {@code true} or
   * {@code false}.
   */
  enum TableConstraintsHeader {
    ConstraintCatalog,
    ConstraintSchema,
    ConstraintName,
    TableCatalog,
    TableSchema,
    TableName,
    ConstraintType,
    ConstraintColumns,
    ReferencedTableCatalog,
    ReferencedTableSchema,
    ReferencedTableName,
    ReferencedColumns,
    Rely
  }

  /**
   * Columns of {@code column_masks.csv}: the {@code mask} of the columns of the REST {@code
   * tables.list} tables. {@code UsingColumns} are separated by commas.
   */
  enum ColumnMasksHeader {
    TableCatalog,
    TableSchema,
    TableName,
    ColumnName,
    MaskName,
    UsingColumns
  }

  /**
   * Columns of {@code row_filters.csv}: the {@code row_filter} of the REST {@code tables.list}
   * tables. {@code TargetColumns} are separated by a comma and a space.
   */
  enum RowFiltersHeader {
    TableCatalog,
    TableSchema,
    TableName,
    FilterName,
    TargetColumns
  }

  final class DatabricksFilter {

    public static final String HIVE_METASTORE_CATALOG = "hive_metastore";
    private static final String INFORMATION_SCHEMA = "information_schema";

    private static final ImmutableSet<String> UNLISTED_EXCLUDED_CATALOGS =
        ImmutableSet.of("system", "samples", "__databricks_internal");

    private final ImmutableSet<String> includedCatalogs;
    private final ImmutableSet<String> includedSchemas;

    /** The names are trimmed and not empty, as {@link ConnectorArguments} returns them. */
    @VisibleForTesting
    public DatabricksFilter(Collection<String> rawCatalogs, Collection<String> rawSchemas) {
      this.includedCatalogs = normalizeSet(rawCatalogs);
      this.includedSchemas = normalizeSet(rawSchemas);
    }

    public static DatabricksFilter fromArguments(ConnectorArguments arguments) {
      return new DatabricksFilter(arguments.getDatabases(), arguments.getSchemata());
    }

    private static ImmutableSet<String> normalizeSet(Collection<String> values) {
      ImmutableSet.Builder<String> builder = ImmutableSet.builder();
      for (String value : values) {
        builder.add(normalize(value));
      }
      return builder.build();
    }

    private static String normalize(String name) {
      return name.toLowerCase(Locale.ROOT);
    }

    public boolean acceptsCatalog(String catalogName) {
      String normalized = normalize(catalogName);
      if (includedCatalogs.isEmpty()) {
        return !UNLISTED_EXCLUDED_CATALOGS.contains(normalized);
      }
      return includedCatalogs.contains(normalized);
    }

    /** Returns whether the filter accepts a catalog other than {@code hive_metastore}. */
    public boolean acceptsUnityCatalog() {
      return !includedCatalogs.equals(ImmutableSet.of(HIVE_METASTORE_CATALOG));
    }

    public boolean acceptsSchema(String schemaName) {
      String normalized = normalize(schemaName);
      return !INFORMATION_SCHEMA.equals(normalized)
          && (includedSchemas.isEmpty() || includedSchemas.contains(normalized));
    }
  }
}
