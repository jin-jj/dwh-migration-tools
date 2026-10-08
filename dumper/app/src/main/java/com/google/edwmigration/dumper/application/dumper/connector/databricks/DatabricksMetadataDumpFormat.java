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
package com.google.edwmigration.dumper.application.dumper.connector.databricks;

/**
 * Output archive contract for the Databricks metadata connector.
 *
 * <p>The {@code hive_metastore} catalog and the Unity Catalog catalogs are written to separate
 * files with the same columns, such as {@code tables-hms.csv} and {@code tables.csv}, so that each
 * task fails on its own and the files of both sources can be concatenated. A column that a source
 * doesn't provide is empty in the files of that source.
 *
 * <p>Times are ISO 8601 instants, such as {@code 2026-09-27T21:19:25Z}. Columns that hold a map,
 * such as {@code Properties}, are JSON objects with sorted keys, and columns that hold a list are
 * JSON arrays. An absent or empty map or list is an empty string.
 */
interface DatabricksMetadataDumpFormat {

  String FORMAT_NAME = "databricks.dump.zip";

  String HIVE_METASTORE_CATALOGS_ENTRY_NAME = "catalogs-hms.csv";
  String HIVE_METASTORE_SCHEMATA_ENTRY_NAME = "schemata-hms.csv";
  String HIVE_METASTORE_TABLES_ENTRY_NAME = "tables-hms.csv";
  String HIVE_METASTORE_COLUMNS_ENTRY_NAME = "columns-hms.csv";
  String HIVE_METASTORE_VIEWS_ENTRY_NAME = "views-hms.csv";
  String HIVE_METASTORE_TABLE_CONSTRAINTS_ENTRY_NAME = "table_constraints-hms.csv";
  String HIVE_METASTORE_FUNCTIONS_ENTRY_NAME = "functions-hms.csv";
  String HIVE_METASTORE_DDLS_ENTRY_NAME = "ddls-hms.csv";
  String HIVE_METASTORE_FUNCTION_DDLS_ENTRY_NAME = "function_ddls-hms.csv";

  /**
   * Columns of the catalogs files.
   *
   * <ul>
   *   <li>{@code SecurableType} and {@code CatalogType}: the Unity Catalog securable type, such as
   *       {@code CATALOG}, and the catalog type, such as {@code MANAGED_CATALOG}, {@code
   *       FOREIGN_CATALOG} or {@code DELTASHARING_CATALOG}. Empty for {@code hive_metastore}, which
   *       is not a Unity Catalog securable.
   *   <li>{@code StorageRoot}: the {@code MANAGED LOCATION} of the catalog, and {@code
   *       StorageLocation}: where the data of its managed tables is stored.
   *   <li>{@code ConnectionName}: the connection of a foreign catalog. {@code ProviderName} and
   *       {@code ShareName}: the Delta Sharing source of a shared catalog.
   *   <li>{@code Options} and {@code Properties}: JSON objects.
   * </ul>
   */
  enum CatalogsHeader {
    CatalogName,
    SecurableType,
    CatalogType,
    Comment,
    Owner,
    CreatedAt,
    UpdatedAt,
    StorageRoot,
    StorageLocation,
    ConnectionName,
    ProviderName,
    ShareName,
    Options,
    Properties
  }

  /**
   * Columns of the schemata files.
   *
   * <ul>
   *   <li>{@code StorageRoot}: the {@code MANAGED LOCATION} of a Unity Catalog schema.
   *   <li>{@code StorageLocation}: where the data of the managed tables of the schema is stored.
   *       For {@code hive_metastore}, the {@code Location} that {@code DESCRIBE SCHEMA EXTENDED}
   *       reports, such as {@code dbfs:/user/hive/warehouse/sales.db}.
   *   <li>{@code Properties}: a JSON object of the schema properties ({@code DBPROPERTIES}).
   *   <li>{@code CreatedAt} and {@code UpdatedAt}: empty for {@code hive_metastore}, because {@code
   *       DESCRIBE SCHEMA EXTENDED} has no times.
   * </ul>
   */
  enum SchemataHeader {
    CatalogName,
    SchemaName,
    Comment,
    Owner,
    CreatedAt,
    UpdatedAt,
    StorageRoot,
    StorageLocation,
    Properties
  }

  /**
   * Columns of the tables files, with a row for each table and view.
   *
   * <ul>
   *   <li>{@code TableType}: such as {@code MANAGED}, {@code EXTERNAL} or {@code VIEW}.
   *   <li>{@code SecurableType} and {@code SecurableKind}: the Unity Catalog securable type and
   *       kind, such as {@code TABLE} and {@code TABLE_DELTA}. Empty for {@code hive_metastore},
   *       whose tables are not Unity Catalog securables.
   *   <li>{@code DataSourceFormat}: such as {@code DELTA}, {@code PARQUET} or {@code HIVE}.
   *   <li>{@code StorageLocation}: where the data of the table is stored.
   *   <li>{@code UpdatedAt}: empty for {@code hive_metastore}, because {@code SHOW TABLE EXTENDED}
   *       has no update time.
   *   <li>{@code SerdeLibrary}, {@code InputFormat}, {@code OutputFormat} and {@code
   *       StorageProperties}: the Hive storage of a {@code hive_metastore} table. They describe the
   *       storage only for Hive compatible tables; for other tables, such as Delta tables, the
   *       metastore records placeholders such as {@code SequenceFileInputFormat}.
   *   <li>{@code StorageProperties} and {@code TableProperties}: JSON objects. {@code SHOW TABLE
   *       EXTENDED} prints them as {@code [key=value, ...]} without escaping, so for {@code
   *       hive_metastore} an entry ends before the next {@code , key=} outside of parentheses and
   *       brackets, and a value that contains such text is split.
   *   <li>{@code NumBuckets}, {@code BucketColumns} and {@code SortColumns}: the bucketing of a
   *       {@code hive_metastore} table, as in {@code CLUSTERED BY (a) SORTED BY (b) INTO 4
   *       BUCKETS}. The columns are JSON arrays of names.
   * </ul>
   */
  enum TablesHeader {
    TableCatalog,
    TableSchema,
    TableName,
    TableType,
    SecurableType,
    SecurableKind,
    DataSourceFormat,
    StorageLocation,
    Comment,
    Owner,
    CreatedAt,
    UpdatedAt,
    SerdeLibrary,
    InputFormat,
    OutputFormat,
    StorageProperties,
    TableProperties,
    NumBuckets,
    BucketColumns,
    SortColumns
  }

  /**
   * Columns of the columns files.
   *
   * <ul>
   *   <li>{@code OrdinalPosition}: the 0-based position of the column in the table.
   *   <li>{@code DataType}: such as {@code bigint}, {@code varchar(20)} or {@code
   *       array<struct<x:bigint,y:string>>}. For {@code hive_metastore}, {@code SHOW TABLE
   *       EXTENDED} reports {@code CHAR} and {@code VARCHAR} types as {@code string}, so their
   *       lengths come from {@code DESCRIBE TABLE}. A column keeps {@code string} if {@code
   *       DESCRIBE TABLE} fails or truncates its type, as it does for a struct with many fields.
   *   <li>{@code Comment}: for {@code hive_metastore}, from {@code DESCRIBE TABLE}, and empty if it
   *       fails.
   *   <li>{@code PartitionIndex}: the 0-based position of the column in the partitioning, or empty
   *       if the table isn't partitioned by the column.
   *   <li>{@code ColumnDefault}, {@code GenerationExpression}, {@code IdentityGeneration} ({@code
   *       ALWAYS} or {@code BY DEFAULT}), {@code IdentityStart} and {@code IdentityIncrement}: the
   *       {@code DEFAULT}, {@code GENERATED ALWAYS AS} and {@code GENERATED ... AS IDENTITY}
   *       clauses of a Unity Catalog column. {@code SHOW TABLE EXTENDED} and {@code DESCRIBE TABLE}
   *       don't show them, so for {@code hive_metastore} they are empty; the DDL files show them.
   *   <li>{@code Mask}: a JSON object of the column mask of a Unity Catalog column.
   * </ul>
   */
  enum ColumnsHeader {
    TableCatalog,
    TableSchema,
    TableName,
    OrdinalPosition,
    ColumnName,
    DataType,
    IsNullable,
    Comment,
    PartitionIndex,
    ColumnDefault,
    GenerationExpression,
    IdentityGeneration,
    IdentityStart,
    IdentityIncrement,
    Mask
  }

  /**
   * Columns of the views files, which are needed to resolve the names in the view text.
   *
   * <ul>
   *   <li>{@code ViewCatalogAndNamespace}: the current catalog and schema when the view was
   *       created, such as {@code spark_catalog.default}, for the unqualified names in the view
   *       text.
   *   <li>{@code ViewQueryOutputColumns}: a JSON array of the names of the columns of the view
   *       query, which the view columns are mapped to.
   *   <li>{@code ViewSchemaMode}: such as {@code BINDING} or {@code COMPENSATION}.
   *   <li>{@code SqlPath}: the SQL path for the unqualified function names in the view text.
   * </ul>
   */
  enum ViewsHeader {
    TableCatalog,
    TableSchema,
    TableName,
    ViewDefinition,
    ViewCatalogAndNamespace,
    ViewQueryOutputColumns,
    ViewSchemaMode,
    SqlPath
  }

  /**
   * Columns of the table constraints files.
   *
   * <p>{@code ConstraintType} is {@code PRIMARY KEY}, {@code FOREIGN KEY} or {@code CHECK}. {@code
   * ConstraintDetails} is a JSON object: {@code expression} for a check constraint; {@code columns}
   * and {@code rely} for a primary key, with {@code timeseriesColumns}; and {@code columns}, {@code
   * referencedTable}, {@code referencedColumns} and {@code rely} for a foreign key. {@code
   * hive_metastore} has only the check constraints of Delta tables.
   */
  enum TableConstraintsHeader {
    TableCatalog,
    TableSchema,
    TableName,
    ConstraintName,
    ConstraintType,
    ConstraintDetails
  }

  /**
   * Columns of the functions files.
   *
   * <ul>
   *   <li>{@code DataType}: the return type, such as {@code INT}, or {@code TABLE(...)} with the
   *       columns of a table function.
   *   <li>{@code InputParams} and {@code ReturnParams}: JSON arrays of the parameters and of the
   *       columns of a table function. Each element has a {@code name} and a {@code type}, and may
   *       have a {@code mode}, such as {@code IN}, a {@code default} SQL expression and a {@code
   *       comment}. A parameter that could not be split has an empty {@code name} and its whole
   *       text as {@code type}. {@code ReturnParams} is empty for other functions.
   *   <li>{@code RoutineDefinition} and {@code RoutineLanguage}: the body and its language, such as
   *       {@code SQL} or {@code PYTHON}.
   *   <li>{@code IsDeterministic}: {@code true} or {@code false}. {@code SqlDataAccess}: such as
   *       {@code CONTAINS SQL} or {@code READS SQL DATA}.
   *   <li>{@code SecurityType} and {@code SqlPath}: such as {@code DEFINER}, and the SQL path for
   *       the unqualified function names in the body. They and {@code UpdatedAt} are empty for
   *       {@code hive_metastore}.
   *   <li>{@code ClassName}: the class of a Hive function, as in {@code CREATE FUNCTION f AS
   *       'com.acme.Udf'}. {@code Handler}: the handler class of a Python table function.
   * </ul>
   */
  enum FunctionsHeader {
    FunctionCatalog,
    FunctionSchema,
    FunctionName,
    DataType,
    InputParams,
    ReturnParams,
    RoutineDefinition,
    RoutineLanguage,
    Comment,
    Owner,
    IsDeterministic,
    SqlDataAccess,
    SecurityType,
    SqlPath,
    CreatedAt,
    UpdatedAt,
    ClassName,
    Handler
  }

  /**
   * Columns of the DDL files, with the {@code SHOW CREATE TABLE} output of each table and view.
   *
   * <p>For {@code hive_metastore}, the DDL of a Delta table also shows the column defaults,
   * generated columns and identity columns that the columns file leaves empty.
   *
   * <p>Databricks rejects {@code SHOW CREATE TABLE} for some {@code hive_metastore} tables, such as
   * Hive tables with an unsupported SerDe configuration. Those tables are left out; their storage
   * is described in the tables file.
   */
  enum DdlsHeader {
    TableCatalog,
    TableSchema,
    TableName,
    TableType,
    Ddl
  }

  /**
   * Columns of the function DDL files, with the {@code CREATE FUNCTION} statement of each function
   * that has a body or a class.
   *
   * <p>Databricks has no {@code SHOW CREATE FUNCTION}, so the statement is built from the values in
   * the functions file. A function whose parameters could not be split is written with the
   * parameter text as {@code DESCRIBE FUNCTION EXTENDED} printed it.
   */
  enum FunctionDdlsHeader {
    FunctionCatalog,
    FunctionSchema,
    FunctionName,
    Ddl
  }
}
