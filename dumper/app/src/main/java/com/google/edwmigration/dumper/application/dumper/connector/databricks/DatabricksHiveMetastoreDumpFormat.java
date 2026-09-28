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

/** Output archive contract for the Databricks {@code hive_metastore} metadata connector. */
interface DatabricksHiveMetastoreDumpFormat {

  String FORMAT_NAME = "databricks-hive-metastore.dump.zip";

  String CATALOGS_ENTRY_NAME = "catalogs.csv";
  String SCHEMATA_ENTRY_NAME = "schemata.csv";
  String TABLES_ENTRY_NAME = "tables.csv";
  String COLUMNS_ENTRY_NAME = "columns.csv";
  String VIEWS_ENTRY_NAME = "views.csv";
  String TABLE_CONSTRAINTS_ENTRY_NAME = "table_constraints.csv";
  String FUNCTIONS_ENTRY_NAME = "functions.csv";
  String DDLS_ENTRY_NAME = "ddls.csv";
  String FUNCTION_DDLS_ENTRY_NAME = "function_ddls.csv";

  enum CatalogsHeader {
    CatalogName,
    Comment,
    Owner,
    CreatedAt,
    UpdatedAt
  }

  /**
   * Columns of {@code schemata.csv}.
   *
   * <p>{@code StorageLocation} is the {@code Location} that {@code DESCRIBE SCHEMA EXTENDED}
   * reports, such as {@code dbfs:/user/hive/warehouse/sales.db}. The data of a managed table in the
   * schema is stored in a directory with the table name under it. {@code CreatedAt} and {@code
   * UpdatedAt} are empty, because {@code DESCRIBE SCHEMA EXTENDED} has no times.
   */
  enum SchemataHeader {
    CatalogName,
    SchemaName,
    Comment,
    Owner,
    CreatedAt,
    UpdatedAt,
    StorageLocation
  }

  /**
   * Columns of {@code tables.csv}.
   *
   * <p>{@code CreatedAt} is an ISO 8601 instant, such as {@code 2026-09-27T21:19:25Z}. {@code
   * UpdatedAt} is empty, because {@code SHOW TABLE EXTENDED} has no update time.
   *
   * <p>The last five columns are copied verbatim from {@code SHOW TABLE EXTENDED}, with properties
   * in the {@code [key=value, ...]} form. The SerDe and formats describe the storage only for Hive
   * compatible tables; for other tables, such as Delta tables, the metastore records placeholders
   * such as {@code SequenceFileInputFormat}.
   */
  enum TablesHeader {
    TableCatalog,
    TableSchema,
    TableName,
    TableType,
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
    TableProperties
  }

  /**
   * Columns of {@code columns.csv}.
   *
   * <p>{@code DataType} is written in Hive type syntax, such as {@code bigint} or {@code
   * array<struct<x:bigint,y:string>>}. {@code SHOW TABLE EXTENDED} reports {@code CHAR} and {@code
   * VARCHAR} columns as {@code string} and has no column comments, so the lengths and the comments
   * are only in {@code ddls.csv}.
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
    PartitionIndex
  }

  enum ViewsHeader {
    TableCatalog,
    TableSchema,
    TableName,
    ViewDefinition
  }

  enum TableConstraintsHeader {
    TableCatalog,
    TableSchema,
    TableName,
    ConstraintName,
    ConstraintType,
    ConstraintDetails
  }

  enum FunctionsHeader {
    FunctionCatalog,
    FunctionSchema,
    FunctionName,
    DataType,
    InputParams,
    RoutineDefinition,
    RoutineLanguage,
    Comment,
    Owner
  }

  enum DdlsHeader {
    TableCatalog,
    TableSchema,
    TableName,
    TableType,
    Ddl
  }

  enum FunctionDdlsHeader {
    FunctionCatalog,
    FunctionSchema,
    FunctionName,
    Ddl
  }
}
