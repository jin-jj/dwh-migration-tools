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
interface DatabricksMetadataDumpFormat {

  String FORMAT_NAME = "databricks.dump.zip";

  String CATALOGS_ENTRY_NAME = "catalogs.csv";
  String SCHEMATA_ENTRY_NAME = "schemata.csv";
  String TABLES_ENTRY_NAME = "tables.csv";
  String COLUMNS_ENTRY_NAME = "columns.csv";
  String VIEWS_ENTRY_NAME = "views.csv";
  String TABLE_CONSTRAINTS_ENTRY_NAME = "table_constraints.csv";
  String FUNCTIONS_ENTRY_NAME = "functions.csv";
  String DDLS_ENTRY_NAME = "ddls.csv";

  enum CatalogsHeader {
    CatalogName,
    Comment,
    Owner,
    CreatedAt,
    UpdatedAt
  }

  enum SchemataHeader {
    CatalogName,
    SchemaName,
    Comment,
    Owner,
    CreatedAt,
    UpdatedAt
  }

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
    UpdatedAt
  }

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
}
