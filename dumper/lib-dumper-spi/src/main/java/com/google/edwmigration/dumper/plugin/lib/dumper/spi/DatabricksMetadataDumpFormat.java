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
package com.google.edwmigration.dumper.plugin.lib.dumper.spi;

/** Format specification for Databricks metadata dumps. */
public interface DatabricksMetadataDumpFormat {

  String FORMAT_NAME = "databricks.dump.zip";

  interface CatalogsFormat {

    String ZIP_ENTRY_NAME = "catalogs.csv";

    enum Header {
      CatalogName,
      Comment,
      Owner,
      CreatedAt,
      UpdatedAt
    }
  }

  interface SchemataFormat {

    String ZIP_ENTRY_NAME = "schemata.csv";
    String HMS_ZIP_ENTRY_NAME = "schemata-hms.csv";

    enum Header {
      CatalogName,
      SchemaName,
      Comment,
      Owner,
      CreatedAt,
      UpdatedAt
    }
  }

  interface TablesFormat {

    String ZIP_ENTRY_NAME = "tables.csv";
    String HMS_ZIP_ENTRY_NAME = "tables-hms.csv";

    enum Header {
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
  }

  interface ColumnsFormat {

    String ZIP_ENTRY_NAME = "columns.csv";
    String HMS_ZIP_ENTRY_NAME = "columns-hms.csv";

    enum Header {
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
  }

  interface ViewsFormat {

    String ZIP_ENTRY_NAME = "views.csv";
    String HMS_ZIP_ENTRY_NAME = "views-hms.csv";

    enum Header {
      TableCatalog,
      TableSchema,
      TableName,
      ViewDefinition
    }
  }

  interface TableConstraintsFormat {

    String ZIP_ENTRY_NAME = "table_constraints.csv";

    enum Header {
      TableCatalog,
      TableSchema,
      TableName,
      ConstraintName,
      ConstraintType,
      ConstraintDetails
    }
  }

  interface FunctionsFormat {

    String ZIP_ENTRY_NAME = "functions.csv";

    enum Header {
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
  }
}
