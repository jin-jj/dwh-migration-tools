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
package com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata;

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper.SqlQuoting.quoteIdentifierIfNeeded;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper.SqlQuoting.quoteQualifiedName;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper.SqlQuoting.quoteStringLiteral;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.DatabricksFilter.HIVE_METASTORE_CATALOG;
import static java.util.Comparator.comparing;
import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.nullsLast;

import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.service.catalog.CatalogInfo;
import com.databricks.sdk.service.catalog.ColumnInfo;
import com.databricks.sdk.service.catalog.ColumnMask;
import com.databricks.sdk.service.catalog.FunctionInfo;
import com.databricks.sdk.service.catalog.FunctionParameterInfo;
import com.databricks.sdk.service.catalog.FunctionParameterInfos;
import com.databricks.sdk.service.catalog.SchemaInfo;
import com.databricks.sdk.service.catalog.TableInfo;
import com.databricks.sdk.service.catalog.TableRowFilter;
import com.databricks.sdk.service.catalog.TableType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper.SqlQuoting;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.DatabricksHandle.MetadataTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.CatalogsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.ColumnMasksHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.ColumnsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.DatabricksFilter;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.FunctionsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.RowFiltersHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.SchemataHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.TableConstraintsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.TablesHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.ViewsHeader;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory, Unity Catalog REST API readers, and CSV row writers for the 9 Unity Catalog metadata
 * extraction tasks in their archive order. The schemas are listed once per run; every task lists
 * the tables or functions of each schema again, so the rows are written schema by schema.
 */
public final class UnityCatalogTasks {

  private static final Pattern DECIMAL_TYPE_PATTERN =
      Pattern.compile("decimal\\((\\d+),\\s*(\\d+)\\)", Pattern.CASE_INSENSITIVE);
  private static final Pattern CHAR_TYPE_PATTERN =
      Pattern.compile("(?:var)?char\\((\\d+)\\)", Pattern.CASE_INSENSITIVE);
  private static final Logger logger = LoggerFactory.getLogger(UnityCatalogTasks.class);

  private static final ObjectReader JSON_READER = new ObjectMapper().readerFor(JsonNode.class);
  private static final ObjectWriter JSON_WRITER =
      new ObjectMapper().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true).writer();
  private static final Splitter PARENT_TABLE_SPLITTER = Splitter.on('.').limit(3);
  private static final Comparator<FunctionParameterInfo> PARAMETER_POSITION_ORDER =
      comparing(FunctionParameterInfo::getPosition, nullsLast(naturalOrder()));

  /** Consumes one table of a {@code tables.list} response. */
  @FunctionalInterface
  private interface TableConsumer {
    void accept(TableInfo table) throws IOException;
  }

  /** The accepted (catalog, schema) pairs, listed by the first task that needs them. */
  private ImmutableList<Map.Entry<String, String>> schemas;

  UnityCatalogTasks() {}

  public static ImmutableList<Task<?>> create() {
    UnityCatalogTasks tasks = new UnityCatalogTasks();
    return ImmutableList.of(
        new MetadataTask<>(
            DatabricksMetadataDumpFormat.UNITY_CATALOG_CATALOGS_ENTRY_NAME,
            CatalogsHeader.class,
            tasks::writeCatalogs),
        new MetadataTask<>(
            DatabricksMetadataDumpFormat.UNITY_CATALOG_SCHEMATA_ENTRY_NAME,
            SchemataHeader.class,
            tasks::writeSchemata),
        new MetadataTask<>(
            DatabricksMetadataDumpFormat.UNITY_CATALOG_TABLES_ENTRY_NAME,
            TablesHeader.class,
            tasks::writeTables),
        new MetadataTask<>(
            DatabricksMetadataDumpFormat.UNITY_CATALOG_COLUMNS_ENTRY_NAME,
            ColumnsHeader.class,
            tasks::writeColumns),
        new MetadataTask<>(
            DatabricksMetadataDumpFormat.UNITY_CATALOG_VIEWS_ENTRY_NAME,
            ViewsHeader.class,
            tasks::writeViews),
        new MetadataTask<>(
            DatabricksMetadataDumpFormat.UNITY_CATALOG_TABLE_CONSTRAINTS_ENTRY_NAME,
            TableConstraintsHeader.class,
            tasks::writeTableConstraints),
        new MetadataTask<>(
            DatabricksMetadataDumpFormat.UNITY_CATALOG_FUNCTIONS_ENTRY_NAME,
            FunctionsHeader.class,
            tasks::writeFunctions),
        new MetadataTask<>(
            DatabricksMetadataDumpFormat.UNITY_CATALOG_COLUMN_MASKS_ENTRY_NAME,
            ColumnMasksHeader.class,
            tasks::writeColumnMasks),
        new MetadataTask<>(
            DatabricksMetadataDumpFormat.UNITY_CATALOG_ROW_FILTERS_ENTRY_NAME,
            RowFiltersHeader.class,
            tasks::writeRowFilters));
  }

  void writeCatalogs(DatabricksHandle handle, CSVPrinter printer) throws IOException {
    for (CatalogInfo catalog : handle.restHelper().listCatalogs()) {
      if (acceptsCatalog(handle.filter(), catalog.getName())) {
        printer.printRecord(catalog.getName(), catalog.getOwner());
      }
    }
  }

  void writeSchemata(DatabricksHandle handle, CSVPrinter printer) throws IOException {
    for (Map.Entry<String, String> schema : listSchemas(handle)) {
      printer.printRecord(schema.getKey(), schema.getValue());
    }
  }

  void writeTables(DatabricksHandle handle, CSVPrinter printer) throws IOException {
    forEachTable(
        handle,
        table ->
            printer.printRecord(
                table.getCatalogName(),
                table.getSchemaName(),
                table.getName(),
                table.getTableType(),
                table.getDataSourceFormat(),
                table.getStorageLocation(),
                table.getProperties() == null || table.getProperties().isEmpty()
                    ? ""
                    : JSON_WRITER.writeValueAsString(table.getProperties())));
  }

  void writeColumns(DatabricksHandle handle, CSVPrinter printer) throws IOException {
    forEachTable(
        handle,
        table -> {
          if (table.getColumns() == null) {
            return;
          }

          for (ColumnInfo column : table.getColumns()) {
            printer.printRecord(columnRecord(table, column));
          }
        });
  }

  void writeViews(DatabricksHandle handle, CSVPrinter printer) throws IOException {
    forEachTable(
        handle,
        table -> {
          if (table.getViewDefinition() != null) {
            printer.printRecord(
                table.getCatalogName(),
                table.getSchemaName(),
                table.getName(),
                table.getViewDefinition());
          }
        });
  }

  /**
   * Writes the key constraints of the tables that aren't views, such as VIEW or MATERIALIZED_VIEW.
   * {@code tables.list} doesn't return constraints, so the names of the tables are listed first,
   * then {@code tables.get} is called for each table.
   */
  void writeTableConstraints(DatabricksHandle handle, CSVPrinter printer) throws IOException {
    ImmutableList.Builder<TableName> tables = ImmutableList.builder();
    forEachTable(
        handle,
        table -> {
          TableType tableType = table.getTableType();
          if (tableType == null || !tableType.name().contains("VIEW")) {
            tables.add(
                new TableName(table.getCatalogName(), table.getSchemaName(), table.getName()));
          }
        });
    handle.runForEach(
        "tables.get",
        tables.build(),
        table -> quoteQualifiedName(table.catalog, table.schema, table.name),
        table -> handle.restHelper().getTableJson(table.catalog, table.schema, table.name),
        (table, tableJson) -> printConstraints(printer, table, tableJson));
  }

  /**
   * Writes the {@code functions.list} functions of each accepted schema. {@code functions.list}
   * doesn't return the {@code input_params} of the functions, so {@code functions.get} is called
   * for each function to write its {@code ArgumentSignature}; the row is written with an empty
   * {@code ArgumentSignature} when {@code functions.get} fails. Neither call returns a {@code
   * sql_path}, so {@code SqlPath} is empty.
   */
  void writeFunctions(DatabricksHandle handle, CSVPrinter printer) throws IOException {
    handle.runForEach(
        "functions.list",
        listSchemas(handle),
        schema -> quoteQualifiedName(schema.getKey(), schema.getValue()),
        schema -> handle.restHelper().listFunctions(schema.getKey(), schema.getValue()),
        (schema, functions) -> {
          for (FunctionInfo function : functions) {
            String argumentSignature =
                fetchArgumentSignature(
                    handle, schema.getKey(), schema.getValue(), function.getName());
            printer.printRecord(functionRecord(function, argumentSignature));
          }
        });
  }

  void writeColumnMasks(DatabricksHandle handle, CSVPrinter printer) throws IOException {
    forEachTable(
        handle,
        table -> {
          if (table.getColumns() == null) {
            return;
          }

          for (ColumnInfo column : table.getColumns()) {
            ColumnMask mask = column.getMask();
            if (mask != null) {
              printer.printRecord(
                  table.getCatalogName(),
                  table.getSchemaName(),
                  table.getName(),
                  column.getName(),
                  mask.getFunctionName(),
                  joinOrEmpty(",", mask.getUsingColumnNames()));
            }
          }
        });
  }

  void writeRowFilters(DatabricksHandle handle, CSVPrinter printer) throws IOException {
    forEachTable(
        handle,
        table -> {
          TableRowFilter rowFilter = table.getRowFilter();
          if (rowFilter != null) {
            printer.printRecord(
                table.getCatalogName(),
                table.getSchemaName(),
                table.getName(),
                rowFilter.getFunctionName(),
                joinOrEmpty(", ", rowFilter.getInputColumnNames()));
          }
        });
  }

  /**
   * Returns the (catalog, schema) pairs accepted by the filter: the schemas of {@code schemas.list}
   * for each catalog of {@code catalogs.list}.
   */
  private synchronized ImmutableList<Map.Entry<String, String>> listSchemas(DatabricksHandle handle)
      throws IOException {
    if (this.schemas == null) {
      ImmutableList.Builder<String> catalogs = ImmutableList.builder();
      for (CatalogInfo catalog : handle.restHelper().listCatalogs()) {
        if (acceptsCatalog(handle.filter(), catalog.getName())) {
          catalogs.add(catalog.getName());
        }
      }

      ImmutableList.Builder<Map.Entry<String, String>> builder = ImmutableList.builder();
      handle.runForEach(
          "schemas.list",
          catalogs.build(),
          SqlQuoting::quoteIdentifier,
          handle.restHelper()::listSchemas,
          (catalog, catalogSchemas) -> {
            for (SchemaInfo schema : catalogSchemas) {
              if (handle.filter().acceptsSchema(schema.getName())) {
                builder.add(Maps.immutableEntry(catalog, schema.getName()));
              }
            }
          });
      this.schemas = builder.build();
    }
    return this.schemas;
  }

  /** Passes the {@code tables.list} tables of each accepted schema to {@code consumer}. */
  private void forEachTable(DatabricksHandle handle, TableConsumer consumer) throws IOException {
    handle.runForEach(
        "tables.list",
        listSchemas(handle),
        schema -> quoteQualifiedName(schema.getKey(), schema.getValue()),
        schema -> handle.restHelper().listTables(schema.getKey(), schema.getValue()),
        (schema, tables) -> {
          for (TableInfo table : tables) {
            consumer.accept(table);
          }
        });
  }

  /** Returns whether {@code catalogName} is a Unity Catalog catalog accepted by the filter. */
  private static boolean acceptsCatalog(DatabricksFilter filter, String catalogName) {
    return !HIVE_METASTORE_CATALOG.equalsIgnoreCase(catalogName)
        && filter.acceptsCatalog(catalogName);
  }

  /** Returns the record of {@code columns.csv} for a column of a {@code tables.list} table. */
  private static Object[] columnRecord(TableInfo table, ColumnInfo column) {
    Object[] record = new Object[ColumnsHeader.values().length];
    record[ColumnsHeader.TableCatalog.ordinal()] = table.getCatalogName();
    record[ColumnsHeader.TableSchema.ordinal()] = table.getSchemaName();
    record[ColumnsHeader.TableName.ordinal()] = table.getName();
    record[ColumnsHeader.OrdinalPosition.ordinal()] = column.getPosition();
    record[ColumnsHeader.ColumnName.ordinal()] = column.getName();
    record[ColumnsHeader.DataType.ordinal()] = column.getTypeName();
    record[ColumnsHeader.FullDataType.ordinal()] = column.getTypeText();
    record[ColumnsHeader.IsNullable.ordinal()] =
        Boolean.FALSE.equals(column.getNullable()) ? "NO" : "YES";
    // The REST API sends type_precision and type_scale as 0, even for DECIMAL columns, and has no
    // character length, so they are read from type_text, such as decimal(18,4) or varchar(16). The
    // REST API has no datetime precision.
    Matcher decimal = DECIMAL_TYPE_PATTERN.matcher(Strings.nullToEmpty(column.getTypeText()));
    if (decimal.matches()) {
      record[ColumnsHeader.NumericPrecision.ordinal()] = decimal.group(1);
      record[ColumnsHeader.NumericScale.ordinal()] = decimal.group(2);
    }

    Matcher characters = CHAR_TYPE_PATTERN.matcher(Strings.nullToEmpty(column.getTypeText()));
    if (characters.matches()) {
      record[ColumnsHeader.CharacterMaximumLength.ordinal()] = characters.group(1);
    }

    record[ColumnsHeader.Comment.ordinal()] = column.getComment();
    record[ColumnsHeader.PartitionIndex.ordinal()] = column.getPartitionIndex();
    record[ColumnsHeader.TypeJson.ordinal()] = column.getTypeJson();
    // The default, generated and identity values come from type_json.metadata. They stay empty when
    // the column has no type_json.
    if (!Strings.isNullOrEmpty(column.getTypeJson())) {
      JsonNode metadata = parseTypeJsonMetadata(column.getTypeJson());
      String generationExpression = metadata.path("delta.generationExpression").asText("");
      String identityStart = metadata.path("delta.identity.start").asText("");
      record[ColumnsHeader.ColumnDefault.ordinal()] = metadata.path("CURRENT_DEFAULT").asText("");
      record[ColumnsHeader.IsGenerated.ordinal()] =
          generationExpression.isEmpty() ? "NEVER" : "ALWAYS";
      record[ColumnsHeader.GenerationExpression.ordinal()] = generationExpression;
      record[ColumnsHeader.IsIdentity.ordinal()] = identityStart.isEmpty() ? "NO" : "YES";
      if (!identityStart.isEmpty()) {
        record[ColumnsHeader.IdentityGeneration.ordinal()] =
            metadata.path("delta.identity.allowExplicitInsert").asBoolean(false)
                ? "BY DEFAULT"
                : "ALWAYS";
        record[ColumnsHeader.IdentityStart.ordinal()] = identityStart;
        record[ColumnsHeader.IdentityIncrement.ordinal()] =
            metadata.path("delta.identity.step").asText("");
      }
    }
    return record;
  }

  /**
   * Prints a row for each primary key, foreign key and UNIQUE constraint of the {@code
   * table_constraints} of a {@code tables.get} response, and skips the other constraints.
   */
  private static void printConstraints(CSVPrinter printer, TableName table, JsonNode tableJson)
      throws IOException {
    for (JsonNode constraint : tableJson.path("table_constraints")) {
      String constraintType;
      JsonNode key;
      if (constraint.has("primary_key_constraint")) {
        constraintType = "PRIMARY KEY";
        key = constraint.path("primary_key_constraint");
      } else if (constraint.has("foreign_key_constraint")) {
        constraintType = "FOREIGN KEY";
        key = constraint.path("foreign_key_constraint");
      } else if (constraint.has("unique_constraint")) {
        constraintType = "UNIQUE";
        key = constraint.path("unique_constraint");
      } else {
        continue;
      }

      // A foreign key has the full name of its parent table, such as main.sales.orders.
      String referencedCatalog = "";
      String referencedSchema = "";
      String referencedTable = key.path("parent_table").asText("");
      List<String> parentTableParts = PARENT_TABLE_SPLITTER.splitToList(referencedTable);
      if (parentTableParts.size() == 3) {
        referencedCatalog = parentTableParts.get(0);
        referencedSchema = parentTableParts.get(1);
        referencedTable = parentTableParts.get(2);
      }

      Object[] record = new Object[TableConstraintsHeader.values().length];
      record[TableConstraintsHeader.ConstraintCatalog.ordinal()] = table.catalog;
      record[TableConstraintsHeader.ConstraintSchema.ordinal()] = table.schema;
      record[TableConstraintsHeader.ConstraintName.ordinal()] = key.path("name").asText("");
      record[TableConstraintsHeader.TableCatalog.ordinal()] = table.catalog;
      record[TableConstraintsHeader.TableSchema.ordinal()] = table.schema;
      record[TableConstraintsHeader.TableName.ordinal()] = table.name;
      record[TableConstraintsHeader.ConstraintType.ordinal()] = constraintType;
      record[TableConstraintsHeader.ConstraintColumns.ordinal()] =
          jsonArrayOrEmpty(key.path("child_columns"));
      record[TableConstraintsHeader.ReferencedTableCatalog.ordinal()] = referencedCatalog;
      record[TableConstraintsHeader.ReferencedTableSchema.ordinal()] = referencedSchema;
      record[TableConstraintsHeader.ReferencedTableName.ordinal()] = referencedTable;
      record[TableConstraintsHeader.ReferencedColumns.ordinal()] =
          jsonArrayOrEmpty(key.path("parent_columns"));
      record[TableConstraintsHeader.Rely.ordinal()] = key.path("rely").asBoolean(false);
      printer.printRecord(record);
    }
  }

  /**
   * Returns the record of {@code functions.csv} for a {@code functions.list} function and its
   * {@code ArgumentSignature}.
   */
  private static Object[] functionRecord(FunctionInfo function, String argumentSignature) {
    Object[] record = new Object[FunctionsHeader.values().length];
    record[FunctionsHeader.RoutineCatalog.ordinal()] = function.getCatalogName();
    record[FunctionsHeader.RoutineSchema.ordinal()] = function.getSchemaName();
    record[FunctionsHeader.RoutineName.ordinal()] = function.getName();
    record[FunctionsHeader.DataType.ordinal()] = function.getDataType();
    record[FunctionsHeader.FullDataType.ordinal()] = function.getFullDataType();
    record[FunctionsHeader.RoutineBody.ordinal()] = function.getRoutineBody();
    record[FunctionsHeader.RoutineDefinition.ordinal()] = function.getRoutineDefinition();
    record[FunctionsHeader.ExternalLanguage.ordinal()] = function.getExternalLanguage();
    record[FunctionsHeader.ExternalName.ordinal()] = function.getExternalName();
    // Written as true or false, like the Deterministic value of the hive_metastore functions.
    record[FunctionsHeader.IsDeterministic.ordinal()] = function.getIsDeterministic();
    record[FunctionsHeader.SqlDataAccess.ordinal()] = function.getSqlDataAccess();
    // Empty, as the REST API doesn't return sql_path.
    record[FunctionsHeader.SqlPath.ordinal()] = function.getSqlPath();
    record[FunctionsHeader.Comment.ordinal()] = function.getComment();
    record[FunctionsHeader.ArgumentSignature.ordinal()] = argumentSignature;
    // The class name is written for the hive_metastore functions only.
    record[FunctionsHeader.ClassName.ordinal()] = "";
    return record;
  }

  /**
   * Returns the {@code ArgumentSignature} of a function: the parameters of the {@code input_params}
   * of its {@code functions.get} response in {@code position} order as a list in parentheses, such
   * as {@code (x INT, y INT DEFAULT 0 COMMENT 'the offset')}, or empty if {@code functions.get}
   * fails.
   */
  private static String fetchArgumentSignature(
      DatabricksHandle handle, String catalogName, String schemaName, String functionName) {
    FunctionInfo function;
    try {
      function = handle.restHelper().getFunction(catalogName, schemaName, functionName);
    } catch (DatabricksException | IllegalArgumentException e) {
      logger.warn(
          "Writing function {} with an empty ArgumentSignature because functions.get failed: {}",
          quoteQualifiedName(catalogName, schemaName, functionName),
          e.getMessage());
      return "";
    }

    FunctionParameterInfos inputParams = function.getInputParams();
    ImmutableList.Builder<String> parameters = ImmutableList.builder();
    if (inputParams != null && inputParams.getParameters() != null) {
      for (FunctionParameterInfo parameter :
          ImmutableList.sortedCopyOf(PARAMETER_POSITION_ORDER, inputParams.getParameters())) {
        StringBuilder definition =
            new StringBuilder(quoteIdentifierIfNeeded(Strings.nullToEmpty(parameter.getName())))
                .append(' ')
                .append(Strings.nullToEmpty(parameter.getTypeText()));
        if (parameter.getParameterDefault() != null) {
          definition.append(" DEFAULT ").append(parameter.getParameterDefault());
        }

        if (parameter.getComment() != null) {
          definition.append(" COMMENT ").append(quoteStringLiteral(parameter.getComment()));
        }
        parameters.add(definition.toString());
      }
    }
    return "(" + String.join(", ", parameters.build()) + ")";
  }

  /** Returns {@code type_json.metadata}, or a missing node when {@code typeJson} isn't JSON. */
  private static JsonNode parseTypeJsonMetadata(String typeJson) {
    try {
      return JSON_READER.readTree(typeJson).path("metadata");
    } catch (IOException e) {
      logger.debug("Failed to parse ColumnInfo.type_json: {}", e.getMessage());
      return MissingNode.getInstance();
    }
  }

  /** Returns {@code node} as JSON if it's an array, such as {@code ["id"]}, or empty otherwise. */
  private static String jsonArrayOrEmpty(JsonNode node) throws IOException {
    return node.isArray() ? JSON_WRITER.writeValueAsString(node) : "";
  }

  private static String joinOrEmpty(String separator, Collection<String> values) {
    return values == null ? "" : String.join(separator, values);
  }

  /** The names of a table listed by {@code tables.list}. */
  private static final class TableName {
    final String catalog;
    final String schema;
    final String name;

    TableName(String catalog, String schema, String name) {
      this.catalog = catalog;
      this.schema = schema;
      this.name = name;
    }
  }
}
