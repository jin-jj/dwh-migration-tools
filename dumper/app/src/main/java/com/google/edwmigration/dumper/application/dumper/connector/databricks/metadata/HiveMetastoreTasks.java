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

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper.SqlQuoting.quoteIdentifier;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper.SqlQuoting.quoteIdentifierIfNeeded;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper.SqlQuoting.quoteQualifiedName;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper.SqlQuoting.quoteStringLiteral;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper.StatementResult.cell;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.DatabricksFilter.HIVE_METASTORE_CATALOG;

import com.databricks.sdk.core.DatabricksException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper.SqlQuoting;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper.StatementResult;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.DatabricksHandle.MetadataTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.CatalogsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.ColumnsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.SchemataHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.TablesHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.ViewsHeader;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory, SQL statement runner, and row writers for the 5 {@code hive_metastore} metadata
 * extraction tasks in their archive order. The tables are read with {@code DESCRIBE TABLE EXTENDED
 * ... AS JSON}, and the partition columns of Delta tables with {@code DESCRIBE DETAIL}.
 */
public final class HiveMetastoreTasks {

  private static final Logger logger = LoggerFactory.getLogger(HiveMetastoreTasks.class);
  private static final String PARTITION_COLUMNS_DETAIL = "partitionColumns";
  private static final String DEFAULT_COLLATION = "UTF8_BINARY";
  private static final ObjectReader JSON_READER = new ObjectMapper().readerFor(JsonNode.class);

  /**
   * The {@code DataType} of the {@code DESCRIBE} type names that Unity Catalog names differently,
   * so that the columns files of both catalogs use the same type names.
   */
  private static final ImmutableMap<String, String> UNITY_CATALOG_TYPE_NAMES =
      ImmutableMap.of(
          "bigint", "LONG",
          "smallint", "SHORT",
          "tinyint", "BYTE",
          "timestamp_ltz", "TIMESTAMP",
          "varchar", "STRING");

  private ImmutableList<String> schemas;
  private ImmutableList<TableRef> tables;

  HiveMetastoreTasks() {}

  public static ImmutableList<Task<?>> create() {
    HiveMetastoreTasks tasks = new HiveMetastoreTasks();
    return ImmutableList.of(
        new MetadataTask<>(
            DatabricksMetadataDumpFormat.HIVE_METASTORE_CATALOGS_ENTRY_NAME,
            CatalogsHeader.class,
            tasks::writeCatalogs),
        new MetadataTask<>(
            DatabricksMetadataDumpFormat.HIVE_METASTORE_SCHEMATA_ENTRY_NAME,
            SchemataHeader.class,
            tasks::writeSchemata),
        new MetadataTask<>(
            DatabricksMetadataDumpFormat.HIVE_METASTORE_TABLES_ENTRY_NAME,
            TablesHeader.class,
            tasks::writeTables),
        new MetadataTask<>(
            DatabricksMetadataDumpFormat.HIVE_METASTORE_COLUMNS_ENTRY_NAME,
            ColumnsHeader.class,
            tasks::writeColumns),
        new MetadataTask<>(
            DatabricksMetadataDumpFormat.HIVE_METASTORE_VIEWS_ENTRY_NAME,
            ViewsHeader.class,
            tasks::writeViews));
  }

  synchronized ImmutableList<String> listSchemas(DatabricksHandle handle) throws IOException {
    if (this.schemas == null) {
      ImmutableList.Builder<String> builder = ImmutableList.builder();
      StatementResult rows =
          handle.sqlHelper().executeSqlInCatalog(HIVE_METASTORE_CATALOG, "SHOW SCHEMAS");
      for (List<String> row = rows.nextRow(); row != null; row = rows.nextRow()) {
        if (handle.filter().acceptsSchema(row.get(0))) {
          builder.add(row.get(0));
        }
      }
      this.schemas = builder.build();
    }
    return this.schemas;
  }

  /**
   * Lists the tables of each schema, and sets the rows of each table from its DESCRIBE TABLE
   * output. A table whose DESCRIBE TABLE fails keeps its name-only row.
   */
  synchronized ImmutableList<TableRef> listTables(DatabricksHandle handle) throws IOException {
    if (this.tables == null) {
      ImmutableList.Builder<TableRef> tablesBuilder = ImmutableList.builder();
      handle.runForEach(
          "SHOW TABLES",
          listSchemas(handle),
          SqlQuoting::quoteIdentifier,
          schema -> showTables(handle, schema),
          (schema, schemaTables) -> tablesBuilder.addAll(schemaTables));
      ImmutableList<TableRef> listedTables = tablesBuilder.build();
      handle.runForEach(
          "DESCRIBE TABLE EXTENDED AS JSON",
          listedTables,
          TableRef::quotedName,
          table -> describeTable(handle, table),
          (table, description) -> setRows(handle, table, description));
      this.tables = listedTables;
    }
    return this.tables;
  }

  void writeCatalogs(DatabricksHandle handle, CSVPrinter printer) throws IOException {
    printer.printRecord(HIVE_METASTORE_CATALOG, null);
  }

  void writeSchemata(DatabricksHandle handle, CSVPrinter printer) throws IOException {
    for (String schema : listSchemas(handle)) {
      printer.printRecord(HIVE_METASTORE_CATALOG, schema);
    }
  }

  void writeTables(DatabricksHandle handle, CSVPrinter printer) throws IOException {
    for (TableRef table : listTables(handle)) {
      printer.printRecord(table.tableRow);
    }
  }

  void writeColumns(DatabricksHandle handle, CSVPrinter printer) throws IOException {
    for (TableRef table : listTables(handle)) {
      for (Object[] columnRow : table.columnRows) {
        printer.printRecord(columnRow);
      }
    }
  }

  void writeViews(DatabricksHandle handle, CSVPrinter printer) throws IOException {
    for (TableRef table : listTables(handle)) {
      if (table.viewRow != null) {
        printer.printRecord(table.viewRow);
      }
    }
  }

  static ImmutableList<TableRef> showTables(DatabricksHandle handle, String schema)
      throws IOException {
    String query = "SHOW TABLES IN " + quoteIdentifier(schema);
    ImmutableList.Builder<TableRef> builder = ImmutableList.builder();
    StatementResult rows = handle.sqlHelper().executeSqlInCatalog(HIVE_METASTORE_CATALOG, query);
    // The cells of a row are the schema, the table name and isTemporary.
    for (List<String> row = rows.nextRow(); row != null; row = rows.nextRow()) {
      if (!"true".equalsIgnoreCase(row.get(2))) {
        builder.add(new TableRef(schema, row.get(1)));
      }
    }
    return builder.build();
  }

  private static JsonNode describeTable(DatabricksHandle handle, TableRef table)
      throws IOException {
    String query = "DESCRIBE TABLE EXTENDED " + table.quotedName() + " AS JSON";
    StatementResult rows = handle.sqlHelper().executeSqlInCatalog(HIVE_METASTORE_CATALOG, query);
    String json = cell(rows.nextRow(), 0);
    JsonNode description = json == null ? null : JSON_READER.readTree(json);
    if (description == null || !description.isObject()) {
      throw new IOException(
          "DESCRIBE TABLE AS JSON returned no JSON object for " + table.quotedName() + ".");
    }
    return description;
  }

  /**
   * Sets the rows of a table from its {@code DESCRIBE TABLE} output. The JSON has no partition
   * columns for Delta tables, so they are read with {@code DESCRIBE DETAIL}.
   */
  private static void setRows(DatabricksHandle handle, TableRef table, JsonNode description) {
    JsonNode partitionColumns = description.path("partition_columns");
    if ("delta".equalsIgnoreCase(description.path("provider").asText(""))) {
      String detailQuery = "DESCRIBE DETAIL " + table.quotedName();
      try {
        StatementResult detail =
            handle.sqlHelper().executeSqlInCatalog(HIVE_METASTORE_CATALOG, detailQuery);
        String detailCell =
            cell(detail.nextRow(), detail.columnNames().indexOf(PARTITION_COLUMNS_DETAIL));
        if (detailCell == null) {
          throw new IOException("DESCRIBE DETAIL returned no partitionColumns.");
        }
        partitionColumns = JSON_READER.readTree(detailCell);
      } catch (IOException | DatabricksException e) {
        logger.warn(
            "Writing the columns of {} without partition indexes because DESCRIBE DETAIL failed: {}",
            table.quotedName(),
            e.getMessage());
      }
    }

    String tableType = description.path("type").asText("");
    table.tableRow[TablesHeader.TableType.ordinal()] = tableType;
    table.tableRow[TablesHeader.DataSourceFormat.ordinal()] =
        description.path("provider").asText("").toUpperCase(Locale.ROOT);
    table.tableRow[TablesHeader.StoragePath.ordinal()] = description.path("location").asText("");
    JsonNode properties = description.path("table_properties");
    table.tableRow[TablesHeader.TableProperties.ordinal()] =
        properties.isEmpty() ? "" : properties.toString();

    ImmutableList.Builder<Object[]> columnRows = ImmutableList.builder();
    int ordinalPosition = 0;
    for (JsonNode column : description.path("columns")) {
      columnRows.add(columnRow(table, ordinalPosition++, column, partitionColumns));
    }
    table.columnRows = columnRows.build();

    if ("VIEW".equalsIgnoreCase(tableType)) {
      table.viewRow = new Object[ViewsHeader.values().length];
      table.viewRow[ViewsHeader.TableCatalog.ordinal()] = HIVE_METASTORE_CATALOG;
      table.viewRow[ViewsHeader.TableSchema.ordinal()] = table.schema;
      table.viewRow[ViewsHeader.TableName.ordinal()] = table.name;
      table.viewRow[ViewsHeader.ViewDefinition.ordinal()] =
          description.path("view_text").asText("");
    }
  }

  private static Object[] columnRow(
      TableRef table, int ordinalPosition, JsonNode column, JsonNode partitionColumns) {
    JsonNode type = column.path("type");
    String typeName = type.path("name").asText("").toLowerCase(Locale.ROOT);
    String columnName = column.path("name").asText("");
    Object[] record = new Object[ColumnsHeader.values().length];
    record[ColumnsHeader.TableCatalog.ordinal()] = HIVE_METASTORE_CATALOG;
    record[ColumnsHeader.TableSchema.ordinal()] = table.schema;
    record[ColumnsHeader.TableName.ordinal()] = table.name;
    record[ColumnsHeader.OrdinalPosition.ordinal()] = ordinalPosition;
    record[ColumnsHeader.ColumnName.ordinal()] = columnName;
    record[ColumnsHeader.DataType.ordinal()] =
        UNITY_CATALOG_TYPE_NAMES.getOrDefault(typeName, typeName.toUpperCase(Locale.ROOT));
    record[ColumnsHeader.FullDataType.ordinal()] = formatDataType(type);
    record[ColumnsHeader.IsNullable.ordinal()] =
        column.path("nullable").asBoolean(true) ? "YES" : "NO";
    record[ColumnsHeader.ColumnDefault.ordinal()] = column.path("default").asText("");
    // Only the char and varchar types have a length, and only decimal a precision and a scale.
    record[ColumnsHeader.CharacterMaximumLength.ordinal()] = type.path("length").asText("");
    record[ColumnsHeader.NumericPrecision.ordinal()] = type.path("precision").asText("");
    record[ColumnsHeader.NumericScale.ordinal()] = type.path("scale").asText("");
    record[ColumnsHeader.Comment.ordinal()] = column.path("comment").asText("");
    for (int index = 0; index < partitionColumns.size(); index++) {
      if (partitionColumns.get(index).asText().equalsIgnoreCase(columnName)) {
        record[ColumnsHeader.PartitionIndex.ordinal()] = index;
        break;
      }
    }
    return record;
  }

  /** Returns a {@code DESCRIBE TABLE ... AS JSON} column type as Databricks prints it. */
  private static String formatDataType(JsonNode type) {
    String name = type.path("name").asText("").toLowerCase(Locale.ROOT);
    switch (name) {
      case "decimal":
        return "decimal("
            + type.path("precision").asText()
            + ","
            + type.path("scale").asText()
            + ")";
      case "char":
      case "varchar":
        return name + "(" + type.path("length").asText() + ")";
      case "string":
        String collation = type.path("collation").asText("");
        return collation.isEmpty() || DEFAULT_COLLATION.equalsIgnoreCase(collation)
            ? "string"
            : "string collate " + collation;
      case "interval":
        String startUnit = type.path("start_unit").asText();
        String endUnit = type.path("end_unit").asText();
        return startUnit.equals(endUnit)
            ? "interval " + startUnit
            : "interval " + startUnit + " to " + endUnit;
      case "array":
        return "array<" + formatDataType(type.path("element_type")) + ">";
      case "map":
        return "map<"
            + formatDataType(type.path("key_type"))
            + ","
            + formatDataType(type.path("value_type"))
            + ">";
      case "struct":
        StringBuilder struct = new StringBuilder("struct<");
        for (JsonNode field : type.path("fields")) {
          if (struct.length() > "struct<".length()) {
            struct.append(',');
          }
          struct
              .append(quoteIdentifierIfNeeded(field.path("name").asText("")))
              .append(':')
              .append(formatDataType(field.path("type")));
          if (!field.path("nullable").asBoolean(true)) {
            struct.append(" not null");
          }
          String comment = field.path("comment").asText("");
          if (!comment.isEmpty()) {
            struct.append(" comment ").append(quoteStringLiteral(comment));
          }
        }
        return struct.append('>').toString();
      case "timestamp_ltz":
        return "timestamp";
      default:
        return name;
    }
  }

  /**
   * A table listed by SHOW TABLES and its rows. Until DESCRIBE TABLE succeeds, the tables row has
   * only the names, and there are no columns or views rows.
   */
  static final class TableRef {
    final String schema;
    final String name;
    final Object[] tableRow;
    ImmutableList<Object[]> columnRows = ImmutableList.of();

    /** The views row, or null if the table isn't a view. */
    Object[] viewRow;

    TableRef(String schema, String name) {
      this.schema = schema;
      this.name = name;
      this.tableRow = new Object[] {HIVE_METASTORE_CATALOG, schema, name, "", "", "", ""};
    }

    String quotedName() {
      return quoteQualifiedName(this.schema, this.name);
    }
  }
}
