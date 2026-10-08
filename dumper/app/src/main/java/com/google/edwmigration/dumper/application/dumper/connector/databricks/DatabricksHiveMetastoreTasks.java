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

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksFilter.HIVE_METASTORE_CATALOG;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.parseDescribeTableColumns;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.withDescribedColumns;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksJson.toJsonOrEmpty;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.cell;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.cellOrEmpty;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.quoteIdentifier;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.ParsedColumn;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.ParsedTableConstraint;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.ParsedTableExtended;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.CatalogsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.ColumnsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.SchemataHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.TableConstraintsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.TablesHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.ViewsHeader;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extraction tasks for the legacy {@code hive_metastore} catalog using {@code SHOW SCHEMAS}, {@code
 * DESCRIBE SCHEMA EXTENDED}, {@code SHOW TABLE EXTENDED} and {@code DESCRIBE TABLE}.
 */
final class DatabricksHiveMetastoreTasks {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksHiveMetastoreTasks.class);

  /** The comment of the {@code hive_metastore} row in the catalogs file. */
  private static final String HIVE_METASTORE_CATALOG_COMMENT = "Legacy Hive Metastore";

  /** The prefix and suffix of the {@code Properties} of {@code DESCRIBE SCHEMA EXTENDED}. */
  private static final String SCHEMA_PROPERTIES_PREFIX = "((";

  private static final String SCHEMA_PROPERTIES_SUFFIX = "))";

  /** The separator between two {@code (key,value)} entries of schema properties. */
  private static final String SCHEMA_PROPERTIES_SEPARATOR = "), (";

  private DatabricksHiveMetastoreTasks() {}

  /**
   * Shared per-connector cache of {@code hive_metastore} schemas and parsed tables.
   *
   * <p>Each list is loaded once. A failed load is remembered, so the other tasks that need the list
   * fail fast instead of repeating a scan that already failed.
   */
  static final class HiveMetastoreSessionCache {

    private final Memoized<ImmutableList<String>> schemas =
        new Memoized<>("Listing hive_metastore schemas");
    private final Memoized<ImmutableList<ParsedTableExtended>> tables =
        new Memoized<>("Listing hive_metastore tables");

    @Nonnull
    ImmutableList<String> listSchemas(@Nonnull DatabricksHandle handle) throws IOException {
      return schemas.load(() -> loadSchemas(handle));
    }

    @Nonnull
    ImmutableList<ParsedTableExtended> listParsedTables(@Nonnull DatabricksHandle handle)
        throws IOException {
      return tables.load(() -> loadTables(handle));
    }

    @Nonnull
    private static ImmutableList<String> loadSchemas(@Nonnull DatabricksHandle handle)
        throws IOException {
      ImmutableList.Builder<String> builder = ImmutableList.builder();
      try (CloseableIterable<List<String>> rows =
          handle.sqlHelper().executeSqlInCatalog(HIVE_METASTORE_CATALOG, "SHOW SCHEMAS")) {
        for (List<String> row : rows) {
          String schemaName = cell(row, 0);
          if (schemaName != null && handle.filter().acceptsSchema(schemaName)) {
            builder.add(schemaName);
          }
        }
      }
      return builder.build();
    }

    @Nonnull
    private ImmutableList<ParsedTableExtended> loadTables(@Nonnull DatabricksHandle handle)
        throws IOException {
      ImmutableList.Builder<ParsedTableExtended> builder = ImmutableList.builder();
      handle
          .statementRunner()
          .runInOrder(
              "SHOW TABLE EXTENDED",
              listSchemas(handle),
              DatabricksSqlHelper::quoteIdentifier,
              schema -> showTableExtended(handle, schema),
              (schema, schemaTables) -> builder.addAll(schemaTables));
      return builder.build();
    }

    @Nonnull
    private static ImmutableList<ParsedTableExtended> showTableExtended(
        @Nonnull DatabricksHandle handle, @Nonnull String schema) throws IOException {
      String query = String.format("SHOW TABLE EXTENDED IN %s LIKE '*'", quoteIdentifier(schema));
      ImmutableList.Builder<ParsedTableExtended> builder = ImmutableList.builder();
      try (CloseableIterable<List<String>> rows =
          handle.sqlHelper().executeSqlInCatalog(HIVE_METASTORE_CATALOG, query)) {
        for (List<String> row : rows) {
          if (row == null || row.isEmpty()) {
            continue;
          }
          String isTemp = row.size() > 2 ? cell(row, 2) : null;
          if ("true".equalsIgnoreCase(isTemp)) {
            continue;
          }
          String rowSchema = row.size() > 1 ? cellOrEmpty(row, 0) : schema;
          String rowTable = row.size() > 1 ? cellOrEmpty(row, 1) : "";
          String infoBlob = row.size() > 3 ? cell(row, 3) : cell(row, row.size() - 1);
          ParsedTableExtended table =
              DatabricksHiveMetastoreTableParser.parseShowTableExtended(
                  rowSchema.isEmpty() ? schema : rowSchema, rowTable, infoBlob);
          if (table.columns.isEmpty() && !isView(table)) {
            logger.warn(
                "Table {} has no columns in SHOW TABLE EXTENDED. Its storage may be unreadable,"
                    + " in which case its CreatedAt is the time of this query.",
                quoteTableName(table));
          }
          builder.add(table);
        }
      }
      return builder.build();
    }
  }

  @Nonnull
  static String quoteTableName(@Nonnull ParsedTableExtended table) {
    return quoteIdentifier(table.schemaName) + "." + quoteIdentifier(table.tableName);
  }

  private static boolean isView(@Nonnull ParsedTableExtended table) {
    return "VIEW".equalsIgnoreCase(table.tableType);
  }

  /**
   * Parses the {@code Properties} that {@code DESCRIBE SCHEMA EXTENDED} reports, such as {@code
   * ((owner_team,data), (purpose,legacy))}, in their printed order.
   *
   * <p>Spark prints the keys and values without escaping, so the entries are split on a best-effort
   * basis: an entry ends before {@code ), (}, and its key ends before its first comma.
   */
  @Nonnull
  static ImmutableMap<String, String> parseSchemaProperties(@Nonnull String text) {
    String trimmed = text.trim();
    if (!trimmed.startsWith(SCHEMA_PROPERTIES_PREFIX)
        || !trimmed.endsWith(SCHEMA_PROPERTIES_SUFFIX)
        || trimmed.length()
            < SCHEMA_PROPERTIES_PREFIX.length() + SCHEMA_PROPERTIES_SUFFIX.length()) {
      return ImmutableMap.of();
    }

    String body =
        trimmed.substring(
            SCHEMA_PROPERTIES_PREFIX.length(),
            trimmed.length() - SCHEMA_PROPERTIES_SUFFIX.length());
    Map<String, String> properties = new LinkedHashMap<>();
    int entryStart = 0;
    while (entryStart <= body.length()) {
      int entryEnd = body.indexOf(SCHEMA_PROPERTIES_SEPARATOR, entryStart);
      if (entryEnd < 0) {
        entryEnd = body.length();
      }
      String entry = body.substring(entryStart, entryEnd);
      int separator = entry.indexOf(',');
      if (separator < 0) {
        properties.put(entry, "");
      } else {
        properties.put(entry.substring(0, separator), entry.substring(separator + 1));
      }
      entryStart = entryEnd + SCHEMA_PROPERTIES_SEPARATOR.length();
    }
    return ImmutableMap.copyOf(properties);
  }

  abstract static class AbstractDatabricksHiveMetastoreTask extends AbstractTask<Void> {

    protected final HiveMetastoreSessionCache sessionCache;

    AbstractDatabricksHiveMetastoreTask(
        @Nonnull String targetPath, @Nonnull HiveMetastoreSessionCache sessionCache) {
      super(targetPath);
      this.sessionCache = sessionCache;
    }

    @Override
    @Nonnull
    public TaskCategory getCategory() {
      return TaskCategory.OPTIONAL;
    }
  }

  static final class DatabricksHiveMetastoreCatalogsTask
      extends AbstractDatabricksHiveMetastoreTask {

    DatabricksHiveMetastoreCatalogsTask(@Nonnull HiveMetastoreSessionCache sessionCache) {
      super(DatabricksMetadataDumpFormat.HIVE_METASTORE_CATALOGS_ENTRY_NAME, sessionCache);
    }

    @Override
    @Nonnull
    public String getName() {
      return "databricks-hms-catalogs";
    }

    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(CatalogsHeader.class).print(writer)) {
        Object[] record = new Object[CatalogsHeader.values().length];
        record[CatalogsHeader.CatalogName.ordinal()] = HIVE_METASTORE_CATALOG;
        record[CatalogsHeader.Comment.ordinal()] = HIVE_METASTORE_CATALOG_COMMENT;
        csv.printRecord(record);
      }
      return null;
    }
  }

  static final class DatabricksHiveMetastoreSchemataTask
      extends AbstractDatabricksHiveMetastoreTask {

    DatabricksHiveMetastoreSchemataTask(@Nonnull HiveMetastoreSessionCache sessionCache) {
      super(DatabricksMetadataDumpFormat.HIVE_METASTORE_SCHEMATA_ENTRY_NAME, sessionCache);
    }

    @Override
    @Nonnull
    public String getName() {
      return "databricks-hms-schemata";
    }

    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      DatabricksHandle dbxHandle = (DatabricksHandle) handle;
      ImmutableList<String> schemas = sessionCache.listSchemas(dbxHandle);
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(SchemataHeader.class).print(writer)) {
        dbxHandle
            .statementRunner()
            .runInOrder(
                "DESCRIBE SCHEMA EXTENDED",
                schemas,
                DatabricksSqlHelper::quoteIdentifier,
                schema -> describeSchemaOrEmpty(dbxHandle, schema),
                (schema, description) ->
                    csv.printRecord(
                        HIVE_METASTORE_CATALOG,
                        schema,
                        description.comment,
                        description.owner,
                        "",
                        "",
                        "",
                        description.location,
                        toJsonOrEmpty(description.properties)));
      }
      return null;
    }

    /**
     * Describes a schema. The schema name is already known from {@code SHOW SCHEMAS}, so if {@code
     * DESCRIBE SCHEMA EXTENDED} fails, the schema is still written, without comment, owner,
     * location and properties.
     */
    @Nonnull
    private static SchemaDescription describeSchemaOrEmpty(
        @Nonnull DatabricksHandle handle, @Nonnull String schema) {
      String query = "DESCRIBE SCHEMA EXTENDED " + quoteIdentifier(schema);
      String comment = "";
      String owner = "";
      String location = "";
      ImmutableMap<String, String> properties = ImmutableMap.of();
      try (CloseableIterable<List<String>> rows =
          handle.sqlHelper().executeSqlInCatalog(HIVE_METASTORE_CATALOG, query)) {
        for (List<String> row : rows) {
          String key = cellOrEmpty(row, 0);
          String value = cellOrEmpty(row, 1);
          if ("Comment".equalsIgnoreCase(key)) {
            comment = value;
          } else if ("Owner".equalsIgnoreCase(key)) {
            owner = value;
          } else if ("Location".equalsIgnoreCase(key)) {
            location = value;
          } else if ("Properties".equalsIgnoreCase(key)) {
            properties = parseSchemaProperties(value);
          }
        }
      } catch (IOException e) {
        return emptySchemaDescription(schema, e);
      } catch (UncheckedIOException e) {
        return emptySchemaDescription(schema, e.getCause());
      }
      return new SchemaDescription(comment, owner, location, properties);
    }

    @Nonnull
    private static SchemaDescription emptySchemaDescription(
        @Nonnull String schema, @Nonnull IOException failure) {
      logger.warn(
          "Writing schema {} without comment, owner, location and properties because DESCRIBE"
              + " SCHEMA EXTENDED failed: {}",
          quoteIdentifier(schema),
          failure.getMessage());
      return new SchemaDescription("", "", "", ImmutableMap.of());
    }
  }

  private static final class SchemaDescription {

    final String comment;
    final String owner;
    final String location;
    final ImmutableMap<String, String> properties;

    SchemaDescription(
        @Nonnull String comment,
        @Nonnull String owner,
        @Nonnull String location,
        @Nonnull ImmutableMap<String, String> properties) {
      this.comment = comment;
      this.owner = owner;
      this.location = location;
      this.properties = properties;
    }
  }

  static final class DatabricksHiveMetastoreTablesTask extends AbstractDatabricksHiveMetastoreTask {

    DatabricksHiveMetastoreTablesTask(@Nonnull HiveMetastoreSessionCache sessionCache) {
      super(DatabricksMetadataDumpFormat.HIVE_METASTORE_TABLES_ENTRY_NAME, sessionCache);
    }

    @Override
    @Nonnull
    public String getName() {
      return "databricks-hms-tables";
    }

    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      DatabricksHandle dbxHandle = (DatabricksHandle) handle;
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(TablesHeader.class).print(writer)) {
        for (ParsedTableExtended tbl : sessionCache.listParsedTables(dbxHandle)) {
          csv.printRecord(
              tbl.catalogName,
              tbl.schemaName,
              tbl.tableName,
              tbl.tableType,
              "",
              "",
              tbl.dataSourceFormat,
              tbl.storageLocation,
              tbl.comment,
              tbl.owner,
              tbl.createdAt,
              "",
              tbl.serdeLibrary,
              tbl.inputFormat,
              tbl.outputFormat,
              toJsonOrEmpty(tbl.storageProperties),
              toJsonOrEmpty(tbl.tableProperties),
              tbl.numBuckets,
              toJsonOrEmpty(tbl.bucketColumns),
              toJsonOrEmpty(tbl.sortColumns),
              "");
        }
      }
      return null;
    }
  }

  /**
   * Writes the columns of each table and view in {@code hive_metastore}.
   *
   * <p>{@code SHOW TABLE EXTENDED} omits column comments and reports {@code CHAR} and {@code
   * VARCHAR} columns as {@code string}, so {@code DESCRIBE TABLE} runs for each table and view to
   * find the comments and the lengths.
   */
  static final class DatabricksHiveMetastoreColumnsTask
      extends AbstractDatabricksHiveMetastoreTask {

    DatabricksHiveMetastoreColumnsTask(@Nonnull HiveMetastoreSessionCache sessionCache) {
      super(DatabricksMetadataDumpFormat.HIVE_METASTORE_COLUMNS_ENTRY_NAME, sessionCache);
    }

    @Override
    @Nonnull
    public String getName() {
      return "databricks-hms-columns";
    }

    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      DatabricksHandle dbxHandle = (DatabricksHandle) handle;
      ImmutableList<ParsedTableExtended> tables = sessionCache.listParsedTables(dbxHandle);
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(ColumnsHeader.class).print(writer)) {
        dbxHandle
            .statementRunner()
            .runInOrder(
                "DESCRIBE TABLE",
                tables,
                DatabricksHiveMetastoreTasks::quoteTableName,
                table -> describeColumns(dbxHandle, table),
                (table, columns) -> {
                  for (ParsedColumn column : columns) {
                    csv.printRecord(
                        table.catalogName,
                        table.schemaName,
                        table.tableName,
                        String.valueOf(column.ordinalPosition),
                        column.columnName,
                        column.dataType,
                        column.isNullable,
                        column.comment,
                        column.partitionIndex,
                        "",
                        "",
                        "",
                        "",
                        "",
                        "");
                  }
                });
      }
      return null;
    }

    /**
     * Returns the columns of a table with their comments and the lengths of their {@code CHAR} and
     * {@code VARCHAR} types. The columns are already known from {@code SHOW TABLE EXTENDED}, so if
     * {@code DESCRIBE TABLE} fails, they are returned without comments and with {@code string}
     * types.
     */
    @Nonnull
    private static ImmutableList<ParsedColumn> describeColumns(
        @Nonnull DatabricksHandle handle, @Nonnull ParsedTableExtended table) {
      if (table.columns.isEmpty()) {
        return table.columns;
      }

      String query = "DESCRIBE TABLE " + quoteTableName(table);
      try (CloseableIterable<List<String>> rows =
          handle.sqlHelper().executeSqlInCatalog(HIVE_METASTORE_CATALOG, query)) {
        return withDescribedColumns(table.columns, parseDescribeTableColumns(rows));
      } catch (IOException e) {
        return undescribedColumns(table, e);
      } catch (UncheckedIOException e) {
        return undescribedColumns(table, e.getCause());
      }
    }

    @Nonnull
    private static ImmutableList<ParsedColumn> undescribedColumns(
        @Nonnull ParsedTableExtended table, @Nonnull IOException failure) {
      logger.warn(
          "Writing the columns of {} without comments, and their CHAR and VARCHAR types as string,"
              + " because DESCRIBE TABLE failed: {}",
          quoteTableName(table),
          failure.getMessage());
      return table.columns;
    }
  }

  static final class DatabricksHiveMetastoreViewsTask extends AbstractDatabricksHiveMetastoreTask {

    DatabricksHiveMetastoreViewsTask(@Nonnull HiveMetastoreSessionCache sessionCache) {
      super(DatabricksMetadataDumpFormat.HIVE_METASTORE_VIEWS_ENTRY_NAME, sessionCache);
    }

    @Override
    @Nonnull
    public String getName() {
      return "databricks-hms-views";
    }

    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      DatabricksHandle dbxHandle = (DatabricksHandle) handle;
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(ViewsHeader.class).print(writer)) {
        for (ParsedTableExtended tbl : sessionCache.listParsedTables(dbxHandle)) {
          if (tbl.viewDefinition != null || isView(tbl)) {
            csv.printRecord(
                tbl.catalogName,
                tbl.schemaName,
                tbl.tableName,
                tbl.viewDefinition != null ? tbl.viewDefinition : "",
                tbl.viewCatalogAndNamespace,
                toJsonOrEmpty(tbl.viewQueryOutputColumns),
                tbl.viewSchemaMode,
                tbl.sqlPath);
          }
        }
      }
      return null;
    }
  }

  static final class DatabricksHiveMetastoreTableConstraintsTask
      extends AbstractDatabricksHiveMetastoreTask {

    DatabricksHiveMetastoreTableConstraintsTask(@Nonnull HiveMetastoreSessionCache sessionCache) {
      super(DatabricksMetadataDumpFormat.HIVE_METASTORE_TABLE_CONSTRAINTS_ENTRY_NAME, sessionCache);
    }

    @Override
    @Nonnull
    public String getName() {
      return "databricks-hms-table-constraints";
    }

    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      DatabricksHandle dbxHandle = (DatabricksHandle) handle;
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(TableConstraintsHeader.class).print(writer)) {
        for (ParsedTableExtended tbl : sessionCache.listParsedTables(dbxHandle)) {
          for (ParsedTableConstraint tc : tbl.constraints) {
            csv.printRecord(
                tbl.catalogName,
                tbl.schemaName,
                tbl.tableName,
                tc.constraintName,
                tc.constraintType,
                toJsonOrEmpty(ImmutableMap.of("expression", tc.expression)));
          }
        }
      }
      return null;
    }
  }
}
