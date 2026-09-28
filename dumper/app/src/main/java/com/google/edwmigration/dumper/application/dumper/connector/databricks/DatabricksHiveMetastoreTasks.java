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

import static com.google.common.base.CharMatcher.whitespace;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksFilter.HIVE_METASTORE_CATALOG;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreParser.hasStringColumn;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreParser.parseDescribeTableTypes;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreParser.withCharVarcharLengths;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.cell;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.cellOrEmpty;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.quoteIdentifier;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDumpFormat.CatalogsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDumpFormat.ColumnsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDumpFormat.DdlsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDumpFormat.FunctionDdlsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDumpFormat.FunctionsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDumpFormat.SchemataHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDumpFormat.TableConstraintsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDumpFormat.TablesHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDumpFormat.ViewsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreParser.ParsedColumn;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreParser.ParsedFunctionExtended;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreParser.ParsedTableConstraint;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreParser.ParsedTableExtended;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.function.IOSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extraction tasks for the legacy {@code hive_metastore} catalog using request-scoped {@code SHOW
 * TABLE EXTENDED}, {@code DESCRIBE TABLE}, {@code SHOW CREATE TABLE}, and {@code DESCRIBE FUNCTION
 * EXTENDED}.
 */
final class DatabricksHiveMetastoreTasks {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksHiveMetastoreTasks.class);

  private DatabricksHiveMetastoreTasks() {}

  /**
   * Shared per-connector cache of {@code hive_metastore} schemas, parsed tables and parsed
   * functions.
   *
   * <p>Each list is loaded once. A failed load is remembered, so the other tasks that need the list
   * fail fast instead of repeating a scan that already failed.
   */
  static final class HiveMetastoreSessionCache {

    private final Memoized<ImmutableList<String>> schemas =
        new Memoized<>("Listing hive_metastore schemas");
    private final Memoized<ImmutableList<ParsedTableExtended>> tables =
        new Memoized<>("Listing hive_metastore tables");
    private final Memoized<ImmutableList<ParsedFunctionExtended>> functions =
        new Memoized<>("Listing hive_metastore functions");

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
    ImmutableList<ParsedFunctionExtended> listParsedFunctions(@Nonnull DatabricksHandle handle)
        throws IOException {
      return functions.load(() -> loadFunctions(handle));
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
              DatabricksHiveMetastoreParser.parseShowTableExtended(
                  rowSchema.isEmpty() ? schema : rowSchema, rowTable, infoBlob);
          if (table.columns.isEmpty() && !"VIEW".equalsIgnoreCase(table.tableType)) {
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

    @Nonnull
    private ImmutableList<ParsedFunctionExtended> loadFunctions(@Nonnull DatabricksHandle handle)
        throws IOException {
      List<FunctionRef> functionRefs = new ArrayList<>();
      handle
          .statementRunner()
          .runInOrder(
              "SHOW USER FUNCTIONS",
              listSchemas(handle),
              DatabricksSqlHelper::quoteIdentifier,
              schema -> showUserFunctions(handle, schema),
              (schema, schemaFunctionRefs) -> functionRefs.addAll(schemaFunctionRefs));
      ImmutableList.Builder<ParsedFunctionExtended> builder = ImmutableList.builder();
      handle
          .statementRunner()
          .runInOrder(
              "DESCRIBE FUNCTION EXTENDED",
              functionRefs,
              FunctionRef::quotedName,
              functionRef -> describeFunction(handle, functionRef),
              (functionRef, parsed) -> builder.add(parsed));
      return builder.build();
    }

    @Nonnull
    private static ImmutableList<FunctionRef> showUserFunctions(
        @Nonnull DatabricksHandle handle, @Nonnull String schema) throws IOException {
      String query = "SHOW USER FUNCTIONS IN " + quoteIdentifier(schema);
      ImmutableList.Builder<FunctionRef> builder = ImmutableList.builder();
      try (CloseableIterable<List<String>> rows =
          handle.sqlHelper().executeSqlInCatalog(HIVE_METASTORE_CATALOG, query)) {
        for (List<String> row : rows) {
          String rawName = cell(row, 0);
          if (rawName != null && !rawName.trim().isEmpty()) {
            builder.add(new FunctionRef(schema, extractShortFunctionName(rawName.trim())));
          }
        }
      }
      return builder.build();
    }

    @Nonnull
    private static ParsedFunctionExtended describeFunction(
        @Nonnull DatabricksHandle handle, @Nonnull FunctionRef functionRef) throws IOException {
      String query = "DESCRIBE FUNCTION EXTENDED " + functionRef.quotedName();
      List<String> lines = new ArrayList<>();
      try (CloseableIterable<List<String>> rows =
          handle.sqlHelper().executeSqlInCatalog(HIVE_METASTORE_CATALOG, query)) {
        for (List<String> row : rows) {
          String line = cell(row, 0);
          if (line != null) {
            lines.add(line);
          }
        }
      }
      return DatabricksHiveMetastoreParser.parseDescribeFunctionExtended(
          HIVE_METASTORE_CATALOG, functionRef.schema, functionRef.name, lines);
    }

    private static String extractShortFunctionName(String fullName) {
      int lastDot = fullName.lastIndexOf('.');
      return lastDot >= 0 ? fullName.substring(lastDot + 1) : fullName;
    }
  }

  /** A user function in a {@code hive_metastore} schema. */
  private static final class FunctionRef {

    final String schema;
    final String name;

    FunctionRef(@Nonnull String schema, @Nonnull String name) {
      this.schema = schema;
      this.name = name;
    }

    @Nonnull
    String quotedName() {
      return quoteIdentifier(schema) + "." + quoteIdentifier(name);
    }
  }

  /**
   * Loads a value at most once. A failed load is remembered and reported to later callers instead
   * of being retried.
   */
  private static final class Memoized<T> {

    private final String description;

    @GuardedBy("this")
    @CheckForNull
    private T value;

    @GuardedBy("this")
    @CheckForNull
    private Exception failure;

    Memoized(@Nonnull String description) {
      this.description = description;
    }

    @Nonnull
    synchronized T load(@Nonnull IOSupplier<T> loader) throws IOException {
      if (value != null) {
        return value;
      }

      if (failure != null) {
        throw new IOException(
            description + " failed earlier and is not retried: " + failure.getMessage(), failure);
      }

      try {
        value = loader.get();
        return value;
      } catch (IOException | RuntimeException e) {
        failure = e;
        throw e;
      }
    }
  }

  @Nonnull
  private static String quoteTableName(@Nonnull ParsedTableExtended table) {
    return quoteIdentifier(table.schemaName) + "." + quoteIdentifier(table.tableName);
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
      super(DatabricksHiveMetastoreDumpFormat.CATALOGS_ENTRY_NAME, sessionCache);
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
        csv.printRecord(HIVE_METASTORE_CATALOG, "Legacy Hive Metastore", "", "", "");
      }
      return null;
    }
  }

  static final class DatabricksHiveMetastoreSchemataTask
      extends AbstractDatabricksHiveMetastoreTask {

    DatabricksHiveMetastoreSchemataTask(@Nonnull HiveMetastoreSessionCache sessionCache) {
      super(DatabricksHiveMetastoreDumpFormat.SCHEMATA_ENTRY_NAME, sessionCache);
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
                        description.location));
      }
      return null;
    }

    /**
     * Describes a schema. The schema name is already known from {@code SHOW SCHEMAS}, so if {@code
     * DESCRIBE SCHEMA EXTENDED} fails, the schema is still written, without comment, owner and
     * location.
     */
    @Nonnull
    private static SchemaDescription describeSchemaOrEmpty(
        @Nonnull DatabricksHandle handle, @Nonnull String schema) {
      String query = "DESCRIBE SCHEMA EXTENDED " + quoteIdentifier(schema);
      String comment = "";
      String owner = "";
      String location = "";
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
          }
        }
      } catch (IOException e) {
        return emptySchemaDescription(schema, e);
      } catch (UncheckedIOException e) {
        return emptySchemaDescription(schema, e.getCause());
      }
      return new SchemaDescription(comment, owner, location);
    }

    @Nonnull
    private static SchemaDescription emptySchemaDescription(
        @Nonnull String schema, @Nonnull IOException failure) {
      logger.warn(
          "Writing schema {} without comment, owner and location because DESCRIBE SCHEMA EXTENDED"
              + " failed: {}",
          quoteIdentifier(schema),
          failure.getMessage());
      return new SchemaDescription("", "", "");
    }
  }

  private static final class SchemaDescription {

    final String comment;
    final String owner;
    final String location;

    SchemaDescription(@Nonnull String comment, @Nonnull String owner, @Nonnull String location) {
      this.comment = comment;
      this.owner = owner;
      this.location = location;
    }
  }

  static final class DatabricksHiveMetastoreTablesTask extends AbstractDatabricksHiveMetastoreTask {

    DatabricksHiveMetastoreTablesTask(@Nonnull HiveMetastoreSessionCache sessionCache) {
      super(DatabricksHiveMetastoreDumpFormat.TABLES_ENTRY_NAME, sessionCache);
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
              tbl.dataSourceFormat,
              tbl.storageLocation,
              tbl.comment,
              tbl.owner,
              tbl.createdAt,
              "",
              tbl.serdeLibrary,
              tbl.inputFormat,
              tbl.outputFormat,
              tbl.storageProperties,
              tbl.tableProperties);
        }
      }
      return null;
    }
  }

  /**
   * Writes the columns of each table and view in {@code hive_metastore} to {@code columns.csv}.
   *
   * <p>{@code SHOW TABLE EXTENDED} reports {@code CHAR} and {@code VARCHAR} columns as {@code
   * string}, so {@code DESCRIBE TABLE} runs for each table and view that has a {@code string} type,
   * to find their lengths.
   */
  static final class DatabricksHiveMetastoreColumnsTask
      extends AbstractDatabricksHiveMetastoreTask {

    DatabricksHiveMetastoreColumnsTask(@Nonnull HiveMetastoreSessionCache sessionCache) {
      super(DatabricksHiveMetastoreDumpFormat.COLUMNS_ENTRY_NAME, sessionCache);
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
                table -> findColumnsWithLengths(dbxHandle, table),
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
                        "",
                        column.partitionIndex);
                  }
                });
      }
      return null;
    }

    /**
     * Returns the columns of a table with the lengths of its {@code CHAR} and {@code VARCHAR}
     * columns. The columns are already known from {@code SHOW TABLE EXTENDED}, so if {@code
     * DESCRIBE TABLE} fails, they are returned with {@code string} types.
     */
    @Nonnull
    private static ImmutableList<ParsedColumn> findColumnsWithLengths(
        @Nonnull DatabricksHandle handle, @Nonnull ParsedTableExtended table) {
      if (!hasStringColumn(table)) {
        return table.columns;
      }

      String query = "DESCRIBE TABLE " + quoteTableName(table);
      try (CloseableIterable<List<String>> rows =
          handle.sqlHelper().executeSqlInCatalog(HIVE_METASTORE_CATALOG, query)) {
        return withCharVarcharLengths(table.columns, parseDescribeTableTypes(rows));
      } catch (IOException e) {
        return columnsWithoutLengths(table, e);
      } catch (UncheckedIOException e) {
        return columnsWithoutLengths(table, e.getCause());
      }
    }

    @Nonnull
    private static ImmutableList<ParsedColumn> columnsWithoutLengths(
        @Nonnull ParsedTableExtended table, @Nonnull IOException failure) {
      logger.warn(
          "Writing the CHAR and VARCHAR columns of {} as string because DESCRIBE TABLE failed: {}",
          quoteTableName(table),
          failure.getMessage());
      return table.columns;
    }
  }

  static final class DatabricksHiveMetastoreViewsTask extends AbstractDatabricksHiveMetastoreTask {

    DatabricksHiveMetastoreViewsTask(@Nonnull HiveMetastoreSessionCache sessionCache) {
      super(DatabricksHiveMetastoreDumpFormat.VIEWS_ENTRY_NAME, sessionCache);
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
          if (tbl.viewDefinition != null || "VIEW".equalsIgnoreCase(tbl.tableType)) {
            csv.printRecord(
                tbl.catalogName,
                tbl.schemaName,
                tbl.tableName,
                tbl.viewDefinition != null ? tbl.viewDefinition : "");
          }
        }
      }
      return null;
    }
  }

  static final class DatabricksHiveMetastoreTableConstraintsTask
      extends AbstractDatabricksHiveMetastoreTask {

    DatabricksHiveMetastoreTableConstraintsTask(@Nonnull HiveMetastoreSessionCache sessionCache) {
      super(DatabricksHiveMetastoreDumpFormat.TABLE_CONSTRAINTS_ENTRY_NAME, sessionCache);
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
                tc.constraintDetails);
          }
        }
      }
      return null;
    }
  }

  static final class DatabricksHiveMetastoreFunctionsTask
      extends AbstractDatabricksHiveMetastoreTask {

    DatabricksHiveMetastoreFunctionsTask(@Nonnull HiveMetastoreSessionCache sessionCache) {
      super(DatabricksHiveMetastoreDumpFormat.FUNCTIONS_ENTRY_NAME, sessionCache);
    }

    @Override
    @Nonnull
    public String getName() {
      return "databricks-hms-functions";
    }

    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      DatabricksHandle dbxHandle = (DatabricksHandle) handle;
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(FunctionsHeader.class).print(writer)) {
        for (ParsedFunctionExtended parsed : sessionCache.listParsedFunctions(dbxHandle)) {
          csv.printRecord(
              parsed.functionCatalog,
              parsed.functionSchema,
              parsed.functionName,
              parsed.dataType,
              parsed.inputParams,
              parsed.routineDefinition,
              parsed.routineLanguage,
              parsed.comment,
              parsed.owner);
        }
      }
      return null;
    }
  }

  /**
   * Complementary task that runs {@code SHOW CREATE TABLE} for each table and view in {@code
   * hive_metastore} and writes the DDL statements to {@code ddls.csv}.
   */
  static final class DatabricksHiveMetastoreDdlsTask extends AbstractDatabricksHiveMetastoreTask {

    DatabricksHiveMetastoreDdlsTask(@Nonnull HiveMetastoreSessionCache sessionCache) {
      super(DatabricksHiveMetastoreDumpFormat.DDLS_ENTRY_NAME, sessionCache);
    }

    @Override
    @Nonnull
    public String getName() {
      return "databricks-hms-ddls";
    }

    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      DatabricksHandle dbxHandle = (DatabricksHandle) handle;
      ImmutableList<ParsedTableExtended> tables = sessionCache.listParsedTables(dbxHandle);
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(DdlsHeader.class).print(writer)) {
        dbxHandle
            .statementRunner()
            .runInOrder(
                "SHOW CREATE TABLE",
                tables,
                DatabricksHiveMetastoreTasks::quoteTableName,
                table -> showCreateTable(dbxHandle, table),
                (table, ddl) -> {
                  if (!ddl.isEmpty()) {
                    csv.printRecord(
                        table.catalogName, table.schemaName, table.tableName, table.tableType, ddl);
                  }
                });
      }
      return null;
    }

    /** Returns the DDL without the line break that Databricks appends to it. */
    @Nonnull
    private static String showCreateTable(
        @Nonnull DatabricksHandle handle, @Nonnull ParsedTableExtended table) throws IOException {
      String query = "SHOW CREATE TABLE " + quoteTableName(table);
      StringBuilder ddl = new StringBuilder();
      try (CloseableIterable<List<String>> rows =
          handle.sqlHelper().executeSqlInCatalog(HIVE_METASTORE_CATALOG, query)) {
        for (List<String> row : rows) {
          String part = cell(row, 0);
          if (part != null) {
            if (ddl.length() > 0) {
              ddl.append("\n");
            }
            ddl.append(part);
          }
        }
      }
      return whitespace().trimTrailingFrom(ddl);
    }
  }

  /**
   * Writes the {@code CREATE FUNCTION} DDLs synthesized from {@code DESCRIBE FUNCTION EXTENDED} to
   * {@code function_ddls.csv}.
   */
  static final class DatabricksHiveMetastoreFunctionDdlsTask
      extends AbstractDatabricksHiveMetastoreTask {

    DatabricksHiveMetastoreFunctionDdlsTask(@Nonnull HiveMetastoreSessionCache sessionCache) {
      super(DatabricksHiveMetastoreDumpFormat.FUNCTION_DDLS_ENTRY_NAME, sessionCache);
    }

    @Override
    @Nonnull
    public String getName() {
      return "databricks-hms-function-ddls";
    }

    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      DatabricksHandle dbxHandle = (DatabricksHandle) handle;
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(FunctionDdlsHeader.class).print(writer)) {
        for (ParsedFunctionExtended function : sessionCache.listParsedFunctions(dbxHandle)) {
          if (!function.ddl.isEmpty()) {
            csv.printRecord(
                function.functionCatalog,
                function.functionSchema,
                function.functionName,
                function.ddl);
          }
        }
      }
      return null;
    }
  }
}
