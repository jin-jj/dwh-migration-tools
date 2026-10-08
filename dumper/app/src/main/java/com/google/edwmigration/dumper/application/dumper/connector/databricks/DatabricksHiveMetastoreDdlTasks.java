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
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreFunctionDdlFormatter.formatCreateFunction;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.quoteTableName;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.cell;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreFunctionParser.ParsedFunction;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreFunctionTasks.HiveMetastoreFunctionCache;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.ParsedTableExtended;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.AbstractDatabricksHiveMetastoreTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.HiveMetastoreSessionCache;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.DdlsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.FunctionDdlsHeader;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extraction tasks for the DDL statements of the legacy {@code hive_metastore} catalog: the {@code
 * SHOW CREATE TABLE} output of the tables and views, and the {@code CREATE FUNCTION} statements of
 * the functions.
 */
final class DatabricksHiveMetastoreDdlTasks {

  private static final Logger logger =
      LoggerFactory.getLogger(DatabricksHiveMetastoreDdlTasks.class);

  private DatabricksHiveMetastoreDdlTasks() {}

  /**
   * Runs {@code SHOW CREATE TABLE} for each table and view.
   *
   * <p>Databricks rejects {@code SHOW CREATE TABLE}, even {@code AS SERDE}, for Hive tables with an
   * unsupported SerDe configuration, such as some {@code RCFILE} and {@code SEQUENCEFILE} tables.
   * Those tables are skipped with a warning; the tables file describes their storage.
   */
  static final class DatabricksHiveMetastoreDdlsTask extends AbstractDatabricksHiveMetastoreTask {

    /** The error class of {@code SHOW CREATE TABLE} for a table with an unsupported Hive SerDe. */
    private static final String UNSUPPORTED_SERDE_ERROR_CLASS =
        "UNSUPPORTED_SHOW_CREATE_TABLE.WITH_UNSUPPORTED_SERDE_CONFIGURATION";

    DatabricksHiveMetastoreDdlsTask(@Nonnull HiveMetastoreSessionCache sessionCache) {
      super(DatabricksMetadataDumpFormat.HIVE_METASTORE_DDLS_ENTRY_NAME, sessionCache);
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

    /** Returns the DDL of a table without the line break that Databricks appends to it. */
    @Nonnull
    private static String showCreateTable(
        @Nonnull DatabricksHandle handle, @Nonnull ParsedTableExtended table) throws IOException {
      String query = "SHOW CREATE TABLE " + quoteTableName(table);
      StringBuilder ddl = new StringBuilder();
      try (CloseableIterable<List<String>> rows =
          handle.sqlHelper().executeSqlInCatalog(HIVE_METASTORE_CATALOG, query)) {
        for (List<String> row : rows) {
          String part = cell(row, 0);
          if (part == null) {
            continue;
          }

          if (ddl.length() > 0) {
            ddl.append('\n');
          }
          ddl.append(part);
        }
      } catch (IOException e) {
        warnIfUnsupportedSerde(table, e);
        throw e;
      } catch (UncheckedIOException e) {
        warnIfUnsupportedSerde(table, e.getCause());
        throw e;
      }
      return whitespace().trimTrailingFrom(ddl);
    }

    private static void warnIfUnsupportedSerde(
        @Nonnull ParsedTableExtended table, @Nonnull IOException failure) {
      String message = failure.getMessage();
      if (message != null && message.contains(UNSUPPORTED_SERDE_ERROR_CLASS)) {
        logger.warn(
            "Databricks doesn't support SHOW CREATE TABLE for {} because of its Hive SerDe"
                + " configuration; its storage is described in {}.",
            quoteTableName(table),
            DatabricksMetadataDumpFormat.HIVE_METASTORE_TABLES_ENTRY_NAME);
      }
    }
  }

  /** Writes the {@code CREATE FUNCTION} statement of each function that has a body or a class. */
  static final class DatabricksHiveMetastoreFunctionDdlsTask extends AbstractTask<Void> {

    private final HiveMetastoreFunctionCache functionCache;

    DatabricksHiveMetastoreFunctionDdlsTask(@Nonnull HiveMetastoreFunctionCache functionCache) {
      super(DatabricksMetadataDumpFormat.HIVE_METASTORE_FUNCTION_DDLS_ENTRY_NAME);
      this.functionCache = functionCache;
    }

    @Override
    @Nonnull
    public String getName() {
      return "databricks-hms-function-ddls";
    }

    @Override
    @Nonnull
    public TaskCategory getCategory() {
      return TaskCategory.OPTIONAL;
    }

    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      DatabricksHandle dbxHandle = (DatabricksHandle) handle;
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(FunctionDdlsHeader.class).print(writer)) {
        for (ParsedFunction function : functionCache.listParsedFunctions(dbxHandle)) {
          String ddl = formatCreateFunction(function);
          if (!ddl.isEmpty()) {
            csv.printRecord(
                function.functionCatalog, function.functionSchema, function.functionName, ddl);
          }
        }
      }
      return null;
    }
  }
}
