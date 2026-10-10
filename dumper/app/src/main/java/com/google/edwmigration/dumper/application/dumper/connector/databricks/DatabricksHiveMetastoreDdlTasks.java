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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.io.ByteSink;
import com.google.common.primitives.Ints;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreFunctionParser.ParsedFunction;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreFunctionTasks.HiveMetastoreFunctionCache;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.ParsedTable;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.AbstractDatabricksHiveMetastoreTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.HiveMetastoreSessionCache;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataConnector.DatabricksProperty;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataConnector.ShowCreateTableMode;
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
import java.util.Locale;
import java.util.Map;
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

  /**
   * The lower-case table properties of the Delta table features that allow the column clauses that
   * only the DDL shows: {@code GENERATED ALWAYS AS} and {@code GENERATED ... AS IDENTITY}.
   */
  private static final ImmutableSet<String> DDL_ONLY_FEATURE_PROPERTIES =
      ImmutableSet.of("delta.feature.generatedcolumns", "delta.feature.identitycolumns");

  private static final String MIN_WRITER_VERSION_PROPERTY = "delta.minWriterVersion";

  /**
   * The writer versions of the Delta protocols that allow generated columns (from 4) or identity
   * columns (6) without listing them as table features, which writer version 7 introduced.
   */
  private static final Range<Integer> LEGACY_GENERATED_COLUMNS_WRITER_VERSIONS = Range.closed(4, 6);

  private DatabricksHiveMetastoreDdlTasks() {}

  /**
   * Returns whether the DDL of a table may have clauses that the other files lack, so that it needs
   * {@code SHOW CREATE TABLE}. Those are the generated and identity columns of Delta tables, which
   * {@code DESCRIBE TABLE EXTENDED ... AS JSON} doesn't report. A Delta table may have them if its
   * protocol lists the {@code generatedColumns} or {@code identityColumns} table feature, or if its
   * writer version is from 4 to 6 or unknown. A Delta table whose partition columns couldn't be
   * read with {@code DESCRIBE DETAIL} also needs it, as does a table that couldn't be described.
   *
   * <p>A table feature stays listed after the columns that needed it are dropped, so a table may
   * get {@code SHOW CREATE TABLE} without needing it.
   */
  static boolean needsShowCreateTable(@Nonnull ParsedTable table) {
    if (!table.described) {
      return true;
    }

    if (!table.isDelta()) {
      return false;
    }

    if (!table.partitioningKnown) {
      return true;
    }

    String minWriterVersion = null;
    for (Map.Entry<String, String> property : table.tableProperties.entrySet()) {
      String key = property.getKey();
      if (DDL_ONLY_FEATURE_PROPERTIES.contains(key.toLowerCase(Locale.ROOT))) {
        return true;
      }

      if (MIN_WRITER_VERSION_PROPERTY.equalsIgnoreCase(key)) {
        minWriterVersion = property.getValue();
      }
    }

    Integer writerVersion =
        minWriterVersion == null ? null : Ints.tryParse(minWriterVersion.trim());
    return writerVersion == null
        || LEGACY_GENERATED_COLUMNS_WRITER_VERSIONS.contains(writerVersion);
  }

  /**
   * Runs {@code SHOW CREATE TABLE} for each table and view that {@link #needsShowCreateTable}
   * selects, or for each of them in {@link ShowCreateTableMode#ALL} mode.
   *
   * <p>Databricks rejects {@code SHOW CREATE TABLE}, even {@code AS SERDE}, for Hive tables with an
   * unsupported SerDe configuration, such as some {@code RCFILE} and {@code SEQUENCEFILE} tables.
   * Those tables are skipped with a warning; the tables file describes their storage.
   */
  static final class DatabricksHiveMetastoreDdlsTask extends AbstractDatabricksHiveMetastoreTask {

    /** The error class of {@code SHOW CREATE TABLE} for a table with an unsupported Hive SerDe. */
    private static final String UNSUPPORTED_SERDE_ERROR_CLASS =
        "UNSUPPORTED_SHOW_CREATE_TABLE.WITH_UNSUPPORTED_SERDE_CONFIGURATION";

    private final ShowCreateTableMode mode;

    DatabricksHiveMetastoreDdlsTask(
        @Nonnull HiveMetastoreSessionCache sessionCache, @Nonnull ShowCreateTableMode mode) {
      super(DatabricksMetadataDumpFormat.HIVE_METASTORE_DDLS_ENTRY_NAME, sessionCache);
      this.mode = mode;
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
      ImmutableList<ParsedTable> tables = selectTables(sessionCache.listParsedTables(dbxHandle));
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
                        HIVE_METASTORE_CATALOG,
                        table.schemaName,
                        table.tableName,
                        table.tableType,
                        ddl);
                  }
                });
      }
      return null;
    }

    /** Returns the tables that get {@code SHOW CREATE TABLE} in the mode of this task. */
    @Nonnull
    private ImmutableList<ParsedTable> selectTables(@Nonnull ImmutableList<ParsedTable> tables) {
      if (mode == ShowCreateTableMode.ALL) {
        return tables;
      }

      ImmutableList.Builder<ParsedTable> builder = ImmutableList.builder();
      for (ParsedTable table : tables) {
        if (needsShowCreateTable(table)) {
          builder.add(table);
        }
      }
      ImmutableList<ParsedTable> selectedTables = builder.build();
      logger.info(
          "Running SHOW CREATE TABLE for {} of {} hive_metastore tables and views, whose DDL may have"
              + " clauses that the other files lack. -D{}={} runs it for every table and view.",
          selectedTables.size(),
          tables.size(),
          DatabricksProperty.SHOW_CREATE_TABLE.getName(),
          ShowCreateTableMode.ALL);
      return selectedTables;
    }

    /** Returns the DDL of a table without the line break that Databricks appends to it. */
    @Nonnull
    private static String showCreateTable(
        @Nonnull DatabricksHandle handle, @Nonnull ParsedTable table) throws IOException {
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
        @Nonnull ParsedTable table, @Nonnull IOException failure) {
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
