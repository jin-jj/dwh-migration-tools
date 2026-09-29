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
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.cell;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.cellOrEmpty;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.quoteIdentifier;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDumpFormat.CatalogsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDumpFormat.ColumnsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDumpFormat.SchemataHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDumpFormat.TableConstraintsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDumpFormat.TablesHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDumpFormat.ViewsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.ParsedColumn;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.ParsedTableConstraint;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.ParsedTableExtended;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.function.IOSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extraction tasks for the legacy {@code hive_metastore} catalog using {@code SHOW SCHEMAS}, {@code
 * DESCRIBE SCHEMA EXTENDED} and {@code SHOW TABLE EXTENDED}.
 */
final class DatabricksHiveMetastoreTasks {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksHiveMetastoreTasks.class);

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

    /**
     * Runs {@code SHOW TABLE EXTENDED} for each schema, one after another. The tables of a schema
     * whose statement fails are logged and skipped, so one broken schema doesn't discard the tables
     * of the others. If the statement fails for every schema, the cause is likely systemic, such as
     * lost access to the warehouse, so the load fails with the first failure as its cause.
     */
    @Nonnull
    private ImmutableList<ParsedTableExtended> loadTables(@Nonnull DatabricksHandle handle)
        throws IOException {
      ImmutableList<String> schemas = listSchemas(handle);
      ImmutableList.Builder<ParsedTableExtended> builder = ImmutableList.builder();
      IOException firstFailure = null;
      int failureCount = 0;
      for (String schema : schemas) {
        if (Thread.currentThread().isInterrupted()) {
          throw new InterruptedIOException(
              "Interrupted before SHOW TABLE EXTENDED for " + quoteIdentifier(schema) + ".");
        }

        IOException failure;
        try {
          builder.addAll(showTableExtended(handle, schema));
          continue;
        } catch (IOException e) {
          failure = e;
        } catch (UncheckedIOException e) {
          failure = e.getCause();
        }
        if (Thread.currentThread().isInterrupted()) {
          InterruptedIOException interrupted =
              new InterruptedIOException(
                  "Interrupted during SHOW TABLE EXTENDED for " + quoteIdentifier(schema) + ".");
          interrupted.initCause(failure);
          throw interrupted;
        }

        failureCount++;
        if (firstFailure == null) {
          firstFailure = failure;
        }
        logger.warn(
            "Skipping SHOW TABLE EXTENDED for {}: {}",
            quoteIdentifier(schema),
            failure.getMessage());
      }

      if (firstFailure != null && failureCount == schemas.size()) {
        throw new IOException(
            String.format(
                "SHOW TABLE EXTENDED failed for all %d schemas. First failure: %s",
                failureCount, firstFailure.getMessage()),
            firstFailure);
      }

      if (firstFailure != null) {
        logger.warn(
            "SHOW TABLE EXTENDED failed for {} of {} schemas; their tables are missing from the"
                + " output.",
            failureCount,
            schemas.size());
      }
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
        for (String schema : schemas) {
          SchemaDescription description = describeSchemaOrEmpty(dbxHandle, schema);
          csv.printRecord(
              HIVE_METASTORE_CATALOG,
              schema,
              description.comment,
              description.owner,
              "",
              "",
              description.location);
        }
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

  /** Writes the columns of each table and view in {@code hive_metastore} to {@code columns.csv}. */
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
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(ColumnsHeader.class).print(writer)) {
        for (ParsedTableExtended table : sessionCache.listParsedTables(dbxHandle)) {
          for (ParsedColumn column : table.columns) {
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
        }
      }
      return null;
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
}
