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
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreParser.ParsedColumn;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreParser.ParsedFunctionExtended;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreParser.ParsedTableConstraint;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreParser.ParsedTableExtended;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.CatalogsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.ColumnsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.DdlsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.FunctionsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.SchemataHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.TableConstraintsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.TablesHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.ViewsHeader;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extraction tasks for the legacy {@code hive_metastore} catalog using request-scoped {@code SHOW
 * TABLE EXTENDED}, {@code SHOW CREATE TABLE}, and {@code DESCRIBE FUNCTION EXTENDED}.
 */
final class DatabricksHiveMetastoreTasks {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksHiveMetastoreTasks.class);

  private DatabricksHiveMetastoreTasks() {}

  /** Shared per-connector cache of {@code hive_metastore} schemas and parsed table descriptors. */
  static final class HiveMetastoreSessionCache {

    private final Object lock = new Object();
    private ImmutableList<String> cachedSchemas;
    private ImmutableList<ParsedTableExtended> cachedTables;
    private ImmutableList<ParsedFunctionExtended> cachedFunctions;
    private final Map<String, String> cachedDdls = new LinkedHashMap<>();

    @Nonnull
    ImmutableList<String> listSchemas(@Nonnull DatabricksHandle handle) throws IOException {
      synchronized (lock) {
        if (cachedSchemas != null) {
          return cachedSchemas;
        }
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
        cachedSchemas = builder.build();
        return cachedSchemas;
      }
    }

    @Nonnull
    ImmutableList<ParsedTableExtended> listParsedTables(@Nonnull DatabricksHandle handle)
        throws IOException {
      synchronized (lock) {
        if (cachedTables != null) {
          return cachedTables;
        }
        ImmutableList<String> schemas = listSchemas(handle);
        ImmutableList.Builder<ParsedTableExtended> builder = ImmutableList.builder();
        for (String schema : schemas) {
          String quotedSchema = quoteIdentifier(schema);
          String query = String.format("SHOW TABLE EXTENDED IN %s LIKE '*'", quotedSchema);
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
              builder.add(
                  DatabricksHiveMetastoreParser.parseShowTableExtended(
                      rowSchema.isEmpty() ? schema : rowSchema, rowTable, infoBlob));
            }
          } catch (DatabricksSqlException e) {
            if (e.isPermissionDenied()) {
              logger.warn("Skipping hive_metastore schema '{}' due to missing privileges.", schema);
              continue;
            }
            throw e;
          }
        }
        cachedTables = builder.build();
        return cachedTables;
      }
    }

    @Nonnull
    ImmutableList<ParsedFunctionExtended> listParsedFunctions(@Nonnull DatabricksHandle handle)
        throws IOException {
      synchronized (lock) {
        if (cachedFunctions != null) {
          return cachedFunctions;
        }
        ImmutableList<String> schemas = listSchemas(handle);
        ImmutableList.Builder<ParsedFunctionExtended> builder = ImmutableList.builder();
        for (String schema : schemas) {
          String quotedSchema = quoteIdentifier(schema);
          List<String> functionNames = new ArrayList<>();
          try (CloseableIterable<List<String>> rows =
              handle
                  .sqlHelper()
                  .executeSqlInCatalog(
                      HIVE_METASTORE_CATALOG, "SHOW USER FUNCTIONS IN " + quotedSchema)) {
            for (List<String> row : rows) {
              String rawName = cell(row, 0);
              if (rawName != null && !rawName.trim().isEmpty()) {
                functionNames.add(extractShortFunctionName(rawName.trim()));
              }
            }
          } catch (DatabricksSqlException e) {
            if (e.isPermissionDenied()) {
              continue;
            }
            throw e;
          }

          for (String funcName : functionNames) {
            String quotedFunc = quoteIdentifier(funcName);
            List<String> descLines = new ArrayList<>();
            try (CloseableIterable<List<String>> descRows =
                handle
                    .sqlHelper()
                    .executeSqlInCatalog(
                        HIVE_METASTORE_CATALOG,
                        "DESCRIBE FUNCTION EXTENDED " + quotedSchema + "." + quotedFunc)) {
              for (List<String> descRow : descRows) {
                String line = cell(descRow, 0);
                if (line != null) {
                  descLines.add(line);
                }
              }
            } catch (DatabricksSqlException e) {
              logger.warn(
                  "Skipping DESCRIBE FUNCTION EXTENDED for '{}.{}': {}",
                  schema,
                  funcName,
                  e.getMessage());
              continue;
            }
            builder.add(
                DatabricksHiveMetastoreParser.parseDescribeFunctionExtended(
                    HIVE_METASTORE_CATALOG, schema, funcName, descLines));
          }
        }
        cachedFunctions = builder.build();
        return cachedFunctions;
      }
    }

    private static String extractShortFunctionName(String fullName) {
      int lastDot = fullName.lastIndexOf('.');
      return lastDot >= 0 ? fullName.substring(lastDot + 1) : fullName;
    }

    @CheckForNull
    String fetchTableDdl(
        @Nonnull DatabricksHandle handle, @Nonnull String schema, @Nonnull String table)
        throws IOException {
      String key = schema.toLowerCase(Locale.ROOT) + "." + table.toLowerCase(Locale.ROOT);
      synchronized (lock) {
        if (cachedDdls.containsKey(key)) {
          return cachedDdls.get(key);
        }
      }
      String qualifiedTable = quoteIdentifier(schema) + "." + quoteIdentifier(table);
      String ddl = executeShowCreateTable(handle, "SHOW CREATE TABLE " + qualifiedTable);
      if (ddl == null) {
        ddl = executeShowCreateTable(handle, "SHOW CREATE TABLE " + qualifiedTable + " AS SERDE");
      }
      synchronized (lock) {
        cachedDdls.put(key, ddl);
      }
      return ddl;
    }

    @CheckForNull
    private static String executeShowCreateTable(
        @Nonnull DatabricksHandle handle, @Nonnull String sql) throws IOException {
      StringBuilder sb = new StringBuilder();
      try (CloseableIterable<List<String>> rows =
          handle.sqlHelper().executeSqlInCatalog(HIVE_METASTORE_CATALOG, sql)) {
        for (List<String> row : rows) {
          String part = cell(row, 0);
          if (part != null) {
            if (sb.length() > 0) {
              sb.append("\n");
            }
            sb.append(part);
          }
        }
      } catch (DatabricksSqlException e) {
        String msg = e.getMessage() != null ? e.getMessage() : "";
        if (msg.contains("AS SERDE") || msg.contains("SHOW_CREATE_TABLE")) {
          return null;
        }
        logger.warn("Skipping '{}' due to SQL error: {}", sql, msg);
        return "";
      }
      return sb.toString();
    }

    private static String quoteIdentifier(String ident) {
      return "`" + ident.replace("`", "``") + "`";
    }
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
      super(DatabricksMetadataDumpFormat.CATALOGS_ENTRY_NAME, sessionCache);
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
      super(DatabricksMetadataDumpFormat.SCHEMATA_ENTRY_NAME, sessionCache);
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
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(SchemataHeader.class).print(writer)) {
        for (String schema : sessionCache.listSchemas(dbxHandle)) {
          String comment = "";
          String owner = "";
          String quotedSchema = "`" + schema.replace("`", "``") + "`";
          try (CloseableIterable<List<String>> descRows =
              dbxHandle
                  .sqlHelper()
                  .executeSqlInCatalog(
                      HIVE_METASTORE_CATALOG, "DESCRIBE SCHEMA EXTENDED " + quotedSchema)) {
            for (List<String> row : descRows) {
              String key = cellOrEmpty(row, 0);
              String val = cellOrEmpty(row, 1);
              if ("Comment".equalsIgnoreCase(key)) {
                comment = val;
              } else if ("Owner".equalsIgnoreCase(key)) {
                owner = val;
              }
            }
          } catch (DatabricksSqlException ignored) {
            // Keep schema row even if DESCRIBE SCHEMA EXTENDED fails
          }
          csv.printRecord(HIVE_METASTORE_CATALOG, schema, comment, owner, "", "");
        }
      }
      return null;
    }
  }

  static final class DatabricksHiveMetastoreTablesTask extends AbstractDatabricksHiveMetastoreTask {

    DatabricksHiveMetastoreTablesTask(@Nonnull HiveMetastoreSessionCache sessionCache) {
      super(DatabricksMetadataDumpFormat.TABLES_ENTRY_NAME, sessionCache);
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
              tbl.updatedAt);
        }
      }
      return null;
    }
  }

  static final class DatabricksHiveMetastoreColumnsTask
      extends AbstractDatabricksHiveMetastoreTask {

    DatabricksHiveMetastoreColumnsTask(@Nonnull HiveMetastoreSessionCache sessionCache) {
      super(DatabricksMetadataDumpFormat.COLUMNS_ENTRY_NAME, sessionCache);
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
        for (ParsedTableExtended tbl : sessionCache.listParsedTables(dbxHandle)) {
          for (ParsedColumn col : tbl.columns) {
            csv.printRecord(
                tbl.catalogName,
                tbl.schemaName,
                tbl.tableName,
                String.valueOf(col.ordinalPosition),
                col.columnName,
                col.dataType,
                col.isNullable,
                col.comment,
                col.partitionIndex);
          }
        }
      }
      return null;
    }
  }

  static final class DatabricksHiveMetastoreViewsTask extends AbstractDatabricksHiveMetastoreTask {

    DatabricksHiveMetastoreViewsTask(@Nonnull HiveMetastoreSessionCache sessionCache) {
      super(DatabricksMetadataDumpFormat.VIEWS_ENTRY_NAME, sessionCache);
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
      super(DatabricksMetadataDumpFormat.TABLE_CONSTRAINTS_ENTRY_NAME, sessionCache);
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
      super(DatabricksMetadataDumpFormat.FUNCTIONS_ENTRY_NAME, sessionCache);
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
   * Complementary task that executes {@code SHOW CREATE TABLE} for each table and view in {@code
   * hive_metastore}, plus synthesizes {@code CREATE FUNCTION} DDLs from {@code DESCRIBE FUNCTION
   * EXTENDED}, and dumps all DDL statements into {@code ddls.csv}.
   */
  static final class DatabricksHiveMetastoreDdlsTask extends AbstractDatabricksHiveMetastoreTask {

    DatabricksHiveMetastoreDdlsTask(@Nonnull HiveMetastoreSessionCache sessionCache) {
      super(DatabricksMetadataDumpFormat.DDLS_ENTRY_NAME, sessionCache);
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
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(DdlsHeader.class).print(writer)) {
        for (ParsedTableExtended tbl : sessionCache.listParsedTables(dbxHandle)) {
          String ddl = sessionCache.fetchTableDdl(dbxHandle, tbl.schemaName, tbl.tableName);
          if (ddl != null && !ddl.isEmpty()) {
            csv.printRecord(tbl.catalogName, tbl.schemaName, tbl.tableName, tbl.tableType, ddl);
          }
        }
        for (ParsedFunctionExtended fn : sessionCache.listParsedFunctions(dbxHandle)) {
          if (fn.ddl != null && !fn.ddl.isEmpty()) {
            csv.printRecord(
                fn.functionCatalog, fn.functionSchema, fn.functionName, "FUNCTION", fn.ddl);
          }
        }
      }
      return null;
    }
  }
}
