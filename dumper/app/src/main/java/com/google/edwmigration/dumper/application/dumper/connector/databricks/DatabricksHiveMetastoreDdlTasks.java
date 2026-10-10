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
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreFunctionDdlFormatter.formatCreateFunction;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreFunctionParser.ParsedFunction;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreFunctionTasks.HiveMetastoreFunctionCache;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.AbstractDatabricksHiveMetastoreTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.HiveMetastoreSessionCache;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.TableDdl;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.DdlsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.FunctionDdlsHeader;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.Writer;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;

/**
 * Extraction tasks for the DDL statements of the legacy {@code hive_metastore} catalog: the {@code
 * SHOW CREATE TABLE} output of the tables and views, and the {@code CREATE FUNCTION} statements of
 * the functions.
 */
final class DatabricksHiveMetastoreDdlTasks {

  private DatabricksHiveMetastoreDdlTasks() {}

  /**
   * Writes the {@code SHOW CREATE TABLE} output of the tables and views that {@link
   * HiveMetastoreSessionCache#listDdls} lists.
   */
  static final class DatabricksHiveMetastoreDdlsTask extends AbstractDatabricksHiveMetastoreTask {

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
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(DdlsHeader.class).print(writer)) {
        for (TableDdl tableDdl : sessionCache.listDdls(dbxHandle)) {
          csv.printRecord(
              HIVE_METASTORE_CATALOG,
              tableDdl.table.schemaName,
              tableDdl.table.tableName,
              tableDdl.table.tableType,
              tableDdl.ddl);
        }
      }
      return null;
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
