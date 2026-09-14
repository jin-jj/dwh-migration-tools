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

import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.RecordProgressMonitor;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.TablesFormat;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dumps table definitions from Databricks Unity Catalog system tables. */
class DatabricksSystemSqlTablesTask extends AbstractDatabricksSystemSqlTask
    implements TablesFormat {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksSystemSqlTablesTask.class);

  private static final String SQL =
      "SELECT table_catalog, table_schema, table_name, table_type, "
          + "data_source_format, storage_path, comment, table_owner, "
          + "unix_millis(created) AS created, unix_millis(last_altered) AS last_altered "
          + "FROM system.information_schema.tables "
          + "ORDER BY table_catalog, table_schema, table_name";

  private static final String COMPATIBILITY_SQL =
      "SELECT table_catalog, table_schema, table_name, table_type, "
          + "data_source_format, storage_path, comment, table_owner, created, last_altered "
          + "FROM system.information_schema.tables "
          + "ORDER BY table_catalog, table_schema, table_name";

  DatabricksSystemSqlTablesTask(
      @Nonnull Predicate<String> catalogPredicate, @Nonnull Predicate<String> schemaPredicate) {
    super(ZIP_ENTRY_NAME, catalogPredicate, schemaPredicate);
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    DatabricksHandle databricksHandle = (DatabricksHandle) handle;
    logger.info("Writing tables from system tables to '{}'", getTargetPath());
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
        CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer);
        RecordProgressMonitor monitor =
            new RecordProgressMonitor("Writing tables from system tables to " + getTargetPath())) {
      executeWithCompatibilityFallback(
          databricksHandle,
          SQL,
          COMPATIBILITY_SQL,
          row -> {
            String catalogName = cell(row, 0);
            String schemaName = cell(row, 1);
            if (catalogName == null
                || schemaName == null
                || !catalogPredicate.test(catalogName)
                || !schemaPredicate.test(schemaName)
                || databricksHandle.isCatalogInaccessible(catalogName)) {
              return;
            }
            monitor.count();
            printer.printRecord(
                catalogName,
                schemaName,
                cell(row, 2),
                cell(row, 3),
                cell(row, 4),
                cell(row, 5),
                cell(row, 6),
                cell(row, 7),
                cell(row, 8),
                cell(row, 9));
          });
    }
    return null;
  }
}
