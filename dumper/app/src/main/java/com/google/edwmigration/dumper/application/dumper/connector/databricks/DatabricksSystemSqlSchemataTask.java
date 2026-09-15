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
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.SchemataFormat;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dumps schema definitions from Databricks Unity Catalog system tables. */
class DatabricksSystemSqlSchemataTask extends AbstractDatabricksSystemSqlTask
    implements SchemataFormat {

  private static final Logger logger =
      LoggerFactory.getLogger(DatabricksSystemSqlSchemataTask.class);

  private static final String SQL =
      "SELECT catalog_name, schema_name, comment, schema_owner, "
          + "unix_millis(created) AS created, unix_millis(last_altered) AS last_altered "
          + "FROM system.information_schema.schemata"
          + WHERE
          + " ORDER BY catalog_name, schema_name";

  private static final String COMPATIBILITY_SQL =
      "SELECT catalog_name, schema_name, comment, schema_owner, created, last_altered "
          + "FROM system.information_schema.schemata"
          + WHERE
          + " ORDER BY catalog_name, schema_name";

  DatabricksSystemSqlSchemataTask(@Nonnull DatabricksFilter filter) {
    super(ZIP_ENTRY_NAME, filter);
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    DatabricksHandle databricksHandle = (DatabricksHandle) handle;
    logger.info("Writing schemata from system tables to '{}'", getTargetPath());
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
        CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer);
        RecordProgressMonitor monitor =
            new RecordProgressMonitor(
                "Writing schemata from system tables to " + getTargetPath())) {
      executeWithCompatibilityFallback(
          databricksHandle,
          withFilter(SQL, "catalog_name", "schema_name"),
          withFilter(COMPATIBILITY_SQL, "catalog_name", "schema_name"),
          row -> {
            String catalogName = cell(row, 0);
            String schemaName = cell(row, 1);
            if (catalogName == null
                || schemaName == null
                || !filter.matchesCatalog(catalogName)
                || !filter.matchesSchema(schemaName)
                || databricksHandle.isCatalogInaccessible(catalogName)) {
              return;
            }
            monitor.count();
            printer.printRecord(
                catalogName, schemaName, cell(row, 2), cell(row, 3), cell(row, 4), cell(row, 5));
          });
    }
    return null;
  }
}
