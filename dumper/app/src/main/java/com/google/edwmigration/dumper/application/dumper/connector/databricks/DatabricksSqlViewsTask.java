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
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.ViewsFormat;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dumps view definitions from the {@code information_schema} of each catalog. */
class DatabricksSqlViewsTask extends AbstractDatabricksSqlTask implements ViewsFormat {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksSqlViewsTask.class);

  private static final String SQL =
      "SELECT table_catalog, table_schema, table_name, view_definition "
          + "FROM "
          + CATALOG
          + ".information_schema.views"
          + WHERE
          + " ORDER BY table_schema, table_name";

  DatabricksSqlViewsTask(@Nonnull DatabricksFilter filter) {
    super(ZIP_ENTRY_NAME, filter);
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    DatabricksHandle databricksHandle = (DatabricksHandle) handle;
    logger.info("Writing views to '{}'", getTargetPath());
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
        CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer);
        RecordProgressMonitor monitor =
            new RecordProgressMonitor("Writing views to " + getTargetPath())) {
      executePerCatalog(
          databricksHandle,
          withFilter(SQL, /* catalogColumn= */ null, "table_schema"),
          /* compatibilitySqlTemplate= */ null,
          row -> {
            String schemaName = cell(row, 1);
            if (schemaName == null || !filter.matchesSchema(schemaName)) {
              return;
            }
            monitor.count();
            printer.printRecord(cell(row, 0), schemaName, cell(row, 2), cell(row, 3));
          });
    }
    return null;
  }
}
