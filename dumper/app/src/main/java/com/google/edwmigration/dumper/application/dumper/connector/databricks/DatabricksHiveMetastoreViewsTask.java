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

import com.google.common.base.Preconditions;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.RecordProgressMonitor;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.ViewsFormat;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dumps view definitions from Databricks legacy hive_metastore via DBSQL. */
class DatabricksHiveMetastoreViewsTask extends AbstractTask<Void> implements ViewsFormat {

  private static final Logger logger =
      LoggerFactory.getLogger(DatabricksHiveMetastoreViewsTask.class);

  private final Predicate<String> schemaPredicate;

  DatabricksHiveMetastoreViewsTask(@Nonnull Predicate<String> schemaPredicate) {
    super(HMS_ZIP_ENTRY_NAME);
    this.schemaPredicate =
        Preconditions.checkNotNull(schemaPredicate, "Schema predicate was null.");
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    DatabricksHandle databricksHandle = (DatabricksHandle) handle;
    if (!databricksHandle.hasWarehouseId()) {
      logger.info("Skipping '{}' because no SQL warehouse was configured", getTargetPath());
      return null;
    }
    logger.info("Writing hive_metastore views to '{}'", getTargetPath());
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
        CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer);
        RecordProgressMonitor monitor =
            new RecordProgressMonitor("Writing hive_metastore views to " + getTargetPath())) {
      List<List<String>> schemaRows =
          DatabricksSqlHelper.executeQuery(databricksHandle, "SHOW SCHEMAS IN hive_metastore");
      for (List<String> schemaRow : schemaRows) {
        if (schemaRow.isEmpty()) {
          continue;
        }
        String schemaName = schemaRow.get(0);
        if (schemaName == null || !schemaPredicate.test(schemaName)) {
          continue;
        }
        List<List<String>> viewRows =
            DatabricksSqlHelper.executeQuery(
                databricksHandle,
                "SHOW VIEWS IN hive_metastore." + DatabricksSqlHelper.escapeIdentifier(schemaName));
        for (List<String> viewRow : viewRows) {
          if (viewRow.size() < 2) {
            continue;
          }
          String viewName = viewRow.get(1);
          List<List<String>> createTableRows =
              DatabricksSqlHelper.executeQuery(
                  databricksHandle,
                  "SHOW CREATE TABLE hive_metastore."
                      + DatabricksSqlHelper.escapeIdentifier(schemaName)
                      + "."
                      + DatabricksSqlHelper.escapeIdentifier(viewName));
          StringBuilder ddlBuilder = new StringBuilder();
          for (List<String> ddlRow : createTableRows) {
            if (!ddlRow.isEmpty() && ddlRow.get(0) != null) {
              ddlBuilder.append(ddlRow.get(0)).append("\n");
            }
          }
          String ddl = ddlBuilder.toString().trim();
          if (!ddl.isEmpty()) {
            monitor.count();
            printer.printRecord("hive_metastore", schemaName, viewName, ddl);
          }
        }
      }
    }
    return null;
  }
}
