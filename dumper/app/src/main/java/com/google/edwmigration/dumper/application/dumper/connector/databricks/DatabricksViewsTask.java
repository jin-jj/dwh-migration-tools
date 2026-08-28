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

import com.databricks.sdk.service.catalog.TableInfo;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
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

/** Dumps view definitions from Databricks Unity Catalog. */
class DatabricksViewsTask extends AbstractDatabricksTask implements ViewsFormat {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksViewsTask.class);

  DatabricksViewsTask(
      @Nonnull Predicate<String> catalogPredicate, @Nonnull Predicate<String> schemaPredicate) {
    super(ZIP_ENTRY_NAME, catalogPredicate, schemaPredicate);
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
      List<String> catalogs = getMatchingCatalogs(databricksHandle);
      for (String catalogName : catalogs) {
        List<String> schemas = getMatchingSchemas(databricksHandle, catalogName);
        for (String schemaName : schemas) {
          try {
            for (TableInfo tableInfo :
                databricksHandle.getClient().tables().list(catalogName, schemaName)) {
              if (tableInfo.getViewDefinition() != null) {
                monitor.count();
                printer.printRecord(
                    tableInfo.getCatalogName(),
                    tableInfo.getSchemaName(),
                    tableInfo.getName(),
                    tableInfo.getViewDefinition());
              }
            }
          } catch (Exception e) {
            logger.warn(
                "Failed to list views for schema '{}.{}': {}",
                catalogName,
                schemaName,
                e.getMessage());
          }
        }
      }
    }
    return null;
  }
}
