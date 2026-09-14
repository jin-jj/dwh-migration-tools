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
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.CatalogsFormat;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dumps catalog definitions from Databricks using SQL Warehouse queries. */
class DatabricksSqlCatalogsTask extends AbstractDatabricksSqlTask implements CatalogsFormat {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksSqlCatalogsTask.class);

  DatabricksSqlCatalogsTask(@Nonnull Predicate<String> catalogPredicate) {
    super(ZIP_ENTRY_NAME, catalogPredicate);
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    DatabricksHandle databricksHandle = (DatabricksHandle) handle;
    logger.info("Writing catalogs to '{}'", getTargetPath());
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
        CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer);
        RecordProgressMonitor monitor =
            new RecordProgressMonitor("Writing catalogs to " + getTargetPath())) {
      List<List<String>> catalogRows =
          DatabricksSqlHelper.executeQuery(databricksHandle, "SHOW CATALOGS");
      for (List<String> row : catalogRows) {
        if (row.isEmpty()) {
          continue;
        }
        String catalogName = row.get(0);
        if (catalogName == null || !catalogPredicate.test(catalogName)) {
          continue;
        }
        monitor.count();
        String comment = null;
        String owner = null;
        try {
          List<List<String>> descRows =
              DatabricksSqlHelper.executeQuery(
                  databricksHandle,
                  "DESCRIBE CATALOG EXTENDED " + DatabricksSqlHelper.escapeIdentifier(catalogName));
          for (List<String> descRow : descRows) {
            if (descRow.size() >= 2) {
              String key = descRow.get(0) != null ? descRow.get(0).trim().toLowerCase() : "";
              String value = descRow.get(1);
              if ("comment".equals(key)) {
                comment = value;
              } else if ("owner".equals(key)) {
                owner = value;
              }
            }
          }
        } catch (Exception e) {
          logger.debug("Failed to describe catalog '{}': {}", catalogName, e.getMessage());
        }
        printer.printRecord(
            catalogName, comment, owner, /* CreatedAt= */ null, /* UpdatedAt= */ null);
      }
    }
    return null;
  }
}
