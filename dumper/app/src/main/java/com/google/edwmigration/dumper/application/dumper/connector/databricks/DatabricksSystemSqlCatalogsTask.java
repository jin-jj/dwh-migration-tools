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
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dumps catalog definitions from Databricks Unity Catalog system tables. */
class DatabricksSystemSqlCatalogsTask extends AbstractDatabricksSystemSqlTask
    implements CatalogsFormat {

  private static final Logger logger =
      LoggerFactory.getLogger(DatabricksSystemSqlCatalogsTask.class);

  DatabricksSystemSqlCatalogsTask(@Nonnull Predicate<String> catalogPredicate) {
    super(ZIP_ENTRY_NAME, catalogPredicate);
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    DatabricksHandle databricksHandle = (DatabricksHandle) handle;
    logger.info("Writing catalogs from system tables to '{}'", getTargetPath());
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
        CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer);
        RecordProgressMonitor monitor =
            new RecordProgressMonitor(
                "Writing catalogs from system tables to " + getTargetPath())) {
      String sql =
          "SELECT catalog_name, comment, owner, "
              + "unix_millis(created) AS created, unix_millis(last_altered) AS last_altered "
              + "FROM system.information_schema.catalogs ORDER BY catalog_name";
      AtomicBoolean success = new AtomicBoolean(false);
      try {
        DatabricksSqlHelper.executeQueryOrThrow(
            databricksHandle,
            sql,
            row -> {
              success.set(true);
              if (!row.isEmpty()) {
                String catalogName = row.get(0);
                if (catalogName != null
                    && catalogPredicate.test(catalogName)
                    && !databricksHandle.isCatalogInaccessible(catalogName)) {
                  monitor.count();
                  try {
                    printer.printRecord(
                        catalogName,
                        row.size() > 1 ? row.get(1) : null,
                        row.size() > 2 ? row.get(2) : null,
                        row.size() > 3 ? row.get(3) : null,
                        row.size() > 4 ? row.get(4) : null);
                  } catch (Exception e) {
                    throw new RuntimeException("Failed to write catalog record", e);
                  }
                }
              }
            });
      } catch (SQLException e) {
        String fallbackSql =
            "SELECT catalog_name, comment, owner, created, last_altered "
                + "FROM system.information_schema.catalogs ORDER BY catalog_name";
        try {
          DatabricksSqlHelper.executeQueryOrThrow(
              databricksHandle,
              fallbackSql,
              row -> {
                success.set(true);
                if (!row.isEmpty()) {
                  String catalogName = row.get(0);
                  if (catalogName != null
                      && catalogPredicate.test(catalogName)
                      && !databricksHandle.isCatalogInaccessible(catalogName)) {
                    monitor.count();
                    try {
                      printer.printRecord(
                          catalogName,
                          row.size() > 1 ? row.get(1) : null,
                          row.size() > 2 ? row.get(2) : null,
                          row.size() > 3 ? row.get(3) : null,
                          row.size() > 4 ? row.get(4) : null);
                    } catch (Exception ex) {
                      throw new RuntimeException("Failed to write catalog record", ex);
                    }
                  }
                }
              });
        } catch (SQLException ex) {
          throw e;
        }
      }
    }
    return null;
  }
}
