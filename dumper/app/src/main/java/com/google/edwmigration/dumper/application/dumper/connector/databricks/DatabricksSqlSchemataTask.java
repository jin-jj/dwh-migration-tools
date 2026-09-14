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
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dumps schema definitions from Databricks Unity Catalog via SQL Warehouse queries. */
class DatabricksSqlSchemataTask extends AbstractDatabricksSqlTask implements SchemataFormat {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksSqlSchemataTask.class);

  DatabricksSqlSchemataTask(
      @Nonnull Predicate<String> catalogPredicate, @Nonnull Predicate<String> schemaPredicate) {
    super(ZIP_ENTRY_NAME, catalogPredicate, schemaPredicate);
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    DatabricksHandle databricksHandle = (DatabricksHandle) handle;
    logger.info("Writing schemata to '{}'", getTargetPath());
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
        CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer);
        RecordProgressMonitor monitor =
            new RecordProgressMonitor("Writing schemata to " + getTargetPath())) {
      List<String> catalogs = fetchMatchingCatalogs(databricksHandle);
      for (String catalogName : catalogs) {
        String escapedCatalog = DatabricksSqlHelper.escapeIdentifier(catalogName);
        String sql =
            "SELECT catalog_name, schema_name, comment, schema_owner, "
                + "unix_millis(created) AS created, unix_millis(last_altered) AS last_altered "
                + "FROM "
                + escapedCatalog
                + ".information_schema.schemata ORDER BY schema_name";
        AtomicBoolean success = new AtomicBoolean(false);
        try {
          DatabricksSqlHelper.executeQuery(
              databricksHandle,
              sql,
              row -> {
                success.set(true);
                if (row.size() >= 2) {
                  String schemaName = row.get(1);
                  if (schemaName != null && schemaPredicate.test(schemaName)) {
                    monitor.count();
                    try {
                      printer.printRecord(
                          row.get(0),
                          row.get(1),
                          row.size() > 2 ? row.get(2) : null,
                          row.size() > 3 ? row.get(3) : null,
                          row.size() > 4 ? row.get(4) : null,
                          row.size() > 5 ? row.get(5) : null);
                    } catch (IOException e) {
                      throw new RuntimeException("Failed to write schema record", e);
                    }
                  }
                }
              });
        } catch (Exception e) {
          logger.warn(
              "Failed to query information_schema.schemata with unix_millis for catalog '{}': {}",
              catalogName,
              e.getMessage());
        }

        if (!success.get()) {
          String fallbackSql =
              "SELECT catalog_name, schema_name, comment, schema_owner, created, last_altered "
                  + "FROM "
                  + escapedCatalog
                  + ".information_schema.schemata ORDER BY schema_name";
          try {
            DatabricksSqlHelper.executeQuery(
                databricksHandle,
                fallbackSql,
                row -> {
                  if (row.size() >= 2) {
                    String schemaName = row.get(1);
                    if (schemaName != null && schemaPredicate.test(schemaName)) {
                      monitor.count();
                      try {
                        printer.printRecord(
                            row.get(0),
                            row.get(1),
                            row.size() > 2 ? row.get(2) : null,
                            row.size() > 3 ? row.get(3) : null,
                            row.size() > 4 ? row.get(4) : null,
                            row.size() > 5 ? row.get(5) : null);
                      } catch (IOException e) {
                        throw new RuntimeException("Failed to write schema record", e);
                      }
                    }
                  }
                });
          } catch (Exception e) {
            logger.warn(
                "Failed fallback query on information_schema.schemata for catalog '{}': {}",
                catalogName,
                e.getMessage());
          }
        }
      }
    }
    return null;
  }
}
