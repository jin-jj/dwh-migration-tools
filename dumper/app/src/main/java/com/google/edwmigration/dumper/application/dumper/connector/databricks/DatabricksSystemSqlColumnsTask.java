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
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.ColumnsFormat;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dumps column definitions from Databricks Unity Catalog system tables. */
class DatabricksSystemSqlColumnsTask extends AbstractDatabricksSystemSqlTask
    implements ColumnsFormat {

  private static final Logger logger =
      LoggerFactory.getLogger(DatabricksSystemSqlColumnsTask.class);

  DatabricksSystemSqlColumnsTask(
      @Nonnull Predicate<String> catalogPredicate, @Nonnull Predicate<String> schemaPredicate) {
    super(ZIP_ENTRY_NAME, catalogPredicate, schemaPredicate);
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    DatabricksHandle databricksHandle = (DatabricksHandle) handle;
    logger.info("Writing columns from system tables to '{}'", getTargetPath());
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
        CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer);
        RecordProgressMonitor monitor =
            new RecordProgressMonitor("Writing columns from system tables to " + getTargetPath())) {
      String sql =
          "SELECT table_catalog, table_schema, table_name, ordinal_position, column_name, "
              + "coalesce(full_data_type, data_type) AS data_type, "
              + "case when is_nullable = 'YES' then 'true' else 'false' end AS is_nullable, "
              + "comment, partition_index "
              + "FROM system.information_schema.columns "
              + "ORDER BY table_catalog, table_schema, table_name, ordinal_position";
      AtomicBoolean success = new AtomicBoolean(false);
      try {
        DatabricksSqlHelper.executeQueryOrThrow(
            databricksHandle,
            sql,
            row -> {
              success.set(true);
              if (row.size() >= 5) {
                String catalogName = row.get(0);
                String schemaName = row.get(1);
                if (catalogName != null
                    && schemaName != null
                    && catalogPredicate.test(catalogName)
                    && schemaPredicate.test(schemaName)
                    && !databricksHandle.isCatalogInaccessible(catalogName)) {
                  monitor.count();
                  try {
                    printer.printRecord(
                        row.get(0),
                        row.get(1),
                        row.get(2),
                        row.get(3),
                        row.get(4),
                        row.size() > 5 ? row.get(5) : null,
                        row.size() > 6 ? row.get(6) : null,
                        row.size() > 7 ? row.get(7) : null,
                        row.size() > 8 ? row.get(8) : null);
                  } catch (Exception e) {
                    throw new RuntimeException("Failed to write column record", e);
                  }
                }
              }
            });
      } catch (SQLException e) {
        String fallbackSql =
            "SELECT table_catalog, table_schema, table_name, ordinal_position, column_name, "
                + "data_type, is_nullable, comment, partition_index "
                + "FROM system.information_schema.columns "
                + "ORDER BY table_catalog, table_schema, table_name, ordinal_position";
        try {
          DatabricksSqlHelper.executeQueryOrThrow(
              databricksHandle,
              fallbackSql,
              row -> {
                success.set(true);
                if (row.size() >= 5) {
                  String catalogName = row.get(0);
                  String schemaName = row.get(1);
                  if (catalogName != null
                      && schemaName != null
                      && catalogPredicate.test(catalogName)
                      && schemaPredicate.test(schemaName)
                      && !databricksHandle.isCatalogInaccessible(catalogName)) {
                    monitor.count();
                    String nullable = row.size() > 6 ? row.get(6) : null;
                    if (nullable != null) {
                      nullable =
                          "YES".equalsIgnoreCase(nullable)
                              ? "true"
                              : ("NO".equalsIgnoreCase(nullable) ? "false" : nullable);
                    }
                    try {
                      printer.printRecord(
                          row.get(0),
                          row.get(1),
                          row.get(2),
                          row.get(3),
                          row.get(4),
                          row.size() > 5 ? row.get(5) : null,
                          nullable,
                          row.size() > 7 ? row.get(7) : null,
                          row.size() > 8 ? row.get(8) : null);
                    } catch (Exception ex) {
                      throw new RuntimeException("Failed to write column record", ex);
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
