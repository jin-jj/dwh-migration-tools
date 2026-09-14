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
import java.util.function.Predicate;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dumps column definitions from the {@code information_schema} of each catalog. */
class DatabricksSqlColumnsTask extends AbstractDatabricksSqlTask implements ColumnsFormat {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksSqlColumnsTask.class);

  /**
   * {@code full_data_type} carries the complete type, including the element types of arrays, maps
   * and structs, where {@code data_type} carries only the outermost type name.
   */
  private static final String SQL =
      "SELECT table_catalog, table_schema, table_name, ordinal_position, column_name, "
          + "coalesce(full_data_type, data_type) AS data_type, is_nullable, comment, "
          + "partition_index "
          + "FROM "
          + CATALOG
          + ".information_schema.columns ORDER BY table_schema, table_name, ordinal_position";

  private static final String COMPATIBILITY_SQL =
      "SELECT table_catalog, table_schema, table_name, ordinal_position, column_name, "
          + "data_type, is_nullable, comment, partition_index "
          + "FROM "
          + CATALOG
          + ".information_schema.columns ORDER BY table_schema, table_name, ordinal_position";

  DatabricksSqlColumnsTask(
      @Nonnull Predicate<String> catalogPredicate, @Nonnull Predicate<String> schemaPredicate) {
    super(ZIP_ENTRY_NAME, catalogPredicate, schemaPredicate);
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    DatabricksHandle databricksHandle = (DatabricksHandle) handle;
    logger.info("Writing columns to '{}'", getTargetPath());
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
        CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer);
        RecordProgressMonitor monitor =
            new RecordProgressMonitor("Writing columns to " + getTargetPath())) {
      executePerCatalog(
          databricksHandle,
          SQL,
          COMPATIBILITY_SQL,
          row -> {
            String schemaName = cell(row, 1);
            if (schemaName == null || !schemaPredicate.test(schemaName)) {
              return;
            }
            monitor.count();
            printer.printRecord(
                cell(row, 0),
                schemaName,
                cell(row, 2),
                cell(row, 3),
                cell(row, 4),
                cell(row, 5),
                toBoolean(cell(row, 6)),
                cell(row, 7),
                cell(row, 8));
          });
    }
    return null;
  }

  /** Maps the {@code YES}/{@code NO} of the information schema onto a boolean literal. */
  @CheckForNull
  private static String toBoolean(@CheckForNull String value) {
    if (value == null) {
      return null;
    }
    if ("YES".equalsIgnoreCase(value)) {
      return "true";
    }
    if ("NO".equalsIgnoreCase(value)) {
      return "false";
    }
    return value;
  }
}
