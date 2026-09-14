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

import com.databricks.sdk.service.catalog.ColumnInfo;
import com.databricks.sdk.service.catalog.ColumnTypeName;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.RecordProgressMonitor;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.ColumnsFormat;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dumps column definitions from the Unity Catalog REST API.
 *
 * <p>{@code /tables/list} embeds the full column array in each table entry, so this costs the same
 * number of requests as the tables task rather than one request per table.
 */
class DatabricksRestColumnsTask extends AbstractDatabricksRestTask implements ColumnsFormat {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksRestColumnsTask.class);

  DatabricksRestColumnsTask(
      @Nonnull Predicate<String> catalogPredicate, @Nonnull Predicate<String> schemaPredicate) {
    super(ZIP_ENTRY_NAME, catalogPredicate, schemaPredicate);
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    DatabricksHandle databricksHandle = (DatabricksHandle) handle;
    logger.info("Writing columns from the REST API to '{}'", getTargetPath());
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
        CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer);
        RecordProgressMonitor monitor =
            new RecordProgressMonitor("Writing columns to " + getTargetPath())) {
      forEachTableInMetastore(
          databricksHandle,
          table -> {
            if (table.getColumns() == null) {
              return;
            }
            for (ColumnInfo column : table.getColumns()) {
              monitor.count();
              printer.printRecord(
                  table.getCatalogName(),
                  table.getSchemaName(),
                  table.getName(),
                  ordinalOf(column),
                  column.getName(),
                  typeOf(column),
                  column.getNullable(),
                  column.getComment(),
                  column.getPartitionIndex());
            }
          });
    }
    return null;
  }

  /**
   * Returns the 1-based ordinal, matching {@code information_schema.columns.ordinal_position}. The
   * REST API reports a 0-based {@code position} instead.
   */
  private static Long ordinalOf(@Nonnull ColumnInfo column) {
    Long position = column.getPosition();
    return position == null ? null : position + 1;
  }

  /** Prefers the rendered type text, which keeps the parameters of nested and decimal types. */
  private static String typeOf(@Nonnull ColumnInfo column) {
    String typeText = column.getTypeText();
    if (typeText != null) {
      return typeText;
    }
    ColumnTypeName typeName = column.getTypeName();
    return typeName == null ? null : typeName.name();
  }
}
