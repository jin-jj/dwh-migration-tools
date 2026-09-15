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

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksCatalogNames.HIVE_METASTORE;

import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTable.Column;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.RecordProgressMonitor;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.ColumnsFormat;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dumps column definitions from the legacy {@code hive_metastore} catalog.
 *
 * <p>Columns are read from the schema tree that {@code SHOW TABLE EXTENDED} prints for each table,
 * which costs one query per schema. {@code DESCRIBE TABLE} would additionally give column comments,
 * but at one query per table, which is not affordable on a metastore of any size. Column comments
 * are therefore left empty here.
 */
class DatabricksHiveMetastoreColumnsTask extends AbstractDatabricksHiveMetastoreTask
    implements ColumnsFormat {

  private static final Logger logger =
      LoggerFactory.getLogger(DatabricksHiveMetastoreColumnsTask.class);

  DatabricksHiveMetastoreColumnsTask(@Nonnull DatabricksFilter filter) {
    super(HMS_ZIP_ENTRY_NAME, filter);
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    DatabricksHandle databricksHandle = (DatabricksHandle) handle;
    if (!databricksHandle.hasWarehouseId()) {
      logger.info("Skipping '{}' because no SQL warehouse was configured", getTargetPath());
      return null;
    }
    logger.info("Writing hive_metastore columns to '{}'", getTargetPath());
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
        CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer);
        RecordProgressMonitor monitor =
            new RecordProgressMonitor("Writing hive_metastore columns to " + getTargetPath())) {
      forEachTable(
          databricksHandle,
          (schemaName, table) -> {
            int ordinal = 1;
            for (Column column : table.columns()) {
              monitor.count();
              printer.printRecord(
                  HIVE_METASTORE,
                  schemaName,
                  table.name(),
                  ordinal++,
                  column.name(),
                  column.dataType(),
                  column.nullable(),
                  /* comment= */ null,
                  table.partitionIndexOf(column.name()));
            }
          });
    }
    return null;
  }
}
