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
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.SchemataFormat;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dumps schema definitions from Databricks legacy hive_metastore via DBSQL. */
class DatabricksHiveMetastoreSchemataTask extends AbstractTask<Void> implements SchemataFormat {

  private static final Logger logger =
      LoggerFactory.getLogger(DatabricksHiveMetastoreSchemataTask.class);

  private final Predicate<String> schemaPredicate;

  DatabricksHiveMetastoreSchemataTask(@Nonnull Predicate<String> schemaPredicate) {
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
    logger.info("Writing hive_metastore schemas to '{}'", getTargetPath());
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
        CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer);
        RecordProgressMonitor monitor =
            new RecordProgressMonitor("Writing hive_metastore schemas to " + getTargetPath())) {
      DatabricksSqlHelper.executeQuery(
          databricksHandle,
          "SHOW SCHEMAS IN hive_metastore",
          row -> {
            if (!row.isEmpty()) {
              String schemaName = row.get(0);
              if (schemaName != null && schemaPredicate.test(schemaName)) {
                monitor.count();
                try {
                  printer.printRecord(
                      "hive_metastore",
                      schemaName,
                      /* comment= */ null,
                      /* owner= */ null,
                      /* createdAt= */ null,
                      /* updatedAt= */ null);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
            }
          });
    }
    return null;
  }
}
