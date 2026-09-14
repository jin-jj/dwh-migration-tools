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
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.escapeIdentifier;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.executeQueryOrThrow;

import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.RecordProgressMonitor;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.SchemataFormat;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dumps schema definitions from the legacy {@code hive_metastore} catalog. */
class DatabricksHiveMetastoreSchemataTask extends AbstractDatabricksHiveMetastoreTask
    implements SchemataFormat {

  private static final Logger logger =
      LoggerFactory.getLogger(DatabricksHiveMetastoreSchemataTask.class);

  DatabricksHiveMetastoreSchemataTask(@Nonnull Predicate<String> schemaPredicate) {
    super(HMS_ZIP_ENTRY_NAME, schemaPredicate);
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
      for (String schemaName : fetchMatchingSchemaNames(databricksHandle)) {
        Map<String, String> description = describeSchema(databricksHandle, schemaName);
        monitor.count();
        printer.printRecord(
            HIVE_METASTORE,
            schemaName,
            description.get("comment"),
            description.get("owner"),
            // The legacy metastore records neither creation nor modification time for schemas.
            /* createdAt= */ null,
            /* updatedAt= */ null);
      }
    }
    return null;
  }

  /**
   * Returns the lower-cased property names and values of {@code DESCRIBE SCHEMA EXTENDED}.
   *
   * <p>There are only ever a handful of schemas, so the query per schema is affordable here. A
   * schema that cannot be described still gets a row, just without its comment and owner.
   */
  private static Map<String, String> describeSchema(
      @Nonnull DatabricksHandle handle, @Nonnull String schemaName) {
    Map<String, String> description = new LinkedHashMap<>();
    List<List<String>> rows;
    try {
      rows =
          executeQueryOrThrow(
              handle,
              "DESCRIBE SCHEMA EXTENDED " + HIVE_METASTORE + "." + escapeIdentifier(schemaName));
    } catch (SQLException e) {
      logger.warn("Failed to describe hive_metastore.{}: {}", schemaName, e.getMessage());
      return description;
    }
    for (List<String> row : rows) {
      if (row.size() >= 2 && row.get(0) != null) {
        description.put(row.get(0).trim().toLowerCase(Locale.ROOT), row.get(1));
      }
    }
    return description;
  }
}
