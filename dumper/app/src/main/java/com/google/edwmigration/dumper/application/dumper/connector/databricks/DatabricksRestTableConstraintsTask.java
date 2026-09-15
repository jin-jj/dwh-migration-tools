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

import com.databricks.sdk.service.catalog.ForeignKeyConstraint;
import com.databricks.sdk.service.catalog.GetTableRequest;
import com.databricks.sdk.service.catalog.PrimaryKeyConstraint;
import com.databricks.sdk.service.catalog.TableConstraint;
import com.databricks.sdk.service.catalog.TableInfo;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.RecordProgressMonitor;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.TableConstraintsFormat;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dumps primary and foreign key constraints from the Unity Catalog REST API.
 *
 * <p>This is the most expensive task in the REST tier by a wide margin. Databricks does not set
 * {@code table_constraints} on the entries returned by {@code /tables/list}, and the {@code
 * /unity-catalog/constraints} endpoints are write-only, so the constraints of a table can only be
 * read with a get on that one table. One request per table is therefore unavoidable here. The SQL
 * tiers read the same information with a single {@code information_schema} query, which is why they
 * are preferred.
 */
class DatabricksRestTableConstraintsTask extends AbstractDatabricksRestTask
    implements TableConstraintsFormat {

  private static final Logger logger =
      LoggerFactory.getLogger(DatabricksRestTableConstraintsTask.class);

  DatabricksRestTableConstraintsTask(@Nonnull DatabricksFilter filter) {
    super(ZIP_ENTRY_NAME, filter);
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    DatabricksHandle databricksHandle = (DatabricksHandle) handle;
    logger.info(
        "Writing table constraints from the REST API to '{}'. This issues one request per table and"
            + " may take a long time on a large metastore.",
        getTargetPath());
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
        CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer);
        RecordProgressMonitor monitor =
            new RecordProgressMonitor("Writing table constraints to " + getTargetPath())) {
      forEachTableInMetastore(
          databricksHandle,
          listed -> {
            String fullName = listed.getFullName();
            if (fullName == null) {
              return;
            }
            TableInfo table =
                DatabricksRestHelper.callWithRetry(
                    databricksHandle,
                    "reading constraints of table '" + fullName + "'",
                    () ->
                        databricksHandle
                            .getClient()
                            .tables()
                            .get(new GetTableRequest().setFullName(fullName)));
            if (table == null || table.getTableConstraints() == null) {
              return;
            }
            for (TableConstraint constraint : table.getTableConstraints()) {
              printConstraint(
                  printer,
                  monitor,
                  listed.getCatalogName(),
                  listed.getSchemaName(),
                  listed.getName(),
                  constraint);
            }
          });
    }
    return null;
  }

  private static void printConstraint(
      CSVPrinter printer,
      RecordProgressMonitor monitor,
      String catalogName,
      String schemaName,
      String tableName,
      TableConstraint constraint)
      throws IOException {
    PrimaryKeyConstraint primaryKey = constraint.getPrimaryKeyConstraint();
    if (primaryKey != null) {
      monitor.count();
      printer.printRecord(
          catalogName,
          schemaName,
          tableName,
          primaryKey.getName(),
          "PRIMARY KEY",
          join(primaryKey.getChildColumns()));
    }
    ForeignKeyConstraint foreignKey = constraint.getForeignKeyConstraint();
    if (foreignKey != null) {
      monitor.count();
      printer.printRecord(
          catalogName,
          schemaName,
          tableName,
          foreignKey.getName(),
          "FOREIGN KEY",
          join(foreignKey.getChildColumns())
              + " -> "
              + foreignKey.getParentTable()
              + "("
              + join(foreignKey.getParentColumns())
              + ")");
    }
  }

  private static String join(Iterable<String> columns) {
    return columns == null ? "" : StringUtils.join(columns, ", ");
  }
}
