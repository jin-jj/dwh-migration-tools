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
import com.databricks.sdk.service.catalog.PrimaryKeyConstraint;
import com.databricks.sdk.service.catalog.TableConstraint;
import com.databricks.sdk.service.catalog.TableInfo;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.RecordProgressMonitor;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.TableConstraintsFormat;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dumps table constraints (primary and foreign keys) from Databricks Unity Catalog. */
class DatabricksTableConstraintsTask extends AbstractDatabricksTask
    implements TableConstraintsFormat {

  private static final Logger logger =
      LoggerFactory.getLogger(DatabricksTableConstraintsTask.class);

  DatabricksTableConstraintsTask(
      @Nonnull Predicate<String> catalogPredicate, @Nonnull Predicate<String> schemaPredicate) {
    super(ZIP_ENTRY_NAME, catalogPredicate, schemaPredicate);
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    DatabricksHandle databricksHandle = (DatabricksHandle) handle;
    logger.info("Writing table constraints to '{}'", getTargetPath());
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
        CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer);
        RecordProgressMonitor monitor =
            new RecordProgressMonitor("Writing table constraints to " + getTargetPath())) {
      List<String> catalogs = fetchMatchingCatalogs(databricksHandle);
      for (String catalogName : catalogs) {
        List<String> schemas = fetchMatchingSchemas(databricksHandle, catalogName);
        for (String schemaName : schemas) {
          try {
            for (TableInfo summary :
                databricksHandle.getClient().tables().list(catalogName, schemaName)) {
              TableInfo tableInfo = summary;
              try {
                String fullName = summary.getFullName();
                if (fullName == null && summary.getName() != null) {
                  fullName = catalogName + "." + schemaName + "." + summary.getName();
                }
                if (fullName != null) {
                  TableInfo detailed = databricksHandle.getClient().tables().get(fullName);
                  if (detailed != null) {
                    tableInfo = detailed;
                  }
                }
              } catch (Exception e) {
                logger.debug(
                    "Failed to get detailed table info for '{}.{}.{}': {}",
                    catalogName,
                    schemaName,
                    summary.getName(),
                    e.getMessage());
              }
              Collection<TableConstraint> constraints = tableInfo.getTableConstraints();
              if (constraints != null) {
                for (TableConstraint constraint : constraints) {
                  if (constraint.getPrimaryKeyConstraint() != null) {
                    PrimaryKeyConstraint pk = constraint.getPrimaryKeyConstraint();
                    monitor.count();
                    String details =
                        pk.getChildColumns() != null
                            ? StringUtils.join(pk.getChildColumns(), ", ")
                            : "";
                    printer.printRecord(
                        tableInfo.getCatalogName(),
                        tableInfo.getSchemaName(),
                        tableInfo.getName(),
                        pk.getName(),
                        "PRIMARY KEY",
                        details);
                  }
                  if (constraint.getForeignKeyConstraint() != null) {
                    ForeignKeyConstraint fk = constraint.getForeignKeyConstraint();
                    monitor.count();
                    String childCols =
                        fk.getChildColumns() != null
                            ? StringUtils.join(fk.getChildColumns(), ", ")
                            : "";
                    String parentCols =
                        fk.getParentColumns() != null
                            ? StringUtils.join(fk.getParentColumns(), ", ")
                            : "";
                    String details =
                        childCols + " -> " + fk.getParentTable() + "(" + parentCols + ")";
                    printer.printRecord(
                        tableInfo.getCatalogName(),
                        tableInfo.getSchemaName(),
                        tableInfo.getName(),
                        fk.getName(),
                        "FOREIGN KEY",
                        details);
                  }
                }
              }
            }
          } catch (Exception e) {
            logger.warn(
                "Failed to list table constraints for schema '{}.{}': {}",
                catalogName,
                schemaName,
                e.getMessage());
          }
        }
      }
    }
    return null;
  }
}
