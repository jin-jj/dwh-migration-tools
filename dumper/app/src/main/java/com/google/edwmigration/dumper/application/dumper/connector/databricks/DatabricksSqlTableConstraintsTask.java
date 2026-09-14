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
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.TableConstraintsFormat;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dumps table constraints from the {@code information_schema} of each catalog. */
class DatabricksSqlTableConstraintsTask extends AbstractDatabricksSqlTask
    implements TableConstraintsFormat {

  private static final Logger logger =
      LoggerFactory.getLogger(DatabricksSqlTableConstraintsTask.class);

  /**
   * One row per constrained column, ordered so that the columns of a constraint arrive together.
   *
   * <p>Every join is a left join: a constraint without key columns (a CHECK) and a constraint
   * without a referenced table (anything that is not a foreign key) must still reach the output.
   */
  private static final String SQL =
      "SELECT tc.table_catalog, tc.table_schema, tc.table_name, tc.constraint_name, "
          + "tc.constraint_type, kcu.column_name, "
          + "coalesce(pk_tc.table_name, rc.unique_constraint_name) AS parent_table, "
          + "pk_kcu.column_name AS parent_column "
          + "FROM "
          + CATALOG
          + ".information_schema.table_constraints tc "
          + "LEFT JOIN "
          + CATALOG
          + ".information_schema.key_column_usage kcu "
          + "ON tc.constraint_catalog = kcu.constraint_catalog "
          + "AND tc.constraint_schema = kcu.constraint_schema "
          + "AND tc.constraint_name = kcu.constraint_name "
          + "LEFT JOIN "
          + CATALOG
          + ".information_schema.referential_constraints rc "
          + "ON tc.constraint_catalog = rc.constraint_catalog "
          + "AND tc.constraint_schema = rc.constraint_schema "
          + "AND tc.constraint_name = rc.constraint_name "
          + "LEFT JOIN "
          + CATALOG
          + ".information_schema.table_constraints pk_tc "
          + "ON rc.unique_constraint_catalog = pk_tc.constraint_catalog "
          + "AND rc.unique_constraint_schema = pk_tc.constraint_schema "
          + "AND rc.unique_constraint_name = pk_tc.constraint_name "
          + "LEFT JOIN "
          + CATALOG
          + ".information_schema.key_column_usage pk_kcu "
          + "ON rc.unique_constraint_catalog = pk_kcu.constraint_catalog "
          + "AND rc.unique_constraint_schema = pk_kcu.constraint_schema "
          + "AND rc.unique_constraint_name = pk_kcu.constraint_name "
          + "AND kcu.position_in_unique_constraint = pk_kcu.ordinal_position "
          + "ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, kcu.ordinal_position";

  /** Names the constraints without their columns, for catalogs where the joins are rejected. */
  private static final String COMPATIBILITY_SQL =
      "SELECT table_catalog, table_schema, table_name, constraint_name, constraint_type "
          + "FROM "
          + CATALOG
          + ".information_schema.table_constraints "
          + "ORDER BY table_schema, table_name, constraint_name";

  DatabricksSqlTableConstraintsTask(
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
      DatabricksConstraintWriter constraints = new DatabricksConstraintWriter(printer, monitor);
      executePerCatalog(
          databricksHandle,
          SQL,
          COMPATIBILITY_SQL,
          row -> {
            String schema = cell(row, 1);
            if (schema == null || !schemaPredicate.test(schema)) {
              return;
            }
            constraints.accept(row);
          });
      constraints.finish();
    }
    return null;
  }
}
