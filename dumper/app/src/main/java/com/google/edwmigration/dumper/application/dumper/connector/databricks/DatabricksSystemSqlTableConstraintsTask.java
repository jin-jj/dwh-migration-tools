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

import static org.apache.commons.lang3.StringUtils.join;

import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.RecordProgressMonitor;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.TableConstraintsFormat;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dumps table constraints from Databricks Unity Catalog system tables. */
class DatabricksSystemSqlTableConstraintsTask extends AbstractDatabricksSystemSqlTask
    implements TableConstraintsFormat {

  private static final Logger logger =
      LoggerFactory.getLogger(DatabricksSystemSqlTableConstraintsTask.class);

  private static final String FOREIGN_KEY = "FOREIGN KEY";

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
          + "FROM system.information_schema.table_constraints tc "
          + "LEFT JOIN system.information_schema.key_column_usage kcu "
          + "ON tc.constraint_catalog = kcu.constraint_catalog "
          + "AND tc.constraint_schema = kcu.constraint_schema "
          + "AND tc.constraint_name = kcu.constraint_name "
          + "LEFT JOIN system.information_schema.referential_constraints rc "
          + "ON tc.constraint_catalog = rc.constraint_catalog "
          + "AND tc.constraint_schema = rc.constraint_schema "
          + "AND tc.constraint_name = rc.constraint_name "
          + "LEFT JOIN system.information_schema.table_constraints pk_tc "
          + "ON rc.unique_constraint_catalog = pk_tc.constraint_catalog "
          + "AND rc.unique_constraint_schema = pk_tc.constraint_schema "
          + "AND rc.unique_constraint_name = pk_tc.constraint_name "
          + "LEFT JOIN system.information_schema.key_column_usage pk_kcu "
          + "ON rc.unique_constraint_catalog = pk_kcu.constraint_catalog "
          + "AND rc.unique_constraint_schema = pk_kcu.constraint_schema "
          + "AND rc.unique_constraint_name = pk_kcu.constraint_name "
          + "AND kcu.position_in_unique_constraint = pk_kcu.ordinal_position "
          + "ORDER BY tc.table_catalog, tc.table_schema, tc.table_name, tc.constraint_name, "
          + "kcu.ordinal_position";

  /** Names the constraints without their columns, for metastores where the joins are rejected. */
  private static final String COMPATIBILITY_SQL =
      "SELECT table_catalog, table_schema, table_name, constraint_name, constraint_type "
          + "FROM system.information_schema.table_constraints "
          + "ORDER BY table_catalog, table_schema, table_name, constraint_name";

  DatabricksSystemSqlTableConstraintsTask(
      @Nonnull Predicate<String> catalogPredicate, @Nonnull Predicate<String> schemaPredicate) {
    super(ZIP_ENTRY_NAME, catalogPredicate, schemaPredicate);
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    DatabricksHandle databricksHandle = (DatabricksHandle) handle;
    logger.info("Writing table constraints from system tables to '{}'", getTargetPath());
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
        CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer);
        RecordProgressMonitor monitor =
            new RecordProgressMonitor(
                "Writing table constraints from system tables to " + getTargetPath())) {
      ConstraintWriter constraints = new ConstraintWriter(printer, monitor);
      executeWithCompatibilityFallback(
          databricksHandle,
          SQL,
          COMPATIBILITY_SQL,
          row -> {
            String catalog = cell(row, 0);
            String schema = cell(row, 1);
            if (catalog == null
                || schema == null
                || !catalogPredicate.test(catalog)
                || !schemaPredicate.test(schema)
                || databricksHandle.isCatalogInaccessible(catalog)) {
              return;
            }
            constraints.accept(row);
          });
      constraints.finish();
    }
    return null;
  }

  /**
   * Collapses the rows of one constraint into a single record.
   *
   * <p>The query orders its rows by constraint, so a record can be emitted as soon as a row for a
   * different constraint arrives; nothing but the constraint being read is held in memory.
   */
  private static final class ConstraintWriter {

    private final CSVPrinter printer;
    private final RecordProgressMonitor monitor;
    private final List<String> columns = new ArrayList<>();
    private final List<String> parentColumns = new ArrayList<>();
    private String catalog;
    private String schema;
    private String table;
    private String name;
    private String type;
    private String parentTable;
    private boolean open;

    ConstraintWriter(CSVPrinter printer, RecordProgressMonitor monitor) {
      this.printer = printer;
      this.monitor = monitor;
    }

    void accept(@Nonnull List<String> row) throws IOException {
      String rowCatalog = cell(row, 0);
      String rowSchema = cell(row, 1);
      String rowTable = cell(row, 2);
      String rowName = cell(row, 3);
      if (!open || !isSameConstraint(rowCatalog, rowSchema, rowTable, rowName)) {
        finish();
        this.catalog = rowCatalog;
        this.schema = rowSchema;
        this.table = rowTable;
        this.name = rowName;
        this.type = cell(row, 4);
        this.parentTable = null;
        this.open = true;
      }
      addColumn(columns, cell(row, 5));
      if (parentTable == null) {
        this.parentTable = cell(row, 6);
      }
      addColumn(parentColumns, cell(row, 7));
    }

    void finish() throws IOException {
      if (!open) {
        return;
      }
      monitor.count();
      printer.printRecord(catalog, schema, table, name, type, describe());
      columns.clear();
      parentColumns.clear();
      this.open = false;
    }

    private boolean isSameConstraint(
        @CheckForNull String rowCatalog,
        @CheckForNull String rowSchema,
        @CheckForNull String rowTable,
        @CheckForNull String rowName) {
      return equal(catalog, rowCatalog)
          && equal(schema, rowSchema)
          && equal(table, rowTable)
          && equal(name, rowName);
    }

    /** Renders a foreign key as {@code child -> parent(column)}, anything else as its columns. */
    private String describe() {
      String childColumns = join(columns, ", ");
      if (!FOREIGN_KEY.equals(type) || (parentTable == null && parentColumns.isEmpty())) {
        return childColumns;
      }
      return childColumns
          + " -> "
          + (parentTable == null ? "" : parentTable)
          + "("
          + join(parentColumns, ", ")
          + ")";
    }

    private static void addColumn(List<String> target, @CheckForNull String column) {
      if (column != null && !target.contains(column)) {
        target.add(column);
      }
    }

    private static boolean equal(@CheckForNull String left, @CheckForNull String right) {
      return left == null ? right == null : left.equals(right);
    }
  }
}
