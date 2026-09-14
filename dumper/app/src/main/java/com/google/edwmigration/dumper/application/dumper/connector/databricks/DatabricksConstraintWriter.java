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

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.AbstractDatabricksSqlTask.cell;
import static org.apache.commons.lang3.StringUtils.join;

import com.google.common.base.Preconditions;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.RecordProgressMonitor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;

/**
 * Collapses the rows of one table constraint into a single record.
 *
 * <p>A constraint spans one row per key column, so the constraint queries order their rows by
 * constraint and this writer emits a record as soon as a row for a different constraint arrives.
 * Nothing but the constraint currently being read is held in memory.
 *
 * <p>Expected row shape: catalog, schema, table, constraint name, constraint type, key column,
 * referenced table, referenced column. The last three may be absent.
 */
final class DatabricksConstraintWriter {

  private static final String FOREIGN_KEY = "FOREIGN KEY";

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

  DatabricksConstraintWriter(@Nonnull CSVPrinter printer, @Nonnull RecordProgressMonitor monitor) {
    Preconditions.checkNotNull(printer, "Printer was null.");
    Preconditions.checkNotNull(monitor, "Monitor was null.");
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

  /** Emits the constraint being read, if any. Must be called once the last row has been seen. */
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
