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
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dumps table constraints from Databricks Unity Catalog via SQL Warehouse queries. */
class DatabricksSqlTableConstraintsTask extends AbstractDatabricksSqlTask
    implements TableConstraintsFormat {

  private static final Logger logger =
      LoggerFactory.getLogger(DatabricksSqlTableConstraintsTask.class);

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
      List<String> catalogs = fetchMatchingCatalogs(databricksHandle);
      for (String catalogName : catalogs) {
        String escapedCatalog = DatabricksSqlHelper.escapeIdentifier(catalogName);
        AtomicBoolean anySuccess = new AtomicBoolean(false);

        // 1. Primary Keys
        String pkSql =
            "SELECT tc.table_catalog, tc.table_schema, tc.table_name, tc.constraint_name, kcu.column_name "
                + "FROM "
                + escapedCatalog
                + ".information_schema.table_constraints tc "
                + "JOIN "
                + escapedCatalog
                + ".information_schema.key_column_usage kcu "
                + "ON tc.constraint_catalog = kcu.constraint_catalog "
                + "AND tc.constraint_schema = kcu.constraint_schema "
                + "AND tc.constraint_name = kcu.constraint_name "
                + "WHERE tc.constraint_type = 'PRIMARY KEY' "
                + "ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, kcu.ordinal_position";
        try {
          Map<String, ConstraintAccumulator> pkMap = new LinkedHashMap<>();
          DatabricksSqlHelper.executeQuery(
              databricksHandle,
              pkSql,
              row -> {
                anySuccess.set(true);
                if (row.size() >= 5) {
                  String schema = row.get(1);
                  if (schema != null && schemaPredicate.test(schema)) {
                    String key =
                        row.get(0) + "." + row.get(1) + "." + row.get(2) + "." + row.get(3);
                    ConstraintAccumulator acc =
                        pkMap.computeIfAbsent(
                            key,
                            k ->
                                new ConstraintAccumulator(
                                    row.get(0), row.get(1), row.get(2), row.get(3), "PRIMARY KEY"));
                    if (row.get(4) != null) {
                      acc.childColumns.add(row.get(4));
                    }
                  }
                }
              });
          for (ConstraintAccumulator acc : pkMap.values()) {
            monitor.count();
            printer.printRecord(
                acc.catalog,
                acc.schema,
                acc.table,
                acc.constraintName,
                acc.constraintType,
                StringUtils.join(acc.childColumns, ", "));
          }
        } catch (Exception e) {
          logger.debug(
              "Failed to query primary keys for catalog '{}': {}", catalogName, e.getMessage());
        }

        // 2. Foreign Keys
        String fkSql =
            "SELECT tc.table_catalog, tc.table_schema, tc.table_name, tc.constraint_name, "
                + "kcu.column_name AS child_col, "
                + "coalesce(pk_tc.table_name, rc.unique_constraint_name) AS parent_table, "
                + "pk_kcu.column_name AS parent_col "
                + "FROM "
                + escapedCatalog
                + ".information_schema.table_constraints tc "
                + "JOIN "
                + escapedCatalog
                + ".information_schema.referential_constraints rc "
                + "ON tc.constraint_catalog = rc.constraint_catalog "
                + "AND tc.constraint_schema = rc.constraint_schema "
                + "AND tc.constraint_name = rc.constraint_name "
                + "JOIN "
                + escapedCatalog
                + ".information_schema.key_column_usage kcu "
                + "ON tc.constraint_catalog = kcu.constraint_catalog "
                + "AND tc.constraint_schema = kcu.constraint_schema "
                + "AND tc.constraint_name = kcu.constraint_name "
                + "LEFT JOIN "
                + escapedCatalog
                + ".information_schema.table_constraints pk_tc "
                + "ON rc.unique_constraint_catalog = pk_tc.constraint_catalog "
                + "AND rc.unique_constraint_schema = pk_tc.constraint_schema "
                + "AND rc.unique_constraint_name = pk_tc.constraint_name "
                + "LEFT JOIN "
                + escapedCatalog
                + ".information_schema.key_column_usage pk_kcu "
                + "ON rc.unique_constraint_catalog = pk_kcu.constraint_catalog "
                + "AND rc.unique_constraint_schema = pk_kcu.constraint_schema "
                + "AND rc.unique_constraint_name = pk_kcu.constraint_name "
                + "AND kcu.position_in_unique_constraint = pk_kcu.ordinal_position "
                + "WHERE tc.constraint_type = 'FOREIGN KEY' "
                + "ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, kcu.ordinal_position";
        try {
          Map<String, ConstraintAccumulator> fkMap = new LinkedHashMap<>();
          DatabricksSqlHelper.executeQuery(
              databricksHandle,
              fkSql,
              row -> {
                anySuccess.set(true);
                if (row.size() >= 4) {
                  String schema = row.get(1);
                  if (schema != null && schemaPredicate.test(schema)) {
                    String key =
                        row.get(0) + "." + row.get(1) + "." + row.get(2) + "." + row.get(3);
                    ConstraintAccumulator acc =
                        fkMap.computeIfAbsent(
                            key,
                            k ->
                                new ConstraintAccumulator(
                                    row.get(0), row.get(1), row.get(2), row.get(3), "FOREIGN KEY"));
                    if (row.size() > 4 && row.get(4) != null) {
                      acc.childColumns.add(row.get(4));
                    }
                    if (row.size() > 5 && row.get(5) != null) {
                      acc.parentTable = row.get(5);
                    }
                    if (row.size() > 6 && row.get(6) != null) {
                      acc.parentColumns.add(row.get(6));
                    }
                  }
                }
              });
          for (ConstraintAccumulator acc : fkMap.values()) {
            monitor.count();
            String details =
                StringUtils.join(acc.childColumns, ", ")
                    + " -> "
                    + (acc.parentTable != null ? acc.parentTable : "")
                    + "("
                    + StringUtils.join(acc.parentColumns, ", ")
                    + ")";
            printer.printRecord(
                acc.catalog,
                acc.schema,
                acc.table,
                acc.constraintName,
                acc.constraintType,
                details);
          }
        } catch (Exception e) {
          logger.debug(
              "Failed to query foreign keys for catalog '{}': {}", catalogName, e.getMessage());
        }

        // 3. Other Constraints (UNIQUE, etc.)
        String otherSql =
            "SELECT tc.table_catalog, tc.table_schema, tc.table_name, tc.constraint_name, tc.constraint_type, kcu.column_name "
                + "FROM "
                + escapedCatalog
                + ".information_schema.table_constraints tc "
                + "LEFT JOIN "
                + escapedCatalog
                + ".information_schema.key_column_usage kcu "
                + "ON tc.constraint_catalog = kcu.constraint_catalog "
                + "AND tc.constraint_schema = kcu.constraint_schema "
                + "AND tc.constraint_name = kcu.constraint_name "
                + "WHERE tc.constraint_type NOT IN ('PRIMARY KEY', 'FOREIGN KEY') "
                + "ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, kcu.ordinal_position";
        try {
          Map<String, ConstraintAccumulator> otherMap = new LinkedHashMap<>();
          DatabricksSqlHelper.executeQuery(
              databricksHandle,
              otherSql,
              row -> {
                anySuccess.set(true);
                if (row.size() >= 5) {
                  String schema = row.get(1);
                  if (schema != null && schemaPredicate.test(schema)) {
                    String key =
                        row.get(0) + "." + row.get(1) + "." + row.get(2) + "." + row.get(3);
                    ConstraintAccumulator acc =
                        otherMap.computeIfAbsent(
                            key,
                            k ->
                                new ConstraintAccumulator(
                                    row.get(0), row.get(1), row.get(2), row.get(3), row.get(4)));
                    if (row.size() > 5 && row.get(5) != null) {
                      acc.childColumns.add(row.get(5));
                    }
                  }
                }
              });
          for (ConstraintAccumulator acc : otherMap.values()) {
            monitor.count();
            printer.printRecord(
                acc.catalog,
                acc.schema,
                acc.table,
                acc.constraintName,
                acc.constraintType,
                StringUtils.join(acc.childColumns, ", "));
          }
        } catch (Exception e) {
          logger.debug(
              "Failed to query other constraints for catalog '{}': {}",
              catalogName,
              e.getMessage());
        }

        // 4. Fallback if joins failed
        if (!anySuccess.get()) {
          String fallbackSql =
              "SELECT table_catalog, table_schema, table_name, constraint_name, constraint_type "
                  + "FROM "
                  + escapedCatalog
                  + ".information_schema.table_constraints "
                  + "ORDER BY table_schema, table_name, constraint_name";
          try {
            DatabricksSqlHelper.executeQuery(
                databricksHandle,
                fallbackSql,
                row -> {
                  if (row.size() >= 5) {
                    String schema = row.get(1);
                    if (schema != null && schemaPredicate.test(schema)) {
                      monitor.count();
                      try {
                        printer.printRecord(
                            row.get(0), row.get(1), row.get(2), row.get(3), row.get(4), null);
                      } catch (IOException e) {
                        throw new RuntimeException("Failed to write constraint record", e);
                      }
                    }
                  }
                });
          } catch (Exception e) {
            logger.warn(
                "Failed to query table constraints for catalog '{}': {}",
                catalogName,
                e.getMessage());
          }
        }
      }
    }
    return null;
  }

  private static class ConstraintAccumulator {
    final String catalog;
    final String schema;
    final String table;
    final String constraintName;
    final String constraintType;
    final List<String> childColumns = new ArrayList<>();
    final List<String> parentColumns = new ArrayList<>();
    String parentTable;

    ConstraintAccumulator(
        String catalog, String schema, String table, String constraintName, String constraintType) {
      this.catalog = catalog;
      this.schema = schema;
      this.table = table;
      this.constraintName = constraintName;
      this.constraintType = constraintType;
    }
  }
}
