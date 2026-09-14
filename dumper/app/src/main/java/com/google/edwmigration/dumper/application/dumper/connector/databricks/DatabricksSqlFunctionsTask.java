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
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.FunctionsFormat;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dumps function and UDF definitions from Databricks Unity Catalog via SQL Warehouse queries. */
class DatabricksSqlFunctionsTask extends AbstractDatabricksSqlTask implements FunctionsFormat {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksSqlFunctionsTask.class);

  DatabricksSqlFunctionsTask(
      @Nonnull Predicate<String> catalogPredicate, @Nonnull Predicate<String> schemaPredicate) {
    super(ZIP_ENTRY_NAME, catalogPredicate, schemaPredicate);
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    DatabricksHandle databricksHandle = (DatabricksHandle) handle;
    logger.info("Writing functions to '{}'", getTargetPath());
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
        CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer);
        RecordProgressMonitor monitor =
            new RecordProgressMonitor("Writing functions to " + getTargetPath())) {
      List<String> catalogs = fetchMatchingCatalogs(databricksHandle);
      for (String catalogName : catalogs) {
        String escapedCatalog = DatabricksSqlHelper.escapeIdentifier(catalogName);

        // Fetch routine parameters
        Map<String, List<String>> paramsByFunction = new HashMap<>();
        String paramsSql =
            "SELECT specific_schema, specific_name, parameter_name, "
                + "coalesce(full_data_type, data_type) AS data_type "
                + "FROM "
                + escapedCatalog
                + ".information_schema.parameters "
                + "ORDER BY specific_schema, specific_name, ordinal_position";
        try {
          DatabricksSqlHelper.executeQuery(
              databricksHandle,
              paramsSql,
              row -> {
                if (row.size() >= 4) {
                  String schema = row.get(0);
                  String name = row.get(1);
                  String paramName = row.get(2);
                  String dataType = row.get(3);
                  String key = schema + "." + name;
                  String formattedParam =
                      paramName != null && dataType != null
                          ? paramName + " " + dataType
                          : (paramName != null ? paramName : dataType);
                  if (formattedParam != null) {
                    paramsByFunction
                        .computeIfAbsent(key, k -> new ArrayList<>())
                        .add(formattedParam);
                  }
                }
              });
        } catch (Exception e) {
          logger.debug(
              "Failed to query parameters for catalog '{}': {}", catalogName, e.getMessage());
        }

        // Fetch routines
        String sql =
            "SELECT routine_catalog, routine_schema, routine_name, "
                + "coalesce(full_data_type, data_type) AS data_type, "
                + "routine_definition, external_language, comment, created_by "
                + "FROM "
                + escapedCatalog
                + ".information_schema.routines "
                + "ORDER BY routine_schema, routine_name";
        AtomicBoolean success = new AtomicBoolean(false);
        try {
          DatabricksSqlHelper.executeQuery(
              databricksHandle,
              sql,
              row -> {
                success.set(true);
                if (row.size() >= 3) {
                  String schemaName = row.get(1);
                  if (schemaName != null && schemaPredicate.test(schemaName)) {
                    monitor.count();
                    String routineName = row.get(2);
                    String key = schemaName + "." + routineName;
                    List<String> params = paramsByFunction.get(key);
                    String inputParams =
                        params != null && !params.isEmpty() ? StringUtils.join(params, ", ") : null;
                    try {
                      printer.printRecord(
                          row.get(0),
                          row.get(1),
                          row.get(2),
                          row.size() > 3 ? row.get(3) : null,
                          inputParams,
                          row.size() > 4 ? row.get(4) : null,
                          row.size() > 5 ? row.get(5) : null,
                          row.size() > 6 ? row.get(6) : null,
                          row.size() > 7 ? row.get(7) : null);
                    } catch (IOException e) {
                      throw new RuntimeException("Failed to write function record", e);
                    }
                  }
                }
              });
        } catch (Exception e) {
          logger.warn(
              "Failed to query information_schema.routines for catalog '{}': {}",
              catalogName,
              e.getMessage());
        }

        if (!success.get()) {
          String fallbackSql =
              "SELECT routine_catalog, routine_schema, routine_name, "
                  + "coalesce(full_data_type, data_type) AS data_type, "
                  + "routine_definition, external_language "
                  + "FROM "
                  + escapedCatalog
                  + ".information_schema.routines "
                  + "ORDER BY routine_schema, routine_name";
          try {
            DatabricksSqlHelper.executeQuery(
                databricksHandle,
                fallbackSql,
                row -> {
                  if (row.size() >= 3) {
                    String schemaName = row.get(1);
                    if (schemaName != null && schemaPredicate.test(schemaName)) {
                      monitor.count();
                      String routineName = row.get(2);
                      String key = schemaName + "." + routineName;
                      List<String> params = paramsByFunction.get(key);
                      String inputParams =
                          params != null && !params.isEmpty()
                              ? StringUtils.join(params, ", ")
                              : null;
                      try {
                        printer.printRecord(
                            row.get(0),
                            row.get(1),
                            row.get(2),
                            row.size() > 3 ? row.get(3) : null,
                            inputParams,
                            row.size() > 4 ? row.get(4) : null,
                            row.size() > 5 ? row.get(5) : null,
                            null,
                            null);
                      } catch (IOException e) {
                        throw new RuntimeException("Failed to write function record", e);
                      }
                    }
                  }
                });
          } catch (Exception e) {
            logger.warn(
                "Failed fallback query on information_schema.routines for catalog '{}': {}",
                catalogName,
                e.getMessage());
          }
        }
      }
    }
    return null;
  }
}
