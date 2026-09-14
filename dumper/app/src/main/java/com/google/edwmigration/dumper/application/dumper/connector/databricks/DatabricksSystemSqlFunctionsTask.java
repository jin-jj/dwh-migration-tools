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
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dumps function and UDF definitions from Databricks Unity Catalog system tables. */
class DatabricksSystemSqlFunctionsTask extends AbstractDatabricksSystemSqlTask
    implements FunctionsFormat {

  private static final Logger logger =
      LoggerFactory.getLogger(DatabricksSystemSqlFunctionsTask.class);

  private static final String PARAMETERS_SQL =
      "SELECT specific_catalog, specific_schema, specific_name, parameter_name, "
          + "coalesce(full_data_type, data_type) AS data_type "
          + "FROM system.information_schema.parameters "
          + "ORDER BY specific_catalog, specific_schema, specific_name, ordinal_position";

  private static final String SQL =
      "SELECT routine_catalog, routine_schema, routine_name, "
          + "coalesce(full_data_type, data_type) AS data_type, "
          + "routine_definition, external_language, comment, created_by "
          + "FROM system.information_schema.routines "
          + "ORDER BY routine_catalog, routine_schema, routine_name";

  /** Older runtimes have no {@code comment} or {@code created_by} on this view. */
  private static final String COMPATIBILITY_SQL =
      "SELECT routine_catalog, routine_schema, routine_name, "
          + "coalesce(full_data_type, data_type) AS data_type, "
          + "routine_definition, external_language "
          + "FROM system.information_schema.routines "
          + "ORDER BY routine_catalog, routine_schema, routine_name";

  DatabricksSystemSqlFunctionsTask(
      @Nonnull Predicate<String> catalogPredicate, @Nonnull Predicate<String> schemaPredicate) {
    super(ZIP_ENTRY_NAME, catalogPredicate, schemaPredicate);
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    DatabricksHandle databricksHandle = (DatabricksHandle) handle;
    logger.info("Writing functions from system tables to '{}'", getTargetPath());
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
        CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer);
        RecordProgressMonitor monitor =
            new RecordProgressMonitor(
                "Writing functions from system tables to " + getTargetPath())) {
      Map<String, List<String>> parametersByFunction = fetchParameters(databricksHandle);
      executeWithCompatibilityFallback(
          databricksHandle,
          SQL,
          COMPATIBILITY_SQL,
          row -> {
            String catalogName = cell(row, 0);
            String schemaName = cell(row, 1);
            if (catalogName == null
                || schemaName == null
                || !catalogPredicate.test(catalogName)
                || !schemaPredicate.test(schemaName)
                || databricksHandle.isCatalogInaccessible(catalogName)) {
              return;
            }
            String routineName = cell(row, 2);
            monitor.count();
            printer.printRecord(
                catalogName,
                schemaName,
                routineName,
                cell(row, 3),
                joinParameters(parametersByFunction.get(key(catalogName, schemaName, routineName))),
                cell(row, 4),
                cell(row, 5),
                cell(row, 6),
                cell(row, 7));
          });
    }
    return null;
  }

  /**
   * Returns the declared parameters of each routine, keyed by its fully qualified name.
   *
   * <p>Parameters are an enrichment: if the view cannot be read the functions themselves are still
   * worth dumping, so a failure here is logged rather than propagated.
   */
  private static Map<String, List<String>> fetchParameters(@Nonnull DatabricksHandle handle) {
    Map<String, List<String>> parametersByFunction = new HashMap<>();
    try {
      DatabricksSqlHelper.executeBulkQueryOrThrow(
          handle,
          PARAMETERS_SQL,
          row -> {
            String catalogName = cell(row, 0);
            String schemaName = cell(row, 1);
            String routineName = cell(row, 2);
            if (catalogName == null || schemaName == null || routineName == null) {
              return;
            }
            String parameter = describeParameter(cell(row, 3), cell(row, 4));
            if (parameter != null) {
              parametersByFunction
                  .computeIfAbsent(
                      key(catalogName, schemaName, routineName), k -> new ArrayList<>())
                  .add(parameter);
            }
          });
    } catch (SQLException e) {
      logger.info(
          "Function parameters are unavailable, so functions will be dumped without them: {}",
          e.getMessage());
    }
    return parametersByFunction;
  }

  private static String key(String catalogName, String schemaName, String routineName) {
    return catalogName + "." + schemaName + "." + routineName;
  }

  private static String describeParameter(String name, String dataType) {
    if (name != null && dataType != null) {
      return name + " " + dataType;
    }
    return name != null ? name : dataType;
  }

  private static String joinParameters(List<String> parameters) {
    return parameters == null || parameters.isEmpty() ? null : StringUtils.join(parameters, ", ");
  }
}
