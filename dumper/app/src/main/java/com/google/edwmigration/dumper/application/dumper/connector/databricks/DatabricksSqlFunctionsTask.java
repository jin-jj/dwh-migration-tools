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
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.FunctionsFormat;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dumps user-defined functions from the {@code information_schema} of each catalog. */
class DatabricksSqlFunctionsTask extends AbstractDatabricksSqlTask implements FunctionsFormat {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksSqlFunctionsTask.class);

  private static final String PARAMETERS_SQL =
      "SELECT specific_schema, specific_name, parameter_name, "
          + "coalesce(full_data_type, data_type) AS data_type "
          + "FROM "
          + CATALOG
          + ".information_schema.parameters"
          + WHERE
          + " ORDER BY specific_schema, specific_name, ordinal_position";

  private static final String SQL =
      "SELECT routine_catalog, routine_schema, routine_name, "
          + "coalesce(full_data_type, data_type) AS data_type, "
          + "routine_definition, external_language, comment, created_by "
          + "FROM "
          + CATALOG
          + ".information_schema.routines"
          + WHERE
          + " ORDER BY routine_schema, routine_name";

  /** {@code comment} and {@code created_by} are absent from older runtimes. */
  private static final String COMPATIBILITY_SQL =
      "SELECT routine_catalog, routine_schema, routine_name, "
          + "coalesce(full_data_type, data_type) AS data_type, "
          + "routine_definition, external_language "
          + "FROM "
          + CATALOG
          + ".information_schema.routines"
          + WHERE
          + " ORDER BY routine_schema, routine_name";

  DatabricksSqlFunctionsTask(@Nonnull DatabricksFilter filter) {
    super(ZIP_ENTRY_NAME, filter);
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
      forEachCatalog(
          databricksHandle,
          escapedCatalog -> {
            Map<String, List<String>> parameters =
                fetchParameters(databricksHandle, escapedCatalog);
            executeWithCompatibilityFallback(
                databricksHandle,
                withFilter(SQL, /* catalogColumn= */ null, "routine_schema")
                    .replace(CATALOG, escapedCatalog),
                withFilter(COMPATIBILITY_SQL, /* catalogColumn= */ null, "routine_schema")
                    .replace(CATALOG, escapedCatalog),
                row -> {
                  String schemaName = cell(row, 1);
                  if (schemaName == null || !filter.matchesSchema(schemaName)) {
                    return;
                  }
                  monitor.count();
                  printer.printRecord(
                      cell(row, 0),
                      schemaName,
                      cell(row, 2),
                      cell(row, 3),
                      joinParameters(parameters.get(key(schemaName, cell(row, 2)))),
                      cell(row, 4),
                      cell(row, 5),
                      cell(row, 6),
                      cell(row, 7));
                });
          });
    }
    return null;
  }

  /**
   * Returns the declared parameters of each routine of one catalog, keyed by schema and name.
   *
   * <p>A missing parameter list only costs the signature column, so a failure here is logged and
   * the routines themselves are still dumped.
   */
  @Nonnull
  private Map<String, List<String>> fetchParameters(
      @Nonnull DatabricksHandle handle, @Nonnull String escapedCatalog) {
    Map<String, List<String>> parameters = new HashMap<>();
    try {
      DatabricksSqlHelper.executeBulkQueryOrThrow(
          handle,
          withFilter(PARAMETERS_SQL, /* catalogColumn= */ null, "specific_schema")
              .replace(CATALOG, escapedCatalog),
          row -> {
            String parameter = describeParameter(cell(row, 2), cell(row, 3));
            if (parameter != null) {
              String key = key(cell(row, 0), cell(row, 1));
              List<String> declared = parameters.get(key);
              if (declared == null) {
                declared = new ArrayList<>();
                parameters.put(key, declared);
              }
              declared.add(parameter);
            }
          });
    } catch (Exception e) {
      logger.warn(
          "Failed to read the parameters of the routines of catalog {}: {}",
          escapedCatalog,
          e.getMessage());
    }
    return parameters;
  }

  private static String key(@CheckForNull String schema, @CheckForNull String name) {
    return schema + "." + name;
  }

  /** Renders one parameter as {@code name type}, tolerating either half being absent. */
  @CheckForNull
  private static String describeParameter(
      @CheckForNull String name, @CheckForNull String dataType) {
    if (name != null && dataType != null) {
      return name + " " + dataType;
    }
    return name != null ? name : dataType;
  }

  @CheckForNull
  private static String joinParameters(@CheckForNull List<String> parameters) {
    return parameters == null || parameters.isEmpty() ? null : join(parameters, ", ");
  }
}
