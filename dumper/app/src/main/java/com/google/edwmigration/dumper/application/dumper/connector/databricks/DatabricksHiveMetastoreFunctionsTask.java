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
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.executeQueryInCatalogOrThrow;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.RecordProgressMonitor;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.FunctionsFormat;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dumps user-defined functions from the legacy {@code hive_metastore} catalog.
 *
 * <p>Far less is knowable here than in Unity Catalog. A Unity Catalog function is described by
 * {@code information_schema.routines}, which carries its return type, parameters, body, language,
 * comment and owner. {@code hive_metastore} has no information schema, and its functions are Hive
 * UDFs: Java classes registered with {@code CREATE FUNCTION ... AS '<class>' USING JAR}. Such a
 * function has no SQL body, and its return type and parameter list are resolved reflectively at
 * each call site rather than recorded in the metastore.
 *
 * <p>What is obtainable is the name and the implementing class, which is the part that matters for
 * a migration: the class identifies the code that has to be ported. The remaining columns are
 * written empty rather than filled with plausible-looking values.
 */
class DatabricksHiveMetastoreFunctionsTask extends AbstractDatabricksHiveMetastoreTask
    implements FunctionsFormat {

  private static final Logger logger =
      LoggerFactory.getLogger(DatabricksHiveMetastoreFunctionsTask.class);

  /** The label {@code DESCRIBE FUNCTION EXTENDED} uses for the implementing class. */
  private static final String CLASS_LABEL = "Class:";

  /** Set when a class name was found, to record that the function is externally implemented. */
  private static final String EXTERNAL_LANGUAGE = "JAVA";

  DatabricksHiveMetastoreFunctionsTask(@Nonnull DatabricksFilter filter) {
    super(HMS_ZIP_ENTRY_NAME, filter);
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    DatabricksHandle databricksHandle = (DatabricksHandle) handle;
    if (!databricksHandle.hasWarehouseId()) {
      logger.info("Skipping '{}' because no SQL warehouse was configured", getTargetPath());
      return null;
    }
    logger.info("Writing hive_metastore functions to '{}'", getTargetPath());
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
        CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer);
        RecordProgressMonitor monitor =
            new RecordProgressMonitor("Writing hive_metastore functions to " + getTargetPath())) {
      forEachSchemaFunction(
          databricksHandle,
          (schemaName, functionName) -> {
            String className = findImplementingClass(databricksHandle, schemaName, functionName);
            monitor.count();
            printer.printRecord(
                HIVE_METASTORE,
                schemaName,
                functionName,
                // A Hive UDF resolves its signature per call site, so neither of these is recorded.
                /* dataType= */ null,
                /* inputParams= */ null,
                className,
                className == null ? null : EXTERNAL_LANGUAGE,
                // DESCRIBE FUNCTION reports neither for a Hive UDF.
                /* comment= */ null,
                /* owner= */ null);
          });
    }
    return null;
  }

  /**
   * Lists the user functions of every matching schema and hands each to {@code consumer}.
   *
   * <p>A schema that cannot be listed is logged and skipped, matching the table walk; if every
   * schema fails the failure is raised so the task does not report success over an empty file.
   */
  private void forEachSchemaFunction(
      @Nonnull DatabricksHandle handle, @Nonnull FunctionConsumer consumer) throws Exception {
    ImmutableList<String> schemaNames = fetchMatchingSchemaNames(handle);
    int failures = 0;
    SQLException lastFailure = null;
    for (String schemaName : schemaNames) {
      List<List<String>> rows = new ArrayList<>();
      try {
        // USER is required: the default is ALL, which would bury the handful of real functions
        // under several hundred built-ins.
        executeQueryInCatalogOrThrow(
            handle,
            HIVE_METASTORE,
            "SHOW USER FUNCTIONS IN " + escapeIdentifier(schemaName),
            rows::add);
      } catch (SQLException e) {
        failures++;
        lastFailure = e;
        logger.warn(
            "Failed to list the functions of hive_metastore.{}: {}", schemaName, e.getMessage());
        continue;
      }
      for (List<String> row : rows) {
        String name = simpleName(row);
        if (name != null) {
          consumer.accept(schemaName, name);
        }
      }
    }
    if (lastFailure != null && failures == schemaNames.size()) {
      throw lastFailure;
    }
  }

  /**
   * Returns the bare function name from a {@code SHOW USER FUNCTIONS} row.
   *
   * <p>The single column is qualified, but by how much depends on the catalog implementation
   * answering: {@code schema.function} and {@code catalog.schema.function} are both observed. The
   * loop already knows the schema, so the trailing segment is taken and the prefix ignored rather
   * than parsed.
   */
  @CheckForNull
  private static String simpleName(@Nonnull List<String> row) {
    if (row.isEmpty() || row.get(0) == null) {
      return null;
    }
    String qualified = row.get(0).trim();
    if (qualified.isEmpty()) {
      return null;
    }
    int lastDot = qualified.lastIndexOf('.');
    return lastDot < 0 ? qualified : qualified.substring(lastDot + 1);
  }

  /**
   * Returns the implementing class of one function, or null if it could not be determined.
   *
   * <p>There is no bulk form of this: {@code SHOW FUNCTIONS} has no {@code EXTENDED} variant and
   * the legacy metastore has no information schema, so one statement per function is the floor.
   * Function counts in a Hive Metastore are typically small enough for that to be affordable.
   *
   * <p>A function that cannot be described still produces a row. The name alone is worth recording,
   * and a describe can fail for reasons that say nothing about the function's existence — the
   * warehouse may be unable to load the JAR behind it.
   */
  @CheckForNull
  private static String findImplementingClass(
      @Nonnull DatabricksHandle handle, @Nonnull String schemaName, @Nonnull String functionName) {
    List<List<String>> rows = new ArrayList<>();
    try {
      executeQueryInCatalogOrThrow(
          handle,
          HIVE_METASTORE,
          "DESCRIBE FUNCTION EXTENDED "
              + escapeIdentifier(schemaName)
              + "."
              + escapeIdentifier(functionName),
          rows::add);
    } catch (SQLException e) {
      logger.warn(
          "Failed to describe hive_metastore.{}.{}: {}", schemaName, functionName, e.getMessage());
      return null;
    }
    // The result is one column of free text, one line per row.
    for (List<String> row : rows) {
      if (row.isEmpty() || row.get(0) == null) {
        continue;
      }
      String line = row.get(0).trim();
      if (line.startsWith(CLASS_LABEL)) {
        String className = line.substring(CLASS_LABEL.length()).trim();
        if (!className.isEmpty()) {
          return className;
        }
      }
    }
    return null;
  }

  /** Receives one function name at a time, together with the schema holding it. */
  private interface FunctionConsumer {
    void accept(@Nonnull String schemaName, @Nonnull String functionName) throws Exception;
  }
}
