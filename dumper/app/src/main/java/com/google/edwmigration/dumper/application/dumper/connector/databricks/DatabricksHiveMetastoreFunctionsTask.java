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
 * <p>Functions in {@code hive_metastore} fall into two broad categories:
 *
 * <ul>
 *   <li><b>SQL functions:</b> Functions defined with {@code CREATE FUNCTION ... RETURN ...}. For
 *       these, {@code DESCRIBE FUNCTION EXTENDED} outputs structured fields including {@code
 *       Input:}, {@code Returns:}, {@code Body:}, {@code Comment:}, and {@code Owner:}.
 *   <li><b>Hive UDFs:</b> External Java classes registered with {@code CREATE FUNCTION ... AS
 *       '<class>' USING JAR}. These report {@code Class:} (the implementing class) and {@code
 *       Usage:}, but resolve their parameter types and return type reflectively per call site.
 * </ul>
 *
 * <p>This task issues {@code DESCRIBE FUNCTION EXTENDED} for each user function and parses
 * whichever metadata fields are returned. If a function cannot be described (e.g. warehouse
 * failure), the function name is still recorded with empty details.
 */
class DatabricksHiveMetastoreFunctionsTask extends AbstractDatabricksHiveMetastoreTask
    implements FunctionsFormat {

  private static final Logger logger =
      LoggerFactory.getLogger(DatabricksHiveMetastoreFunctionsTask.class);

  private static final String SQL_LANGUAGE = "SQL";
  private static final String JAVA_LANGUAGE = "JAVA";
  private static final String NOT_AVAILABLE = "N/A.";

  private static final ImmutableList<String> KNOWN_LABELS =
      ImmutableList.of(
          "Function",
          "Type",
          "Input",
          "Returns",
          "Comment",
          "Deterministic",
          "Data Access",
          "Configs",
          "Owner",
          "Create Time",
          "Body",
          "Class",
          "Usage",
          "Extended Usage");

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
            FunctionDescription desc = describeFunction(databricksHandle, schemaName, functionName);
            monitor.count();
            printer.printRecord(
                HIVE_METASTORE,
                schemaName,
                functionName,
                desc == null ? null : desc.getDataType(),
                desc == null ? null : desc.getInputParams(),
                desc == null ? null : desc.getRoutineDefinition(),
                desc == null ? null : desc.getRoutineLanguage(),
                desc == null ? null : desc.getComment(),
                desc == null ? null : desc.getOwner());
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
   * Describes one function via {@code DESCRIBE FUNCTION EXTENDED}, returning parsed metadata.
   *
   * <p>A function that cannot be described still produces a row with empty details. The name alone
   * is worth recording, and a describe can fail for reasons that say nothing about the function's
   * existence — for example, if the warehouse is unable to load an external JAR.
   */
  @CheckForNull
  private static FunctionDescription describeFunction(
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
    return parseDescription(rows);
  }

  /**
   * Parses the multi-line text output of {@code DESCRIBE FUNCTION EXTENDED}.
   *
   * <p>Each row returned by Databricks SQL represents a single line of free-form text.
   */
  @Nonnull
  static FunctionDescription parseDescription(@Nonnull List<List<String>> rows) {
    java.util.Map<String, String> fields = new java.util.HashMap<>();
    String currentField = null;
    StringBuilder currentContent = new StringBuilder();

    for (List<String> row : rows) {
      if (row.isEmpty() || row.get(0) == null) {
        continue;
      }
      String line = row.get(0);
      String trimmed = line.trim();
      if (trimmed.isEmpty()) {
        continue;
      }

      String matchedLabel = matchLabel(trimmed);
      if (matchedLabel != null) {
        if (currentField != null && currentContent.length() > 0) {
          fields.put(currentField, currentContent.toString().trim());
          currentContent.setLength(0);
        }
        currentField = matchedLabel;
        int colon = trimmed.indexOf(':');
        String remainder = trimmed.substring(colon + 1).trim();
        if (!remainder.isEmpty()) {
          currentContent.append(remainder);
        }
      } else if (currentField != null) {
        if (currentContent.length() > 0) {
          currentContent.append("\n");
        }
        currentContent.append(trimmed);
      }
    }
    if (currentField != null && currentContent.length() > 0) {
      fields.put(currentField, currentContent.toString().trim());
    }

    String dataType = fields.get("Returns");
    String inputParams = fields.get("Input");
    String body = fields.get("Body");
    String className = fields.get("Class");
    String comment = fields.get("Comment");
    String owner = fields.get("Owner");

    String routineDefinition = null;
    String routineLanguage = null;
    if (body != null && !body.isEmpty()) {
      routineDefinition = body;
      routineLanguage = SQL_LANGUAGE;
    } else if (className != null && !className.isEmpty()) {
      routineDefinition = className;
      routineLanguage = JAVA_LANGUAGE;
    }

    if (comment == null || comment.isEmpty()) {
      String usage = fields.get("Usage");
      if (usage != null && !usage.isEmpty() && !NOT_AVAILABLE.equalsIgnoreCase(usage)) {
        comment = usage;
      }
    }

    return new FunctionDescription(
        dataType, inputParams, routineDefinition, routineLanguage, comment, owner);
  }

  @CheckForNull
  private static String matchLabel(@Nonnull String trimmedLine) {
    int colon = trimmedLine.indexOf(':');
    if (colon < 0) {
      return null;
    }
    String candidate = trimmedLine.substring(0, colon).trim();
    for (String label : KNOWN_LABELS) {
      if (label.equalsIgnoreCase(candidate)) {
        return label;
      }
    }
    return null;
  }

  /** Parsed details of a function from {@code DESCRIBE FUNCTION EXTENDED}. */
  static class FunctionDescription {
    private final String dataType;
    private final String inputParams;
    private final String routineDefinition;
    private final String routineLanguage;
    private final String comment;
    private final String owner;

    FunctionDescription(
        @CheckForNull String dataType,
        @CheckForNull String inputParams,
        @CheckForNull String routineDefinition,
        @CheckForNull String routineLanguage,
        @CheckForNull String comment,
        @CheckForNull String owner) {
      this.dataType = dataType;
      this.inputParams = inputParams;
      this.routineDefinition = routineDefinition;
      this.routineLanguage = routineLanguage;
      this.comment = comment;
      this.owner = owner;
    }

    @CheckForNull
    String getDataType() {
      return dataType;
    }

    @CheckForNull
    String getInputParams() {
      return inputParams;
    }

    @CheckForNull
    String getRoutineDefinition() {
      return routineDefinition;
    }

    @CheckForNull
    String getRoutineLanguage() {
      return routineLanguage;
    }

    @CheckForNull
    String getComment() {
      return comment;
    }

    @CheckForNull
    String getOwner() {
      return owner;
    }
  }

  /** Receives one function name at a time, together with the schema holding it. */
  private interface FunctionConsumer {
    void accept(@Nonnull String schemaName, @Nonnull String functionName) throws Exception;
  }
}
