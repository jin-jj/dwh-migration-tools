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

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksFilter.HIVE_METASTORE_CATALOG;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksJson.toJsonOrEmpty;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.cell;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.quoteIdentifier;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreFunctionParser.ParsedFunction;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.HiveMetastoreSessionCache;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.FunctionsHeader;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.io.Writer;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;

/**
 * Extraction tasks for the functions of the legacy {@code hive_metastore} catalog using {@code SHOW
 * USER FUNCTIONS} and {@code DESCRIBE FUNCTION EXTENDED}.
 */
final class DatabricksHiveMetastoreFunctionTasks {

  private DatabricksHiveMetastoreFunctionTasks() {}

  /**
   * Shared per-connector cache of the parsed {@code hive_metastore} functions, so that each
   * function is described once for all the tasks that need it.
   */
  static final class HiveMetastoreFunctionCache {

    private final HiveMetastoreSessionCache sessionCache;
    private final Memoized<ImmutableList<ParsedFunction>> functions =
        new Memoized<>("Listing hive_metastore functions");

    HiveMetastoreFunctionCache(@Nonnull HiveMetastoreSessionCache sessionCache) {
      this.sessionCache = Preconditions.checkNotNull(sessionCache, "Session cache was null.");
    }

    @Nonnull
    ImmutableList<ParsedFunction> listParsedFunctions(@Nonnull DatabricksHandle handle)
        throws IOException {
      return functions.load(() -> loadFunctions(handle));
    }

    @Nonnull
    private ImmutableList<ParsedFunction> loadFunctions(@Nonnull DatabricksHandle handle)
        throws IOException {
      ImmutableList.Builder<FunctionRef> functionRefs = ImmutableList.builder();
      handle
          .statementRunner()
          .runInOrder(
              "SHOW USER FUNCTIONS",
              sessionCache.listSchemas(handle),
              DatabricksSqlHelper::quoteIdentifier,
              schema -> showUserFunctions(handle, schema),
              (schema, schemaFunctionRefs) -> functionRefs.addAll(schemaFunctionRefs));
      ImmutableList.Builder<ParsedFunction> builder = ImmutableList.builder();
      handle
          .statementRunner()
          .runInOrder(
              "DESCRIBE FUNCTION EXTENDED",
              functionRefs.build(),
              FunctionRef::quotedName,
              functionRef -> describeFunction(handle, functionRef),
              (functionRef, parsed) -> builder.add(parsed));
      return builder.build();
    }

    @Nonnull
    private static ImmutableList<FunctionRef> showUserFunctions(
        @Nonnull DatabricksHandle handle, @Nonnull String schema) throws IOException {
      String query = "SHOW USER FUNCTIONS IN " + quoteIdentifier(schema);
      ImmutableList.Builder<FunctionRef> builder = ImmutableList.builder();
      try (CloseableIterable<List<String>> rows =
          handle.sqlHelper().executeSqlInCatalog(HIVE_METASTORE_CATALOG, query)) {
        for (List<String> row : rows) {
          String rawName = cell(row, 0);
          if (rawName != null && !rawName.trim().isEmpty()) {
            builder.add(new FunctionRef(schema, parseShortFunctionName(rawName.trim())));
          }
        }
      }
      return builder.build();
    }

    @Nonnull
    private static ParsedFunction describeFunction(
        @Nonnull DatabricksHandle handle, @Nonnull FunctionRef functionRef) throws IOException {
      String query = "DESCRIBE FUNCTION EXTENDED " + functionRef.quotedName();
      ImmutableList.Builder<String> lines = ImmutableList.builder();
      try (CloseableIterable<List<String>> rows =
          handle.sqlHelper().executeSqlInCatalog(HIVE_METASTORE_CATALOG, query)) {
        for (List<String> row : rows) {
          String line = cell(row, 0);
          if (line != null) {
            lines.add(line);
          }
        }
      }
      return DatabricksHiveMetastoreFunctionParser.parseDescribeFunctionExtended(
          HIVE_METASTORE_CATALOG, functionRef.schema, functionRef.name, lines.build());
    }

    /** Returns the function name of a qualified name, such as {@code f} in {@code schema.f}. */
    @Nonnull
    private static String parseShortFunctionName(@Nonnull String qualifiedName) {
      int lastDot = qualifiedName.lastIndexOf('.');
      return lastDot >= 0 ? qualifiedName.substring(lastDot + 1) : qualifiedName;
    }
  }

  /** A user function in a {@code hive_metastore} schema. */
  private static final class FunctionRef {

    final String schema;
    final String name;

    FunctionRef(@Nonnull String schema, @Nonnull String name) {
      this.schema = schema;
      this.name = name;
    }

    @Nonnull
    String quotedName() {
      return quoteIdentifier(schema) + "." + quoteIdentifier(name);
    }
  }

  static final class DatabricksHiveMetastoreFunctionsTask extends AbstractTask<Void> {

    private final HiveMetastoreFunctionCache functionCache;

    DatabricksHiveMetastoreFunctionsTask(@Nonnull HiveMetastoreFunctionCache functionCache) {
      super(DatabricksMetadataDumpFormat.HIVE_METASTORE_FUNCTIONS_ENTRY_NAME);
      this.functionCache = functionCache;
    }

    @Override
    @Nonnull
    public String getName() {
      return "databricks-hms-functions";
    }

    @Override
    @Nonnull
    public TaskCategory getCategory() {
      return TaskCategory.OPTIONAL;
    }

    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      DatabricksHandle dbxHandle = (DatabricksHandle) handle;
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(FunctionsHeader.class).print(writer)) {
        for (ParsedFunction function : functionCache.listParsedFunctions(dbxHandle)) {
          csv.printRecord(
              function.functionCatalog,
              function.functionSchema,
              function.functionName,
              function.dataType,
              toJsonOrEmpty(function.inputParams),
              toJsonOrEmpty(function.returnParams),
              function.routineDefinition,
              function.routineLanguage,
              function.comment,
              function.owner,
              function.isDeterministic,
              function.sqlDataAccess,
              "",
              "",
              function.createdAt,
              "",
              function.className,
              function.handler);
        }
      }
      return null;
    }
  }
}
