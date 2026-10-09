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
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.acceptAllFilter;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.failed;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.mockStatements;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.succeeded;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.StatementExecutionAPI;
import com.databricks.sdk.service.sql.StatementResponse;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreFunctionParser.ParsedFunction;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreFunctionTasks.DatabricksHiveMetastoreFunctionsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreFunctionTasks.HiveMetastoreFunctionCache;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.HiveMetastoreSessionCache;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.FunctionsHeader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksHiveMetastoreFunctionTasksTest {

  private static final String SCHEMA = "legacy";
  private static final String SHOW_SCHEMAS = "SHOW SCHEMAS";
  private static final String SHOW_FUNCTIONS = "SHOW USER FUNCTIONS IN `legacy`";

  /** The value of the columns that only Unity Catalog provides, such as {@code SecurityType}. */
  private static final String UNITY_CATALOG_ONLY = "";

  private static final ObjectMapper JSON = new ObjectMapper();

  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void functionsTask_writesParsedFunctions() throws Exception {
    ImmutableList<String> sqlLines =
        ImmutableList.of(
            "Function:      spark_catalog.legacy.double_it",
            "Type:          SCALAR",
            "Input:         x INT 'the input'",
            "Returns:       INT",
            "Comment:       Doubles input",
            "Deterministic: true",
            "Data Access:   CONTAINS SQL",
            "Owner:         alice",
            "Create Time:   Sun Sep 27 21:03:11 UTC 2026",
            "Body:          x * 2");
    ImmutableList<String> classLines =
        ImmutableList.of(
            "Function: spark_catalog.legacy.custom_hash",
            "Class:    com.acme.udf.CustomHashUdf",
            "Owner:    bob");
    ImmutableList<String> tableLines =
        ImmutableList.of(
            "Function:      spark_catalog.legacy.ids",
            "Type:          TABLE",
            "Input:         n INT",
            "Returns:       id BIGINT 'the id'",
            "Body:          SELECT id FROM range(n)");
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.<String, StatementResponse>builder()
                .put(SHOW_SCHEMAS, succeeded(SCHEMA))
                .put(SHOW_FUNCTIONS, showUserFunctions("double_it", "custom_hash", "ids"))
                .put(describeFunction("double_it"), succeeded(sqlLines.toArray(new String[0])))
                .put(describeFunction("custom_hash"), succeeded(classLines.toArray(new String[0])))
                .put(describeFunction("ids"), succeeded(tableLines.toArray(new String[0])))
                .build());

    List<CSVRecord> records = runFunctionsTask(statements, newFunctionCache());

    ImmutableList<List<String>> expected =
        ImmutableList.of(
            functionRow(parseFunction("double_it", sqlLines)),
            functionRow(parseFunction("custom_hash", classLines)),
            functionRow(parseFunction("ids", tableLines)));
    List<List<String>> written = new ArrayList<>();
    for (CSVRecord record : records) {
      written.add(record.toList());
    }
    assertEquals(expected, written);
    assertEquals(
        ImmutableList.of(
            ImmutableMap.of("name", "x", "type", "INT", "mode", "IN", "comment", "the input")),
        parseJsonArray(records.get(0).get(FunctionsHeader.InputParams)));
    assertEquals(
        ImmutableList.of(ImmutableMap.of("name", "id", "type", "BIGINT", "comment", "the id")),
        parseJsonArray(records.get(2).get(FunctionsHeader.ReturnParams)));
  }

  @Test
  public void functionsTask_skipsSchemaWhoseShowUserFunctionsFails() throws Exception {
    String restrictedSchema = "restricted";
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(restrictedSchema, SCHEMA),
                "SHOW USER FUNCTIONS IN `" + restrictedSchema + "`",
                failed("[INSUFFICIENT_PERMISSIONS] Insufficient privileges."),
                SHOW_FUNCTIONS,
                showUserFunctions("double_it"),
                describeFunction("double_it"),
                succeeded("Body: 2")));

    List<CSVRecord> records = runFunctionsTask(statements, newFunctionCache());

    assertEquals(1, records.size());
    assertEquals(SCHEMA, records.get(0).get(FunctionsHeader.FunctionSchema));
    assertEquals("double_it", records.get(0).get(FunctionsHeader.FunctionName));
  }

  @Test
  public void functionsTask_skipsFunctionWhoseDescribeFails() throws Exception {
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_FUNCTIONS,
                showUserFunctions("broken_fn", "double_it"),
                describeFunction("broken_fn"),
                failed("[ROUTINE_NOT_FOUND] The routine `legacy`.`broken_fn` cannot be found."),
                describeFunction("double_it"),
                succeeded("Body: 2")));

    List<CSVRecord> records = runFunctionsTask(statements, newFunctionCache());

    assertEquals(1, records.size());
    assertEquals("double_it", records.get(0).get(FunctionsHeader.FunctionName));
    assertEquals("2", records.get(0).get(FunctionsHeader.RoutineDefinition));
  }

  @Test
  public void functionCache_describesEachFunctionOnceForAllTasks() throws Exception {
    String describeStatement = describeFunction("double_it");
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_FUNCTIONS,
                showUserFunctions("double_it"),
                describeStatement,
                succeeded("Body: 2")));
    HiveMetastoreFunctionCache functionCache = newFunctionCache();

    runFunctionsTask(statements, functionCache);
    runFunctionsTask(statements, functionCache);

    int statementCount = ImmutableList.of(SHOW_SCHEMAS, SHOW_FUNCTIONS, describeStatement).size();
    verify(statements, times(statementCount)).executeStatement(any(ExecuteStatementRequest.class));
  }

  private List<CSVRecord> runFunctionsTask(
      StatementExecutionAPI statements, HiveMetastoreFunctionCache functionCache) throws Exception {
    return DatabricksStatementStubs.runTask(
        tempFolder.newFile(),
        statements,
        acceptAllFilter(),
        (sink, handle) ->
            new DatabricksHiveMetastoreFunctionsTask(functionCache).doRun(null, sink, handle));
  }

  private static HiveMetastoreFunctionCache newFunctionCache() {
    return new HiveMetastoreFunctionCache(new HiveMetastoreSessionCache());
  }

  /** Returns the {@code SHOW USER FUNCTIONS} result of functions of the schema. */
  private static StatementResponse showUserFunctions(String... functionNames) {
    List<String> qualifiedNames = new ArrayList<>();
    for (String functionName : functionNames) {
      qualifiedNames.add("spark_catalog." + SCHEMA + "." + functionName);
    }
    return succeeded(qualifiedNames.toArray(new String[0]));
  }

  private static String describeFunction(String functionName) {
    return "DESCRIBE FUNCTION EXTENDED `" + SCHEMA + "`.`" + functionName + "`";
  }

  private static ParsedFunction parseFunction(String functionName, List<String> lines) {
    return DatabricksHiveMetastoreFunctionParser.parseDescribeFunctionExtended(
        HIVE_METASTORE_CATALOG, SCHEMA, functionName, lines);
  }

  /** Returns the functions file row of a function in {@link FunctionsHeader} order. */
  private static List<String> functionRow(ParsedFunction function) {
    return ImmutableList.of(
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
        UNITY_CATALOG_ONLY,
        UNITY_CATALOG_ONLY,
        function.createdAt,
        UNITY_CATALOG_ONLY,
        function.className,
        function.handler,
        UNITY_CATALOG_ONLY);
  }

  private static List<Map<String, String>> parseJsonArray(String json) throws IOException {
    return JSON.readValue(json, new TypeReference<List<Map<String, String>>>() {});
  }
}
