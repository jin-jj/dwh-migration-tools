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
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreFunctionDdlFormatter.formatCreateFunction;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.acceptAllFilter;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.failed;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.mockStatements;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.succeeded;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.succeededRows;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.StatementExecutionAPI;
import com.databricks.sdk.service.sql.StatementResponse;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDdlTasks.DatabricksHiveMetastoreDdlsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDdlTasks.DatabricksHiveMetastoreFunctionDdlsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreFunctionParser.ParsedFunction;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreFunctionTasks.DatabricksHiveMetastoreFunctionsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreFunctionTasks.HiveMetastoreFunctionCache;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.HiveMetastoreSessionCache;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.DdlsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.FunctionDdlsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.TaskBody;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.csv.CSVRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksHiveMetastoreDdlTasksTest {

  private static final String SCHEMA = "legacy";
  private static final String SHOW_SCHEMAS = "SHOW SCHEMAS";
  private static final String SHOW_TABLES = "SHOW TABLE EXTENDED IN `legacy` LIKE '*'";
  private static final String SHOW_FUNCTIONS = "SHOW USER FUNCTIONS IN `legacy`";

  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void ddlsTask_skipsTableWithUnsupportedSerde() throws Exception {
    String deltaDdl = "CREATE TABLE hive_metastore.legacy.delta_tbl (id INT NOT NULL) USING delta";
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                showTables("delta_tbl", "hive_serde_tbl"),
                showCreateTable("delta_tbl"),
                succeeded(deltaDdl),
                showCreateTable("hive_serde_tbl"),
                failed(
                    "[UNSUPPORTED_SHOW_CREATE_TABLE.WITH_UNSUPPORTED_SERDE_CONFIGURATION]"
                        + " Unsupported SHOW CREATE TABLE command.")));
    HiveMetastoreSessionCache sessionCache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreDdlsTask(sessionCache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    assertEquals(HIVE_METASTORE_CATALOG, records.get(0).get(DdlsHeader.TableCatalog));
    assertEquals(SCHEMA, records.get(0).get(DdlsHeader.TableSchema));
    assertEquals("delta_tbl", records.get(0).get(DdlsHeader.TableName));
    assertEquals("MANAGED", records.get(0).get(DdlsHeader.TableType));
    assertEquals(deltaDdl, records.get(0).get(DdlsHeader.Ddl));
  }

  @Test
  public void ddlsTask_skipsTableWhoseResultDownloadFails() throws Exception {
    String goodDdl = "CREATE TABLE hive_metastore.legacy.good_tbl (id INT) USING delta";
    String brokenStatementId = "stmt-broken";
    StatementResponse brokenResponse =
        new StatementResponse()
            .setStatementId(brokenStatementId)
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
            .setResult(
                new ResultData()
                    .setChunkIndex(0L)
                    .setNextChunkIndex(1L)
                    .setDataArray(
                        ImmutableList.<Collection<String>>of(ImmutableList.of("CREATE TABLE"))));
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                showTables("broken_tbl", "good_tbl"),
                showCreateTable("broken_tbl"),
                brokenResponse,
                showCreateTable("good_tbl"),
                succeeded(goodDdl)));
    when(statements.getStatementResultChunkN(brokenStatementId, 1L))
        .thenThrow(new DatabricksException("Connection reset"));
    HiveMetastoreSessionCache sessionCache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreDdlsTask(sessionCache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    assertEquals("good_tbl", records.get(0).get(DdlsHeader.TableName));
    assertEquals(goodDdl, records.get(0).get(DdlsHeader.Ddl));
  }

  @Test
  public void ddlsTask_dropsTrailingLineBreak() throws Exception {
    String ddl = "CREATE TABLE hive_metastore.legacy.delta_tbl (\n  id INT)\nUSING delta";
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                showTables("delta_tbl"),
                showCreateTable("delta_tbl"),
                succeeded(ddl + "\n")));
    HiveMetastoreSessionCache sessionCache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreDdlsTask(sessionCache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    assertEquals(ddl, records.get(0).get(DdlsHeader.Ddl));
  }

  @Test
  public void ddlsTask_writesHiveMetastoreAsCatalogWhenSparkCatalogIsPrinted() throws Exception {
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                showTablesInCatalog("spark_catalog", "delta_tbl"),
                showCreateTable("delta_tbl"),
                succeeded("CREATE TABLE spark_catalog.legacy.delta_tbl (id INT) USING delta")));
    HiveMetastoreSessionCache sessionCache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreDdlsTask(sessionCache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    assertEquals(HIVE_METASTORE_CATALOG, records.get(0).get(DdlsHeader.TableCatalog));
  }

  @Test
  public void functionDdlsTask_writesDdlOfFunctionsWithBodyOrClass() throws Exception {
    ImmutableList<String> sqlLines =
        ImmutableList.of("Input: x INT", "Returns: INT", "Deterministic: true", "Body: x * 2");
    ImmutableList<String> classLines = ImmutableList.of("Class: com.acme.udf.CustomHashUdf");
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_FUNCTIONS,
                showUserFunctions("double_it", "custom_hash", "no_definition"),
                describeFunction("double_it"),
                succeeded(sqlLines.toArray(new String[0])),
                describeFunction("custom_hash"),
                succeeded(classLines.toArray(new String[0])),
                describeFunction("no_definition"),
                succeeded("Owner: bob")));

    List<CSVRecord> records = runFunctionDdlsTask(statements, newFunctionCache());

    ImmutableList<String> expected =
        ImmutableList.of(
            String.join(
                "|", "double_it", formatCreateFunction(parseFunction("double_it", sqlLines))),
            String.join(
                "|",
                "custom_hash",
                formatCreateFunction(parseFunction("custom_hash", classLines))));
    List<String> written = new ArrayList<>();
    for (CSVRecord record : records) {
      assertEquals(HIVE_METASTORE_CATALOG, record.get(FunctionDdlsHeader.FunctionCatalog));
      assertEquals(SCHEMA, record.get(FunctionDdlsHeader.FunctionSchema));
      written.add(
          String.join(
              "|",
              record.get(FunctionDdlsHeader.FunctionName),
              record.get(FunctionDdlsHeader.Ddl)));
    }
    assertEquals(expected, written);
  }

  @Test
  public void functionDdlsTask_skipsFunctionWhoseDescribeFails() throws Exception {
    ImmutableList<String> describeLines = ImmutableList.of("Returns: INT", "Body: 2");
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_FUNCTIONS,
                showUserFunctions("double_it", "broken_fn"),
                describeFunction("double_it"),
                succeeded(describeLines.toArray(new String[0])),
                describeFunction("broken_fn"),
                failed("[ROUTINE_NOT_FOUND] The routine `legacy`.`broken_fn` cannot be found.")));

    List<CSVRecord> records = runFunctionDdlsTask(statements, newFunctionCache());

    assertEquals(1, records.size());
    assertEquals("double_it", records.get(0).get(FunctionDdlsHeader.FunctionName));
    assertEquals(
        formatCreateFunction(parseFunction("double_it", describeLines)),
        records.get(0).get(FunctionDdlsHeader.Ddl));
  }

  @Test
  public void functionCache_describesFunctionsOnceForFunctionsAndFunctionDdls() throws Exception {
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

    runTask(
        statements,
        (sink, handle) ->
            new DatabricksHiveMetastoreFunctionsTask(functionCache).doRun(null, sink, handle));
    runFunctionDdlsTask(statements, functionCache);

    int statementCount = ImmutableList.of(SHOW_SCHEMAS, SHOW_FUNCTIONS, describeStatement).size();
    verify(statements, times(statementCount)).executeStatement(any(ExecuteStatementRequest.class));
  }

  private List<CSVRecord> runFunctionDdlsTask(
      StatementExecutionAPI statements, HiveMetastoreFunctionCache functionCache) throws Exception {
    return runTask(
        statements,
        (sink, handle) ->
            new DatabricksHiveMetastoreFunctionDdlsTask(functionCache).doRun(null, sink, handle));
  }

  private List<CSVRecord> runTask(StatementExecutionAPI statements, TaskBody body)
      throws Exception {
    return DatabricksStatementStubs.runTask(
        tempFolder.newFile(), statements, acceptAllFilter(), body);
  }

  private static HiveMetastoreFunctionCache newFunctionCache() {
    return new HiveMetastoreFunctionCache(new HiveMetastoreSessionCache());
  }

  /** Returns the {@code SHOW TABLE EXTENDED} result of managed Delta tables with an int column. */
  private static StatementResponse showTables(String... tableNames) {
    return showTablesInCatalog(HIVE_METASTORE_CATALOG, tableNames);
  }

  /**
   * Returns the {@code SHOW TABLE EXTENDED} result of managed Delta tables with an int column,
   * whose {@code Catalog} line prints {@code printedCatalog}.
   */
  private static StatementResponse showTablesInCatalog(
      String printedCatalog, String... tableNames) {
    List<List<String>> rows = new ArrayList<>();
    for (String tableName : tableNames) {
      String information =
          String.join(
              "\n",
              "Catalog: " + printedCatalog,
              "Database: " + SCHEMA,
              "Table: " + tableName,
              "Type: MANAGED",
              "Provider: delta",
              "Schema: root",
              " |-- id: integer (nullable = false)",
              "");
      rows.add(ImmutableList.of(SCHEMA, tableName, "false", information));
    }
    return succeededRows(rows);
  }

  private static String showCreateTable(String tableName) {
    return "SHOW CREATE TABLE `" + SCHEMA + "`.`" + tableName + "`";
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
}
