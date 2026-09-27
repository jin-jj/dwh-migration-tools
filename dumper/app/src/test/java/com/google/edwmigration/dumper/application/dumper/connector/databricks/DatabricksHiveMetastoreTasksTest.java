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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ServiceError;
import com.databricks.sdk.service.sql.StatementExecutionAPI;
import com.databricks.sdk.service.sql.StatementResponse;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSink;
import com.google.common.io.Files;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDumpFormat.DdlsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDumpFormat.FunctionDdlsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDumpFormat.SchemataHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreDdlsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreFunctionDdlsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreSchemataTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.HiveMetastoreSessionCache;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksHiveMetastoreTasksTest {

  private static final String SCHEMA = "legacy";
  private static final String SHOW_SCHEMAS = "SHOW SCHEMAS";
  private static final String SHOW_TABLES = "SHOW TABLE EXTENDED IN `legacy` LIKE '*'";
  private static final String SHOW_FUNCTIONS = "SHOW USER FUNCTIONS IN `legacy`";

  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void ddlsTask_skipsTableWhoseShowCreateTableFails() throws Exception {
    String deltaDdl = "CREATE TABLE hive_metastore.legacy.delta_tbl (id INT NOT NULL) USING delta";
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                showTablesResponse("delta_tbl", "hive_serde_tbl"),
                "SHOW CREATE TABLE `legacy`.`delta_tbl`",
                succeeded(deltaDdl),
                "SHOW CREATE TABLE `legacy`.`hive_serde_tbl`",
                failed(
                    "[UNSUPPORTED_SHOW_CREATE_TABLE.WITH_UNSUPPORTED_SERDE_CONFIGURATION]"
                        + " Unsupported SHOW CREATE TABLE command.")));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) -> new DatabricksHiveMetastoreDdlsTask(cache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    assertEquals("delta_tbl", records.get(0).get(DdlsHeader.TableName));
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
                    .setDataArray(rows(ImmutableList.of(ImmutableList.of("CREATE TABLE")))));
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                showTablesResponse("broken_tbl", "good_tbl"),
                "SHOW CREATE TABLE `legacy`.`broken_tbl`",
                brokenResponse,
                "SHOW CREATE TABLE `legacy`.`good_tbl`",
                succeeded(goodDdl)));
    when(statements.getStatementResultChunkN(brokenStatementId, 1L))
        .thenThrow(new DatabricksException("Connection reset"));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) -> new DatabricksHiveMetastoreDdlsTask(cache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    assertEquals("good_tbl", records.get(0).get(DdlsHeader.TableName));
    assertEquals(goodDdl, records.get(0).get(DdlsHeader.Ddl));
  }

  @Test
  public void functionDdlsTask_skipsFunctionWhoseDescribeFails() throws Exception {
    ImmutableList<String> describeLines =
        ImmutableList.of(
            "Function: hive_metastore.legacy.double_it",
            "Type: SCALAR",
            "Input: x INT",
            "Returns: INT",
            "Body: x * 2");
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_FUNCTIONS,
                succeeded("legacy.double_it", "legacy.broken_fn"),
                "DESCRIBE FUNCTION EXTENDED `legacy`.`double_it`",
                succeeded(describeLines.toArray(new String[0])),
                "DESCRIBE FUNCTION EXTENDED `legacy`.`broken_fn`",
                failed("[ROUTINE_NOT_FOUND] The routine `legacy`.`broken_fn` cannot be found.")));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreFunctionDdlsTask(cache).doRun(null, sink, handle));

    String expectedDdl =
        DatabricksHiveMetastoreParser.parseDescribeFunctionExtended(
                DatabricksFilter.HIVE_METASTORE_CATALOG, SCHEMA, "double_it", describeLines)
            .ddl;
    assertEquals(1, records.size());
    assertEquals("double_it", records.get(0).get(FunctionDdlsHeader.FunctionName));
    assertEquals(expectedDdl, records.get(0).get(FunctionDdlsHeader.Ddl));
  }

  @Test
  public void schemataTask_keepsSchemaWhoseDescribeFails() throws Exception {
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA, "locked"),
                "DESCRIBE SCHEMA EXTENDED `legacy`",
                succeededRows(
                    ImmutableList.of(
                        ImmutableList.of("Comment", "Old data"),
                        ImmutableList.of("Owner", "alice"))),
                "DESCRIBE SCHEMA EXTENDED `locked`",
                failed("[INSUFFICIENT_PERMISSIONS] Insufficient privileges.")));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreSchemataTask(cache).doRun(null, sink, handle));

    assertEquals(2, records.size());
    assertEquals("Old data", records.get(0).get(SchemataHeader.Comment));
    assertEquals("alice", records.get(0).get(SchemataHeader.Owner));
    assertEquals("locked", records.get(1).get(SchemataHeader.SchemaName));
    assertEquals("", records.get(1).get(SchemataHeader.Owner));
  }

  @Test
  public void sessionCache_doesNotRepeatFailedScan() throws Exception {
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS, failed("[INSUFFICIENT_PERMISSIONS] Insufficient privileges.")));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    try (DatabricksHandle handle = newHandle(statements)) {
      assertThrows(IOException.class, () -> cache.listParsedTables(handle));
      IOException repeated =
          assertThrows(IOException.class, () -> cache.listParsedFunctions(handle));
      assertTrue(repeated.getMessage().contains("failed earlier"));
    }

    verify(statements, times(1)).executeStatement(any(ExecuteStatementRequest.class));
  }

  private interface TaskBody {

    void run(ByteSink sink, DatabricksHandle handle) throws Exception;
  }

  private List<CSVRecord> runTask(StatementExecutionAPI statements, TaskBody body)
      throws Exception {
    File output = tempFolder.newFile();
    try (DatabricksHandle handle = newHandle(statements)) {
      body.run(Files.asByteSink(output), handle);
    }
    String csv = Files.asCharSource(output, UTF_8).read();
    try (CSVParser parser =
        AbstractTask.FORMAT
            .builder()
            .setHeader()
            .setSkipHeaderRecord(true)
            .get()
            .parse(new StringReader(csv))) {
      return parser.getRecords();
    }
  }

  private static DatabricksHandle newHandle(StatementExecutionAPI statements) {
    WorkspaceClient client = mock(WorkspaceClient.class);
    when(client.statementExecution()).thenReturn(statements);
    DatabricksSqlHelper sqlHelper =
        new DatabricksSqlHelper(client, "wh-1", new DatabricksChunkDownloader(), 0L);
    return new DatabricksHandle(sqlHelper, new DatabricksFilter(ImmutableList.of()));
  }

  /** Mocks the Statement Execution API to answer each SQL statement from the map. */
  private static StatementExecutionAPI mockStatements(
      Map<String, StatementResponse> responsesBySql) {
    StatementExecutionAPI statements = mock(StatementExecutionAPI.class);
    when(statements.executeStatement(any(ExecuteStatementRequest.class)))
        .thenAnswer(
            invocation -> {
              ExecuteStatementRequest request = invocation.getArgument(0);
              StatementResponse response = responsesBySql.get(request.getStatement());
              if (response == null) {
                throw new IllegalArgumentException("Unexpected SQL: " + request.getStatement());
              }
              return response;
            });
    return statements;
  }

  private static StatementResponse showTablesResponse(String... tableNames) {
    List<List<String>> tableRows = new ArrayList<>();
    for (String tableName : tableNames) {
      String information =
          "Catalog: hive_metastore\n"
              + "Database: legacy\n"
              + "Table: "
              + tableName
              + "\n"
              + "Type: MANAGED\n"
              + "Provider: delta\n"
              + "Schema: root\n"
              + " |-- id: integer (nullable = false)\n";
      tableRows.add(ImmutableList.of(SCHEMA, tableName, "false", information));
    }
    return succeededRows(tableRows);
  }

  private static StatementResponse succeeded(String... singleColumnRows) {
    List<List<String>> tableRows = new ArrayList<>();
    for (String value : singleColumnRows) {
      tableRows.add(ImmutableList.of(value));
    }
    return succeededRows(tableRows);
  }

  private static StatementResponse succeededRows(List<? extends List<String>> tableRows) {
    return new StatementResponse()
        .setStatementId("stmt-ok")
        .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
        .setResult(new ResultData().setDataArray(rows(tableRows)));
  }

  private static Collection<Collection<String>> rows(List<? extends List<String>> tableRows) {
    return new ArrayList<Collection<String>>(tableRows);
  }

  private static StatementResponse failed(String message) {
    return new StatementResponse()
        .setStatementId("stmt-failed")
        .setStatus(
            new StatementStatus()
                .setState(StatementState.FAILED)
                .setError(new ServiceError().setMessage(message)));
  }
}
