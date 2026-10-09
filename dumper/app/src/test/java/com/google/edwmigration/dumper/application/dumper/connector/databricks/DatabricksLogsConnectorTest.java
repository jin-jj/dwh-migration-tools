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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.UserAgent;
import com.databricks.sdk.service.sql.ListQueriesResponse;
import com.databricks.sdk.service.sql.ListQueryHistoryRequest;
import com.databricks.sdk.service.sql.QueryHistoryAPI;
import com.databricks.sdk.service.sql.QueryHistoryService;
import com.databricks.sdk.service.sql.QueryInfo;
import com.databricks.sdk.service.sql.QueryMetrics;
import com.databricks.sdk.service.sql.QueryStatementType;
import com.databricks.sdk.service.sql.QueryStatus;
import com.databricks.sdk.service.sql.QueryTag;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.StatementExecutionAPI;
import com.databricks.sdk.service.sql.StatementResponse;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.LogsConnector;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.MemoryByteSink;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.time.Clock;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksLogsConnectorTest {

  private final DatabricksLogsConnector connector = new DatabricksLogsConnector();

  private WorkspaceClient client;
  private StatementExecutionAPI statementAPI;
  private QueryHistoryService queryHistoryService;
  private DatabricksHandle handle;
  private TaskRunContext context;

  @Before
  public void setUp() {
    client = mock(WorkspaceClient.class);
    statementAPI = mock(StatementExecutionAPI.class);
    when(client.statementExecution()).thenReturn(statementAPI);

    queryHistoryService = mock(QueryHistoryService.class);
    QueryHistoryAPI queryHistoryAPI = mock(QueryHistoryAPI.class);
    when(queryHistoryAPI.impl()).thenReturn(queryHistoryService);
    when(client.queryHistory()).thenReturn(queryHistoryAPI);

    handle = new DatabricksHandle(client, "warehouse123");
    context = mock(TaskRunContext.class);
  }

  private void mockSqlQuery(String expectedSqlSubstring, List<List<String>> rows) {
    StatementResponse response = new StatementResponse();
    response.setStatementId("stmt-" + Math.abs(expectedSqlSubstring.hashCode()));
    response.setStatus(new StatementStatus().setState(StatementState.SUCCEEDED));
    ResultData resultData = new ResultData();
    List<Collection<String>> collRows = new ArrayList<>();
    for (List<String> row : rows) {
      collRows.add(row);
    }
    resultData.setDataArray(collRows);
    response.setResult(resultData);

    when(statementAPI.executeStatement(
            org.mockito.ArgumentMatchers.argThat(
                req ->
                    req != null
                        && req.getStatement() != null
                        && req.getStatement().contains(expectedSqlSubstring))))
        .thenReturn(response);
  }

  @Test
  public void serviceLoader_findsConnector() {
    boolean found = false;
    for (Connector c : ServiceLoader.load(Connector.class)) {
      if ("databricks-logs".equals(c.getName())) {
        found = true;
        assertTrue(c instanceof LogsConnector);
        break;
      }
    }
    assertTrue("DatabricksLogsConnector should be discoverable via ServiceLoader", found);
  }

  @Test
  public void getName_returnsDatabricksLogs() {
    assertEquals("databricks-logs", connector.getName());
  }

  @Test
  public void getDefaultFileName_returnsDatabricksLogsZip() {
    String filename = connector.getDefaultFileName(false, Clock.systemUTC());
    assertEquals("dwh-migration-databricks-logs.zip", filename);
  }

  @Test
  public void validate_assessmentFlag_throwsException() {
    ConnectorArguments arguments =
        ConnectorArguments.create(
            ImmutableList.of(
                "--connector",
                "databricks-logs",
                "--url",
                "https://test.databricks.com",
                "--warehouse",
                "w123",
                "--assessment"));

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> connector.validate(arguments));
    assertTrue(exception.getMessage().contains("--assessment is not supported"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void validate_missingUri_throwsException() {
    ConnectorArguments arguments =
        ConnectorArguments.create(
            ImmutableList.of("--connector", "databricks-logs", "--warehouse", "w123"));
    connector.validate(arguments);
  }

  @Test(expected = IllegalArgumentException.class)
  public void validate_missingWarehouse_throwsException() {
    ConnectorArguments arguments =
        ConnectorArguments.create(
            ImmutableList.of(
                "--connector", "databricks-logs", "--url", "https://test.databricks.com"));
    connector.validate(arguments);
  }

  @Test
  public void validate_validArguments_success() {
    ConnectorArguments arguments =
        ConnectorArguments.create(
            ImmutableList.of(
                "--connector",
                "databricks-logs",
                "--url",
                "https://test.databricks.com",
                "--warehouse",
                "w123"));
    connector.validate(arguments);
  }

  @Test
  public void buildQueryTemplate_defaultQuery_containsExpectedColumnsAndTable() {
    ConnectorArguments arguments =
        ConnectorArguments.create(
            ImmutableList.of(
                "--connector",
                "databricks-logs",
                "--url",
                "https://test.databricks.com",
                "--warehouse",
                "w123"));
    String template = DatabricksLogsConnector.buildQueryTemplate(arguments);
    assertTrue(template.contains("FROM system.query.history"));
    assertTrue(template.contains("statement_id"));
    assertTrue(template.contains("statement_text"));
    assertTrue(template.contains("client_application"));
    assertTrue(template.contains("end_time >= to_timestamp_ltz('%s')"));
    assertTrue(template.contains("end_time <= to_timestamp_ltz('%s')"));
    assertTrue(template.contains("AND COALESCE(client_application, '') <> 'dwh-migration-dumper'"));
    assertTrue(
        template.contains("AND COALESCE(query_tags['source'], '') <> 'dwh-migration-dumper'"));
  }

  @Test
  public void buildQueryTemplate_withWhereOverride_appendsClause() {
    ConnectorArguments arguments =
        ConnectorArguments.create(
            ImmutableList.of(
                "--connector",
                "databricks-logs",
                "--url",
                "https://test.databricks.com",
                "--warehouse",
                "w123",
                "-Ddatabricks.logs.where=execution_status = 'FINISHED'"));
    String template = DatabricksLogsConnector.buildQueryTemplate(arguments);
    assertTrue(template.contains("AND execution_status = 'FINISHED'"));
  }

  @Test
  public void buildQueryTemplate_withQueryOverride_returnsCustomQuery() {
    String customQuery =
        "SELECT statement_id, statement_text FROM system.query.history WHERE end_time >= '%s' AND"
            + " end_time <= '%s'";
    ConnectorArguments arguments =
        ConnectorArguments.create(
            ImmutableList.of(
                "--connector",
                "databricks-logs",
                "--url",
                "https://test.databricks.com",
                "--warehouse",
                "w123",
                "-Ddatabricks.logs.query=" + customQuery));
    String template = DatabricksLogsConnector.buildQueryTemplate(arguments);
    assertEquals(customQuery, template);
  }

  @Test
  public void buildQueryTemplate_withBadQueryOverride_throwsException() {
    String badQuery = "SELECT statement_id FROM system.query.history";
    ConnectorArguments arguments =
        ConnectorArguments.create(
            ImmutableList.of(
                "--connector",
                "databricks-logs",
                "--url",
                "https://test.databricks.com",
                "--warehouse",
                "w123",
                "-Ddatabricks.logs.query=" + badQuery));
    assertThrows(
        MetadataDumperUsageException.class,
        () -> DatabricksLogsConnector.buildQueryTemplate(arguments));
  }

  @Test
  public void addTasksTo_createsSetupAndIntervalTasks() {
    ConnectorArguments arguments =
        ConnectorArguments.create(
            ImmutableList.of(
                "--connector",
                "databricks-logs",
                "--url",
                "https://test.databricks.com",
                "--warehouse",
                "w123",
                "--query-log-start",
                "2026-03-01 00:00:00",
                "--query-log-end",
                "2026-03-01 06:00:00"));

    List<Task<?>> tasks = new ArrayList<>();
    connector.addTasksTo(tasks, arguments);

    // 1 DumpMetadataTask + 1 FormatTask + 6 hourly intervals
    assertEquals(8, tasks.size());
    assertTrue(tasks.get(0) instanceof DumpMetadataTask);
    assertTrue(tasks.get(1) instanceof FormatTask);
    for (int i = 2; i < tasks.size(); i++) {
      Task<?> task = tasks.get(i);
      assertTrue(task instanceof DatabricksQueryHistoryTask);
      assertTrue(task.getTargetPath().startsWith("query_history_"));
      assertTrue(task.getTargetPath().endsWith(".csv"));
    }
  }

  private static List<String> readLines(MemoryByteSink sink) {
    String content = sink.openStream().toString();
    if (content.isEmpty()) {
      return java.util.Collections.emptyList();
    }
    return Arrays.asList(content.split("\\r?\\n"));
  }

  @Test
  public void queryHistoryTask_writesCsvWithHeadersAndRows() throws Exception {
    mockSqlQuery(
        "system.query.history",
        ImmutableList.of(
            Arrays.asList(
                "acc_123",
                "ws_456",
                "stmt_789",
                "sess_001",
                "user@example.com",
                "SELECT 1",
                "SELECT",
                "FINISHED",
                null,
                "wh_main",
                "WAREHOUSE",
                "2026-03-01T12:00:00Z",
                "2026-03-01T12:00:01Z",
                "1000",
                "100",
                "900",
                "0",
                "1024",
                "10",
                "512",
                "5",
                "0")));

    DatabricksQueryHistoryTask task =
        new DatabricksQueryHistoryTask(
            "query_history_test.csv", "SELECT * FROM system.query.history");
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertTrue(lines.size() >= 2);
    String headerLine = lines.get(0);
    assertTrue(headerLine.contains("AccountId"));
    assertTrue(headerLine.contains("StatementId"));
    assertTrue(headerLine.contains("StatementText"));
    assertTrue(headerLine.contains("SpilledLocalBytes"));

    String dataLine = lines.get(1);
    assertTrue(dataLine.contains("acc_123"));
    assertTrue(dataLine.contains("stmt_789"));
    assertTrue(dataLine.contains("SELECT 1"));
    assertTrue(dataLine.contains("FINISHED"));
    assertTrue(dataLine.contains("wh_main"));
  }

  @Test
  public void queryHistoryTask_withRedactedQueryText_writesRedactedWithoutThrowing()
      throws Exception {
    mockSqlQuery(
        "system.query.history",
        ImmutableList.of(
            Arrays.asList(
                "acc_123",
                "ws_456",
                "stmt_999",
                "sess_002",
                "user@example.com",
                "<REDACTED>",
                "SELECT",
                "FINISHED",
                null,
                "wh_main",
                "WAREHOUSE",
                "2026-03-01T12:00:00Z",
                "2026-03-01T12:00:01Z",
                "1000",
                "100",
                "900",
                "0",
                "0",
                "0",
                "0",
                "0",
                "0")));

    DatabricksQueryHistoryTask task =
        new DatabricksQueryHistoryTask(
            "query_history_redacted.csv", "SELECT * FROM system.query.history");
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(2, lines.size());
    assertTrue(lines.get(1).contains("<REDACTED>"));
  }

  @Test
  public void validate_restOnlyWithoutWarehouse_succeeds() {
    ConnectorArguments arguments =
        ConnectorArguments.create(
            ImmutableList.of(
                "--connector",
                "databricks-logs",
                "--url",
                "https://test.databricks.com",
                "-Ddatabricks.logs.strategy=rest-only"));
    connector.validate(arguments);
  }

  @Test
  public void buildQueryTemplate_withWorkspaceIdFlag_appendsWorkspaceFilter() {
    ConnectorArguments arguments =
        ConnectorArguments.create(
            ImmutableList.of(
                "--connector",
                "databricks-logs",
                "--url",
                "https://test.databricks.com",
                "--warehouse",
                "w123",
                "--workspace-id",
                "2319810376257260"));
    String template = DatabricksLogsConnector.buildQueryTemplate(arguments);
    assertTrue(template.contains("AND workspace_id = '2319810376257260'"));
  }

  @Test
  public void buildQueryTemplate_withMultipleWorkspaceIds_appendsInClause() {
    ConnectorArguments arguments =
        ConnectorArguments.create(
            ImmutableList.of(
                "--connector",
                "databricks-logs",
                "--url",
                "https://test.databricks.com",
                "--warehouse",
                "w123",
                "--workspace-id",
                "111, 222"));
    String template = DatabricksLogsConnector.buildQueryTemplate(arguments);
    assertTrue(template.contains("AND workspace_id IN ('111', '222')"));
  }

  @Test
  public void buildQueryTemplate_withWorkspaceIdProperty_appendsWorkspaceFilter() {
    ConnectorArguments arguments =
        ConnectorArguments.create(
            ImmutableList.of(
                "--connector",
                "databricks-logs",
                "--url",
                "https://test.databricks.com",
                "--warehouse",
                "w123",
                "-Ddatabricks.logs.workspace-id=999888777"));
    String template = DatabricksLogsConnector.buildQueryTemplate(arguments);
    assertTrue(template.contains("AND workspace_id = '999888777'"));
  }

  @Test
  public void resolveRestWorkspaceId_extractsFromAzureUrl() {
    ConnectorArguments arguments =
        ConnectorArguments.create(
            ImmutableList.of(
                "--connector",
                "databricks-logs",
                "--url",
                "https://adb-2319810376257260.0.azuredatabricks.net/sql/1.0/warehouses/51e6d279e66d1fbb",
                "--warehouse",
                "w123"));
    assertEquals("2319810376257260", DatabricksLogsConnector.resolveRestWorkspaceId(arguments));
  }

  @Test
  public void queryHistoryTask_whenSystemCatalogFails_fallsBackToRestApi() throws Exception {
    when(statementAPI.executeStatement(any()))
        .thenThrow(
            new DatabricksException(
                "[INSUFFICIENT_PERMISSIONS] Insufficient privileges: User does not have USE"
                    + " CATALOG on Catalog 'system'. SQLSTATE: 42501"));

    QueryInfo restQuery =
        new QueryInfo()
            .setQueryId("rest-q-101")
            .setExecutedAsUserName("alice@example.com")
            .setQueryText("SELECT * FROM sales.orders WHERE id = 42")
            .setStatementType(QueryStatementType.SELECT)
            .setStatus(QueryStatus.FINISHED)
            .setWarehouseId("wh_main")
            .setQueryStartTimeMs(1772366400000L)
            .setQueryEndTimeMs(1772366401500L)
            .setMetrics(
                new QueryMetrics()
                    .setTotalTimeMs(1500L)
                    .setCompilationTimeMs(200L)
                    .setExecutionTimeMs(1300L)
                    .setReadBytes(4096L)
                    .setRowsReadCount(50L)
                    .setWriteRemoteBytes(0L)
                    .setRowsProducedCount(10L)
                    .setSpillToDiskBytes(0L));

    when(queryHistoryService.list(any(ListQueryHistoryRequest.class)))
        .thenReturn(
            new ListQueriesResponse().setRes(ImmutableList.of(restQuery)).setHasNextPage(false));

    ZonedInterval interval =
        new ZonedInterval(
            ZonedDateTime.of(2026, 3, 1, 12, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2026, 3, 1, 13, 0, 0, 0, ZoneOffset.UTC));

    DatabricksQueryHistoryTask task =
        new DatabricksQueryHistoryTask(
            "query_history_fallback_perm.csv",
            "SELECT * FROM system.query.history",
            interval,
            "2319810376257260",
            true);
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    assertTrue(handle.isSystemQueryHistoryUnavailable());
    List<String> lines = readLines(sink);
    assertEquals(2, lines.size());
    assertTrue(lines.get(1).contains("2319810376257260"));
    assertTrue(lines.get(1).contains("rest-q-101"));
    assertTrue(lines.get(1).contains("SELECT * FROM sales.orders WHERE id = 42"));
  }

  @Test
  public void
      queryHistoryTask_whenStatementTextIsRedacted_fallsBackToRestApiAndWritesUnredactedText()
          throws Exception {
    mockSqlQuery(
        "system.query.history",
        ImmutableList.of(
            Arrays.asList(
                "acc_123",
                "2319810376257260",
                "stmt_999",
                "sess_002",
                "user@example.com",
                "<REDACTED>",
                "SELECT",
                "FINISHED",
                null,
                "wh_main",
                "WAREHOUSE",
                "2026-03-01T12:00:00Z",
                "2026-03-01T12:00:01Z",
                "1000",
                "100",
                "900",
                "0",
                "0",
                "0",
                "0",
                "0",
                "0")));

    QueryInfo restQuery =
        new QueryInfo()
            .setQueryId("stmt_999")
            .setExecutedAsUserName("user@example.com")
            .setQueryText("SELECT customer_id, total FROM default.invoices")
            .setStatementType(QueryStatementType.SELECT)
            .setStatus(QueryStatus.FINISHED)
            .setWarehouseId("wh_main")
            .setQueryStartTimeMs(1772366400000L)
            .setQueryEndTimeMs(1772366401000L)
            .setDuration(1000L);

    when(queryHistoryService.list(any(ListQueryHistoryRequest.class)))
        .thenReturn(
            new ListQueriesResponse().setRes(ImmutableList.of(restQuery)).setHasNextPage(false));

    ZonedInterval interval =
        new ZonedInterval(
            ZonedDateTime.of(2026, 3, 1, 12, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2026, 3, 1, 13, 0, 0, 0, ZoneOffset.UTC));

    DatabricksQueryHistoryTask task1 =
        new DatabricksQueryHistoryTask(
            "query_history_1.csv",
            "SELECT * FROM system.query.history",
            interval,
            "2319810376257260",
            true);
    MemoryByteSink sink1 = new MemoryByteSink();
    task1.doRun(context, sink1, handle);

    List<String> lines1 = readLines(sink1);
    assertEquals(2, lines1.size());
    assertTrue(lines1.get(1).contains("SELECT customer_id, total FROM default.invoices"));
    assertTrue(!lines1.get(1).contains("<REDACTED>"));
    assertTrue(handle.isSystemQueryHistoryUnavailable());

    // Subsequent task should skip SQL execution altogether and call REST directly
    DatabricksQueryHistoryTask task2 =
        new DatabricksQueryHistoryTask(
            "query_history_2.csv",
            "SELECT * FROM system.query.history",
            interval,
            "2319810376257260",
            true);
    MemoryByteSink sink2 = new MemoryByteSink();
    task2.doRun(context, sink2, handle);

    // StatementExecutionAPI should have been called only once (by task1)
    verify(statementAPI, times(1)).executeStatement(any());
    verify(queryHistoryService, times(2)).list(any(ListQueryHistoryRequest.class));
  }

  @Test
  public void databricksHandle_registersDumperUserAgent() {
    DatabricksHandle.registerUserAgent();
    assertTrue(
        UserAgent.asString()
            .startsWith(
                DatabricksHandle.CLIENT_APPLICATION + "/" + DatabricksHandle.CLIENT_VERSION));
  }

  @Test
  public void queryHistoryTask_restFallback_excludesDumperSelfQueries() throws Exception {
    handle.markSystemQueryHistoryUnavailable();

    QueryInfo userQuery =
        new QueryInfo()
            .setQueryId("user-q-1")
            .setSessionId("sess-user-1")
            .setExecutedAsUserName("alice@example.com")
            .setQueryText("SELECT * FROM sales.orders")
            .setStatementType(QueryStatementType.SELECT)
            .setStatus(QueryStatus.FINISHED)
            .setWarehouseId("wh_main")
            .setClientApplication("Databricks SQL Editor")
            .setQueryStartTimeMs(1772366400000L)
            .setQueryEndTimeMs(1772366401000L)
            .setDuration(1000L);

    QueryInfo dumperClientAppQuery =
        new QueryInfo()
            .setQueryId("dumper-q-1")
            .setExecutedAsUserName("alice@example.com")
            .setQueryText("SELECT * FROM system.query.history")
            .setStatementType(QueryStatementType.SELECT)
            .setStatus(QueryStatus.FINISHED)
            .setWarehouseId("wh_main")
            .setClientApplication(DatabricksHandle.CLIENT_APPLICATION)
            .setQueryStartTimeMs(1772366402000L)
            .setQueryEndTimeMs(1772366403000L)
            .setDuration(1000L);

    QueryInfo dumperTaggedQuery =
        new QueryInfo()
            .setQueryId("dumper-q-2")
            .setExecutedAsUserName("alice@example.com")
            .setQueryText("DESCRIBE FUNCTION EXTENDED default.my_udf")
            .setStatementType(QueryStatementType.DESCRIBE)
            .setStatus(QueryStatus.FINISHED)
            .setWarehouseId("wh_main")
            .setClientApplication("unknown")
            .setQueryTags(
                ImmutableList.of(
                    new QueryTag()
                        .setKey(DatabricksHandle.QUERY_TAG_KEY)
                        .setValue(DatabricksHandle.CLIENT_APPLICATION)))
            .setQueryStartTimeMs(1772366404000L)
            .setQueryEndTimeMs(1772366405000L)
            .setDuration(1000L);

    when(queryHistoryService.list(any(ListQueryHistoryRequest.class)))
        .thenReturn(
            new ListQueriesResponse()
                .setRes(ImmutableList.of(userQuery, dumperClientAppQuery, dumperTaggedQuery))
                .setHasNextPage(false));

    ZonedInterval interval =
        new ZonedInterval(
            ZonedDateTime.of(2026, 3, 1, 12, 0, 0, 0, ZoneOffset.UTC),
            ZonedDateTime.of(2026, 3, 1, 13, 0, 0, 0, ZoneOffset.UTC));

    DatabricksQueryHistoryTask task =
        new DatabricksQueryHistoryTask(
            "query_history_filter_rest.csv",
            "SELECT * FROM system.query.history",
            interval,
            "2319810376257260",
            true);
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(2, lines.size());
    assertTrue(lines.get(0).contains("ClientApplication"));
    assertTrue(lines.get(1).contains("user-q-1"));
    assertTrue(lines.get(1).contains("sess-user-1"));
    assertTrue(lines.get(1).contains("Databricks SQL Editor"));
    assertFalse(lines.get(1).contains("dumper-q-1"));
    assertFalse(lines.get(1).contains("dumper-q-2"));
  }
}
