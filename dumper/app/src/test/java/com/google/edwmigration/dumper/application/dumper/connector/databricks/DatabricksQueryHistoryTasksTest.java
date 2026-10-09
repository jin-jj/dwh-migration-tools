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

import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksLogsConnector.DatabricksLogsProperty.WHERE;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksQueryHistoryTasks.REDACTED_STATEMENT_TEXT;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestStubs.QUERY_HISTORY_PATH;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestStubs.UNTHROTTLED_REQUESTS_PER_SECOND;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestStubs.newWorkspaceClient;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestStubs.okModel;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.QUERY_TAG_KEY;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.QUERY_TAG_VALUE;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.quoteStringLiteral;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.failed;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.readRecords;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.succeededRows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.sql.ListQueriesResponse;
import com.databricks.sdk.service.sql.QueryInfo;
import com.databricks.sdk.service.sql.QueryMetrics;
import com.databricks.sdk.service.sql.QueryStatementType;
import com.databricks.sdk.service.sql.QueryStatus;
import com.databricks.sdk.service.sql.QueryTag;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksLogsConnector.Strategy;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksLogsDumpFormat.Header;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksQueryHistoryTasks.DatabricksQueryHistoryTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksQueryHistoryTasks.QueryHistorySession;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import org.apache.commons.csv.CSVRecord;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksQueryHistoryTasksTest {

  private static final String STATEMENTS_PATH = "/api/2.0/sql/statements";
  private static final String WORKSPACE = "1234567890123456";
  private static final ZonedInterval INTERVAL =
      new ZonedInterval(
          ZonedDateTime.parse("2026-10-09T13:00:00Z"), ZonedDateTime.parse("2026-10-09T14:00:00Z"));
  private static final long START_TIME_MILLIS = millis(INTERVAL.getStart()) + 1_000L;
  private static final long END_TIME_MILLIS = START_TIME_MILLIS + 2_500L;

  private static WireMockServer server;

  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  @BeforeClass
  public static void startServer() {
    server = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
    server.start();
  }

  @AfterClass
  public static void stopServer() {
    server.stop();
  }

  @Before
  public void resetServer() {
    server.resetAll();
  }

  @Test
  public void systemTable_writesTheQueriesOfTheIntervalWithInstantTimes() throws Exception {
    stubProbe(okModel(succeededRows(ImmutableList.of(systemTableRow("probe", "SELECT 1")))));
    stubIntervalQuery(
        INTERVAL,
        systemTableRow("stmt-1", "SELECT * FROM sales.orders"),
        systemTableRow("stmt-2", "SELECT 2"));

    List<CSVRecord> records =
        runTask(newHandle(ImmutableList.of(WORKSPACE)), session(Strategy.SYSTEM_THEN_REST));

    assertEquals(ImmutableList.of("stmt-1", "stmt-2"), column(records, Header.StatementId));
    CSVRecord first = records.get(0);
    assertEquals(WORKSPACE, first.get(Header.WorkspaceId));
    assertEquals("SELECT * FROM sales.orders", first.get(Header.StatementText));
    assertEquals(Instant.ofEpochMilli(START_TIME_MILLIS).toString(), first.get(Header.StartTime));
    assertEquals(Instant.ofEpochMilli(END_TIME_MILLIS).toString(), first.get(Header.EndTime));
    server.verify(
        2,
        postRequestedFor(urlPathEqualTo(STATEMENTS_PATH))
            .withRequestBody(
                matchingJsonPath(
                    "$.statement",
                    containing("workspace_id IN (" + quoteStringLiteral(WORKSPACE) + ")")))
            .withRequestBody(
                matchingJsonPath("$.statement", containing(quoteStringLiteral(QUERY_TAG_VALUE)))));
    server.verify(0, getRequestedFor(urlPathEqualTo(QUERY_HISTORY_PATH)));
  }

  @Test
  public void redactedStatementText_readsTheQueryHistoryApiInstead() throws Exception {
    stubProbe(
        okModel(succeededRows(ImmutableList.of(systemTableRow("probe", REDACTED_STATEMENT_TEXT)))));
    stubQueryHistory(apiQuery("q-1", "SELECT * FROM sales.orders"));

    List<CSVRecord> records =
        runTask(newHandle(ImmutableList.of(WORKSPACE)), session(Strategy.SYSTEM_THEN_REST));

    assertEquals(ImmutableList.of("q-1"), column(records, Header.StatementId));
    assertEquals("SELECT * FROM sales.orders", records.get(0).get(Header.StatementText));
    server.verify(1, postRequestedFor(urlPathEqualTo(STATEMENTS_PATH)));
  }

  @Test
  public void unreadableSystemTable_readsTheQueryHistoryApiInstead() throws Exception {
    stubProbe(okModel(failed("[INSUFFICIENT_PERMISSIONS] User does not have USE SCHEMA.")));
    stubQueryHistory(apiQuery("q-1", "SELECT 1"));

    List<CSVRecord> records =
        runTask(newHandle(ImmutableList.of(WORKSPACE)), session(Strategy.SYSTEM_THEN_REST));

    assertEquals(ImmutableList.of("q-1"), column(records, Header.StatementId));
  }

  @Test
  public void unreadableSystemTable_failsIfTheApiDoesNotHaveTheWorkspaces() {
    stubProbe(okModel(failed("[INSUFFICIENT_PERMISSIONS] User does not have USE SCHEMA.")));

    IOException exception =
        assertThrows(
            IOException.class,
            () -> runTask(newHandle(ImmutableList.of()), session(Strategy.SYSTEM_THEN_REST)));

    assertTrue(exception.getMessage(), exception.getMessage().contains("INSUFFICIENT_PERMISSIONS"));
    assertTrue(exception.getMessage(), exception.getMessage().contains("every workspace"));
    server.verify(0, getRequestedFor(urlPathEqualTo(QUERY_HISTORY_PATH)));
  }

  @Test
  public void unreadableSystemTable_failsIfTheApiCannotApplyTheCondition() {
    stubProbe(okModel(failed("[INSUFFICIENT_PERMISSIONS] User does not have USE SCHEMA.")));
    QueryHistorySession session =
        new QueryHistorySession(
            Strategy.SYSTEM_THEN_REST, null, "execution_status = 'FINISHED'", INTERVAL);

    IOException exception =
        assertThrows(
            IOException.class, () -> runTask(newHandle(ImmutableList.of(WORKSPACE)), session));

    assertTrue(exception.getMessage(), exception.getMessage().contains(WHERE.getName()));
    server.verify(0, getRequestedFor(urlPathEqualTo(QUERY_HISTORY_PATH)));
  }

  @Test
  public void systemOnly_dumpsTheRedactedStatementText() throws Exception {
    stubProbe(
        okModel(succeededRows(ImmutableList.of(systemTableRow("probe", REDACTED_STATEMENT_TEXT)))));
    stubIntervalQuery(INTERVAL, systemTableRow("stmt-1", REDACTED_STATEMENT_TEXT));

    List<CSVRecord> records =
        runTask(newHandle(ImmutableList.of(WORKSPACE)), session(Strategy.SYSTEM_ONLY));

    assertEquals(REDACTED_STATEMENT_TEXT, getOnlyElement(records).get(Header.StatementText));
    server.verify(0, getRequestedFor(urlPathEqualTo(QUERY_HISTORY_PATH)));
  }

  @Test
  public void restOnly_readsOnlyTheQueryHistoryApi() throws Exception {
    stubQueryHistory(apiQuery("q-1", "SELECT 1"));

    List<CSVRecord> records = runTask(newRestOnlyHandle(), session(Strategy.REST_ONLY));

    assertEquals(ImmutableList.of("q-1"), column(records, Header.StatementId));
    server.verify(0, postRequestedFor(urlPathEqualTo(STATEMENTS_PATH)));
  }

  @Test
  public void queryHistoryApi_writesTheMetricsAndSkipsTheStatementsOfTheDumper() throws Exception {
    QueryMetrics metrics =
        new QueryMetrics()
            .setTotalTimeMs(END_TIME_MILLIS - START_TIME_MILLIS)
            .setCompilationTimeMs(100L)
            .setExecutionTimeMs(2_300L)
            .setReadBytes(1_024L)
            .setRowsReadCount(10L)
            .setRowsProducedCount(1L)
            .setWriteRemoteBytes(512L)
            .setSpillToDiskBytes(64L);
    QueryInfo query =
        apiQuery("q-1", "INSERT INTO sales.orders SELECT * FROM staging.orders")
            .setUserName("user@example.com")
            .setExecutedAsUserName("service-principal")
            .setStatementType(QueryStatementType.INSERT)
            .setWarehouseId("wh-1")
            .setClientApplication("Databricks SQL Editor")
            .setMetrics(metrics);
    QueryInfo dumperQuery =
        apiQuery("q-dumper", "SHOW SCHEMAS IN hive_metastore")
            .setQueryTags(
                ImmutableList.of(new QueryTag().setKey(QUERY_TAG_KEY).setValue(QUERY_TAG_VALUE)));
    stubQueryHistory(query, dumperQuery);

    CSVRecord record = getOnlyElement(runTask(newRestOnlyHandle(), session(Strategy.REST_ONLY)));

    assertEquals("", record.get(Header.AccountId));
    assertEquals(WORKSPACE, record.get(Header.WorkspaceId));
    assertEquals(query.getQueryId(), record.get(Header.StatementId));
    assertEquals(query.getUserName(), record.get(Header.ExecutedBy));
    assertEquals(query.getQueryText(), record.get(Header.StatementText));
    assertEquals(QueryStatementType.INSERT.name(), record.get(Header.StatementType));
    assertEquals(QueryStatus.FINISHED.name(), record.get(Header.ExecutionStatus));
    assertEquals(query.getWarehouseId(), record.get(Header.WarehouseId));
    assertEquals("WAREHOUSE", record.get(Header.ComputeType));
    assertEquals(Instant.ofEpochMilli(START_TIME_MILLIS).toString(), record.get(Header.StartTime));
    assertEquals(Instant.ofEpochMilli(END_TIME_MILLIS).toString(), record.get(Header.EndTime));
    assertEquals(String.valueOf(metrics.getTotalTimeMs()), record.get(Header.TotalDurationMs));
    assertEquals(
        String.valueOf(metrics.getCompilationTimeMs()), record.get(Header.CompilationDurationMs));
    assertEquals(
        String.valueOf(metrics.getExecutionTimeMs()), record.get(Header.ExecutionDurationMs));
    assertEquals("", record.get(Header.WaitingAtCapacityDurationMs));
    assertEquals(String.valueOf(metrics.getReadBytes()), record.get(Header.ReadBytes));
    assertEquals(String.valueOf(metrics.getRowsReadCount()), record.get(Header.ReadRows));
    assertEquals(String.valueOf(metrics.getRowsProducedCount()), record.get(Header.ProducedRows));
    assertEquals(String.valueOf(metrics.getWriteRemoteBytes()), record.get(Header.WrittenBytes));
    assertEquals("", record.get(Header.WrittenRows));
    assertEquals(
        String.valueOf(metrics.getSpillToDiskBytes()), record.get(Header.SpilledLocalBytes));
    assertEquals(query.getClientApplication(), record.get(Header.ClientApplication));
  }

  @Test
  public void source_isChosenOnceForAllTheTasks() throws Exception {
    ZonedInterval nextInterval =
        new ZonedInterval(INTERVAL.getEndExclusive(), INTERVAL.getEndExclusive().plusHours(1));
    QueryHistorySession session =
        new QueryHistorySession(Strategy.SYSTEM_THEN_REST, null, null, INTERVAL.span(nextInterval));
    stubProbe(okModel(succeededRows(ImmutableList.of(systemTableRow("probe", "SELECT 1")))));
    stubIntervalQuery(INTERVAL, systemTableRow("stmt-1", "SELECT 1"));
    stubIntervalQuery(nextInterval, systemTableRow("stmt-2", "SELECT 2"));
    DatabricksLogsHandle handle = newHandle(ImmutableList.of(WORKSPACE));

    List<CSVRecord> firstRecords = runTask(handle, session, INTERVAL);
    List<CSVRecord> nextRecords = runTask(handle, session, nextInterval);

    assertEquals(ImmutableList.of("stmt-1"), column(firstRecords, Header.StatementId));
    assertEquals(ImmutableList.of("stmt-2"), column(nextRecords, Header.StatementId));
    server.verify(
        1,
        postRequestedFor(urlPathEqualTo(STATEMENTS_PATH))
            .withRequestBody(matchingJsonPath("$.statement", containing("LIMIT 1"))));
  }

  @Test
  public void customQuery_runsWithoutProbeAndKeepsTimesThatAreNotNumbers() throws Exception {
    String query = "SELECT * FROM main.ops.query_log WHERE start_time >= '%s' AND end < '%s'";
    String startTime = "2026-10-09 13:00:01";
    server.stubFor(
        post(urlPathEqualTo(STATEMENTS_PATH))
            .withRequestBody(
                matchingJsonPath(
                    "$.statement",
                    equalTo(DatabricksQueryHistoryTasks.customQuery(query, INTERVAL))))
            .willReturn(
                okModel(
                    succeededRows(
                        ImmutableList.of(
                            systemTableRow("stmt-1", "SELECT 1", startTime, startTime))))));

    List<CSVRecord> records =
        runTask(
            newHandle(ImmutableList.of(WORKSPACE)),
            new QueryHistorySession(Strategy.SYSTEM_THEN_REST, query, null, INTERVAL));

    assertEquals(startTime, getOnlyElement(records).get(Header.StartTime));
    server.verify(1, postRequestedFor(urlPathEqualTo(STATEMENTS_PATH)));
  }

  @Test
  public void customQuery_replacesThePlaceholdersWithTheStartAndEndOfTheInterval() {
    String query = "SELECT * FROM t WHERE start_time >= '%s' AND start_time < '%s'";

    assertEquals(
        "SELECT * FROM t WHERE start_time >= '"
            + INTERVAL.getStart().toInstant()
            + "' AND start_time < '"
            + INTERVAL.getEndExclusive().toInstant()
            + "'",
        DatabricksQueryHistoryTasks.customQuery(query, INTERVAL));
  }

  @Test
  public void systemTableQuery_selectsByStartTimeAndAddsTheConditionInParentheses() {
    String condition = "execution_status = 'FINISHED' OR statement_type = 'SELECT'";

    String sql =
        DatabricksQueryHistoryTasks.systemTableQuery(INTERVAL, ImmutableList.of(), condition);

    assertTrue(
        sql, sql.contains("start_time >= timestamp_millis(" + millis(INTERVAL.getStart()) + ")"));
    assertTrue(
        sql,
        sql.contains("start_time < timestamp_millis(" + millis(INTERVAL.getEndExclusive()) + ")"));
    assertTrue(sql, sql.endsWith(" AND (" + condition + ")"));
    assertFalse(sql, sql.contains("workspace_id IN"));
  }

  private List<CSVRecord> runTask(DatabricksLogsHandle handle, QueryHistorySession session)
      throws Exception {
    return runTask(handle, session, INTERVAL);
  }

  private List<CSVRecord> runTask(
      DatabricksLogsHandle handle, QueryHistorySession session, ZonedInterval interval)
      throws Exception {
    File output = tempFolder.newFile();
    new DatabricksQueryHistoryTask(output.getName(), interval, session)
        .doRun(null, Files.asByteSink(output), handle);
    return readRecords(output);
  }

  private static QueryHistorySession session(Strategy strategy) {
    return new QueryHistorySession(strategy, null, null, INTERVAL);
  }

  /** Returns a handle that reads system.query.history of the workspaces, or of every workspace. */
  private static DatabricksLogsHandle newHandle(ImmutableList<String> workspaceIds) {
    WorkspaceClient client = newWorkspaceClient(server.baseUrl());
    DatabricksSqlHelper sqlHelper =
        new DatabricksSqlHelper(client, "wh-1", new DatabricksChunkDownloader(), 0L);
    return new DatabricksLogsHandle(sqlHelper, newRestHelper(client), WORKSPACE, workspaceIds);
  }

  /** Returns a handle without a SQL warehouse, as for -Ddatabricks.logs.strategy=rest-only. */
  private static DatabricksLogsHandle newRestOnlyHandle() {
    WorkspaceClient client = newWorkspaceClient(server.baseUrl());
    return new DatabricksLogsHandle(null, newRestHelper(client), WORKSPACE, ImmutableList.of());
  }

  private static DatabricksRestHelper newRestHelper(WorkspaceClient client) {
    return new DatabricksRestHelper(client, UNTHROTTLED_REQUESTS_PER_SECOND, millis -> {});
  }

  /**
   * Stubs the query that tells whether system.query.history can be read and has the statement text.
   * It takes precedence over the stubs of the intervals, whose times it may also have.
   */
  private static void stubProbe(ResponseDefinitionBuilder response) {
    server.stubFor(
        post(urlPathEqualTo(STATEMENTS_PATH))
            .atPriority(1)
            .withRequestBody(matchingJsonPath("$.statement", containing("LIMIT 1")))
            .willReturn(response));
  }

  /** Stubs the query of system.query.history for the queries that started in the interval. */
  @SafeVarargs
  private static void stubIntervalQuery(ZonedInterval interval, List<String>... rows) {
    server.stubFor(
        post(urlPathEqualTo(STATEMENTS_PATH))
            .atPriority(2)
            .withRequestBody(
                matchingJsonPath(
                    "$.statement",
                    containing("start_time >= timestamp_millis(" + millis(interval.getStart()))))
            .withRequestBody(
                matchingJsonPath(
                    "$.statement",
                    containing(
                        "start_time < timestamp_millis(" + millis(interval.getEndExclusive()))))
            .willReturn(okModel(succeededRows(ImmutableList.copyOf(rows)))));
  }

  private static void stubQueryHistory(QueryInfo... queries) {
    server.stubFor(
        get(urlPathEqualTo(QUERY_HISTORY_PATH))
            .willReturn(
                okModel(
                    new ListQueriesResponse()
                        .setRes(ImmutableList.copyOf(queries))
                        .setHasNextPage(false))));
  }

  private static QueryInfo apiQuery(String queryId, String queryText) {
    return new QueryInfo()
        .setQueryId(queryId)
        .setQueryText(queryText)
        .setStatus(QueryStatus.FINISHED)
        .setQueryStartTimeMs(START_TIME_MILLIS)
        .setQueryEndTimeMs(END_TIME_MILLIS);
  }

  /** Returns a row of system.query.history, with the columns of the query history files. */
  private static ImmutableList<String> systemTableRow(String statementId, String statementText) {
    return systemTableRow(
        statementId,
        statementText,
        String.valueOf(START_TIME_MILLIS),
        String.valueOf(END_TIME_MILLIS));
  }

  private static ImmutableList<String> systemTableRow(
      String statementId, String statementText, String startTime, String endTime) {
    return ImmutableList.of(
        "account-1",
        WORKSPACE,
        statementId,
        "session-1",
        "user@example.com",
        statementText,
        "SELECT",
        "FINISHED",
        "",
        "wh-1",
        "WAREHOUSE",
        startTime,
        endTime,
        "2500",
        "100",
        "2300",
        "0",
        "1024",
        "10",
        "1",
        "0",
        "0",
        "0",
        "Databricks SQL Editor");
  }

  private static ImmutableList<String> column(List<CSVRecord> records, Header header) {
    ImmutableList.Builder<String> values = ImmutableList.builder();
    for (CSVRecord record : records) {
      values.add(record.get(header));
    }
    return values.build();
  }

  private static long millis(ZonedDateTime dateTime) {
    return dateTime.toInstant().toEpochMilli();
  }
}
