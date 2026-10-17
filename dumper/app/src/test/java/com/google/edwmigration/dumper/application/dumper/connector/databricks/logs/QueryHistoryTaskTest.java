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
package com.google.edwmigration.dumper.application.dumper.connector.databricks.logs;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.logs.QueryHistoryTask.DatabricksLogsHandle;
import com.google.edwmigration.dumper.application.dumper.task.MemoryByteSink;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class QueryHistoryTaskTest {

  private static final String STATEMENTS_PATH = "/api/2.0/sql/statements";
  private static final String WORKSPACE = "1234567890123456";
  private static final ZonedInterval INTERVAL =
      new ZonedInterval(
          ZonedDateTime.parse("2026-10-09T13:00:00Z"), ZonedDateTime.parse("2026-10-09T14:00:00Z"));
  private static final long START_TIME_MILLIS = millis(INTERVAL.getStart()) + 1_000L;
  private static final long END_TIME_MILLIS = START_TIME_MILLIS + 2_500L;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static WireMockServer server;

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
  public void systemTable_writesExpectedCsv() throws Exception {
    stubIntervalSucceeded(
        INTERVAL,
        ImmutableList.of(
            systemTableRow("stmt-1", "SELECT * FROM sales.orders"),
            systemTableRow("stmt-2", "SELECT 2")));
    String actualCsv =
        runTask(new QueryHistoryTask("query_history_system.csv", INTERVAL), newSystemTableHandle());
    assertMatchesExpectedCsv("query_history_system.csv", actualCsv);
  }

  @Test
  public void queryHistoryApi_writesExpectedCsvFromRecordedFixtures() throws Exception {
    JsonNode root =
        MAPPER.readTree(
            Resources.toString(
                Resources.getResource("connector/databricks/logs-responses.json"), UTF_8));
    for (Map.Entry<String, JsonNode> entry : root.properties()) {
      server.stubFor(
          get(urlEqualTo(entry.getKey())).willReturn(okJson(entry.getValue().toString())));
    }
    String actualCsv =
        runTask(new QueryHistoryTask("query_history_rest.csv", INTERVAL), newApiHandle());
    // The fixture URLs pin the requests. The API returns the queries that started at end_time_ms
    // too, so endExclusive - 1 is sent.
    assertMatchesExpectedCsv("query_history_rest.csv", actualCsv);
  }

  @Test
  public void systemTableQuery_selectsTheHeaderColumnsOfTheIntervalAndWorkspaces() {
    assertEquals(
        "SELECT account_id, workspace_id, statement_id, session_id, executed_by, statement_text,"
            + " statement_type, execution_status, error_message, compute.warehouse_id,"
            + " compute.type, unix_millis(start_time), unix_millis(end_time), total_duration_ms,"
            + " compilation_duration_ms, execution_duration_ms, waiting_at_capacity_duration_ms,"
            + " read_bytes, read_rows, produced_rows, written_bytes, written_rows,"
            + " spilled_local_bytes, client_application FROM system.query.history"
            + " WHERE start_time >= timestamp_millis("
            + millis(INTERVAL.getStart())
            + ") AND start_time < timestamp_millis("
            + millis(INTERVAL.getEndExclusive())
            + ") AND COALESCE(try_element_at(query_tags, 'source'), '') <> 'dwh-migration-dumper'"
            + " AND workspace_id IN ('111', '222')",
        QueryHistoryTask.systemTableQuery(INTERVAL, ImmutableList.of("111", "222")));
  }

  private static String runTask(QueryHistoryTask task, DatabricksLogsHandle handle)
      throws Exception {
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(null, sink, handle);
    return sink.openStream().toString();
  }

  private static void assertMatchesExpectedCsv(String entryName, String actualCsv)
      throws IOException {
    String expectedCsv =
        Resources.toString(
            Resources.getResource("connector/databricks/expected/" + entryName), UTF_8);
    assertEquals(entryName, expectedCsv.replace("\r\n", "\n"), actualCsv.replace("\r\n", "\n"));
  }

  private static DatabricksLogsHandle newSystemTableHandle() {
    WorkspaceClient client = newWorkspaceClient();
    return new DatabricksLogsHandle(
        new DatabricksSqlHelper(client, "wh-1"), client, WORKSPACE, ImmutableList.of(WORKSPACE));
  }

  private static DatabricksLogsHandle newApiHandle() {
    return new DatabricksLogsHandle(null, newWorkspaceClient(), WORKSPACE, ImmutableList.of());
  }

  private static WorkspaceClient newWorkspaceClient() {
    return new WorkspaceClient(
        new DatabricksConfig().setHost(server.baseUrl()).setToken("test-token").setAuthType("pat"));
  }

  private static void stubIntervalSucceeded(ZonedInterval interval, Object rows)
      throws IOException {
    String chunkPath = "/chunk/interval";
    server.stubFor(get(urlEqualTo(chunkPath)).willReturn(okJson(MAPPER.writeValueAsString(rows))));
    server.stubFor(
        post(urlPathEqualTo(STATEMENTS_PATH))
            .withRequestBody(
                matchingJsonPath(
                    "$.statement",
                    equalTo(
                        QueryHistoryTask.systemTableQuery(interval, ImmutableList.of(WORKSPACE)))))
            .willReturn(
                okJson(
                    String.format(
                        "{\"statement_id\": \"stmt-ok\", \"status\": {\"state\": \"SUCCEEDED\"},"
                            + " \"manifest\": {\"total_chunk_count\": 1},"
                            + " \"result\": {\"external_links\": [{\"external_link\": \"%s\"}]}}",
                        server.baseUrl() + chunkPath))));
  }

  private static ImmutableList<String> systemTableRow(String statementId, String statementText) {
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
        String.valueOf(START_TIME_MILLIS),
        String.valueOf(END_TIME_MILLIS),
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

  private static long millis(ZonedDateTime dateTime) {
    return dateTime.toInstant().toEpochMilli();
  }
}
