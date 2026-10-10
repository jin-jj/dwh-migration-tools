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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.request;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestStubs.error;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestStubs.newWorkspaceClient;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestStubs.okModel;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.abbreviateStatement;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksError;
import com.databricks.sdk.service.sql.CancelExecutionRequest;
import com.databricks.sdk.service.sql.ColumnInfo;
import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.ExternalLink;
import com.databricks.sdk.service.sql.QueryTag;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;
import com.databricks.sdk.service.sql.ResultSchema;
import com.databricks.sdk.service.sql.ServiceError;
import com.databricks.sdk.service.sql.ServiceErrorCode;
import com.databricks.sdk.service.sql.StatementExecutionAPI;
import com.databricks.sdk.service.sql.StatementResponse;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class DatabricksChunkDownloaderAndSqlHelperTest {

  private static final String CATALOG = "hive_metastore";

  private static final String STATEMENTS_PATH = "/api/2.0/sql/statements";

  private WireMockServer wireMock;

  @Before
  public void setUp() {
    wireMock = new WireMockServer(wireMockConfig().dynamicPort());
    wireMock.start();
  }

  @After
  public void tearDown() {
    if (wireMock != null) {
      wireMock.stop();
    }
  }

  @Test
  public void chunkDownloader_retriesOn429WithRetryAfterAndPreservesNullVsStringNull()
      throws Exception {
    wireMock.stubFor(
        get(urlEqualTo("/chunk/0"))
            .inScenario("retry-chunk")
            .whenScenarioStateIs(Scenario.STARTED)
            .willReturn(aResponse().withStatus(429).withHeader("Retry-After", "2"))
            .willSetStateTo("SECOND_TRY"));

    wireMock.stubFor(
        get(urlEqualTo("/chunk/0"))
            .inScenario("retry-chunk")
            .whenScenarioStateIs("SECOND_TRY")
            .willReturn(
                aResponse()
                    .withStatus(503)
                    .withHeader("Retry-After", "Fri, 18 Sep 2026 19:00:03 GMT"))
            .willSetStateTo("SUCCESS"));

    wireMock.stubFor(
        get(urlEqualTo("/chunk/0"))
            .inScenario("retry-chunk")
            .whenScenarioStateIs("SUCCESS")
            .withHeader("X-Custom-Cloud-Header", equalTo("sig-token"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("[[\"cat1\", null, \"null\"], [\"cat2\", \"comment\", \"owner\"]]")));

    List<Long> recordedSleeps = new ArrayList<>();
    Clock fixedClock = Clock.fixed(Instant.parse("2026-09-18T19:00:00Z"), ZoneOffset.UTC);
    DatabricksChunkDownloader downloader =
        new DatabricksChunkDownloader(5, 100L, 1000L, fixedClock, recordedSleeps::add);

    ExternalLink link =
        new ExternalLink()
            .setExternalLink(wireMock.baseUrl() + "/chunk/0")
            .setHttpHeaders(ImmutableMap.of("X-Custom-Cloud-Header", "sig-token"));

    List<List<String>> rows = downloader.downloadChunkRows(link);
    assertEquals(2, rows.size());
    assertEquals("cat1", rows.get(0).get(0));
    assertNull(rows.get(0).get(1));
    assertEquals("null", rows.get(0).get(2));
    assertEquals(2, recordedSleeps.size());
    assertEquals(Long.valueOf(2000L), recordedSleeps.get(0));
    assertEquals(Long.valueOf(3000L), recordedSleeps.get(1));
    wireMock.verify(3, getRequestedFor(urlEqualTo("/chunk/0")));
  }

  @Test
  public void chunkDownloader_sendsLinkHeadersAndKeepsNullApartFromStringNull() throws Exception {
    wireMock.stubFor(
        get(urlEqualTo("/chunk/0"))
            .withHeader("X-Custom-Cloud-Header", equalTo("sig-token"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("[[\"cat1\", null, \"null\"], [\"cat2\", \"comment\", \"owner\"]]")));
    ExternalLink link =
        new ExternalLink()
            .setExternalLink(wireMock.baseUrl() + "/chunk/0")
            .setHttpHeaders(ImmutableMap.of("X-Custom-Cloud-Header", "sig-token"));

    List<List<String>> rows = new DatabricksChunkDownloader().downloadChunkRows(link);

    assertEquals(2, rows.size());
    assertEquals("cat1", rows.get(0).get(0));
    assertNull(rows.get(0).get(1));
    assertEquals("null", rows.get(0).get(2));
  }

  @Test
  public void chunkDownloader_failsOnHttpErrorWithoutUrlQueryInMessage() {
    wireMock.stubFor(get(urlPathEqualTo("/chunk/0")).willReturn(aResponse().withStatus(403)));
    String url = wireMock.baseUrl() + "/chunk/0";
    ExternalLink link = new ExternalLink().setExternalLink(url + "?X-Amz-Signature=sig-value");

    IOException thrown =
        assertThrows(
            IOException.class, () -> new DatabricksChunkDownloader().downloadChunkRows(link));

    assertEquals(
        "Failed to download external result chunk (HTTP 403) from " + url + "?<redacted>.",
        thrown.getMessage());
  }

  @Test
  public void sqlHelper_pollsPendingStatementAndStreamsMultipleExternalLinkChunks()
      throws Exception {
    wireMock.stubFor(
        get(urlEqualTo("/chunk/0"))
            .willReturn(aResponse().withStatus(200).withBody("[[\"row0_col0\", \"row0_col1\"]]")));
    wireMock.stubFor(
        get(urlEqualTo("/chunk/1"))
            .willReturn(aResponse().withStatus(200).withBody("[[\"row1_col0\", \"row1_col1\"]]")));

    WorkspaceClient client = mock(WorkspaceClient.class);
    StatementExecutionAPI stmtApi = mock(StatementExecutionAPI.class);
    when(client.statementExecution()).thenReturn(stmtApi);

    StatementResponse pendingResp =
        new StatementResponse()
            .setStatementId("stmt-100")
            .setStatus(new StatementStatus().setState(StatementState.PENDING));

    StatementResponse succeededResp =
        new StatementResponse()
            .setStatementId("stmt-100")
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
            .setManifest(new ResultManifest().setTotalChunkCount(2L))
            .setResult(
                new ResultData()
                    .setChunkIndex(0L)
                    .setNextChunkIndex(1L)
                    .setExternalLinks(
                        Collections.singletonList(
                            new ExternalLink()
                                .setChunkIndex(0L)
                                .setNextChunkIndex(1L)
                                .setExternalLink(wireMock.baseUrl() + "/chunk/0"))));

    ResultData chunk1Data =
        new ResultData()
            .setChunkIndex(1L)
            .setExternalLinks(
                Collections.singletonList(
                    new ExternalLink()
                        .setChunkIndex(1L)
                        .setExternalLink(wireMock.baseUrl() + "/chunk/1")));

    when(stmtApi.executeStatement(any(ExecuteStatementRequest.class))).thenReturn(pendingResp);
    when(stmtApi.getStatement("stmt-100")).thenReturn(succeededResp);
    when(stmtApi.getStatementResultChunkN("stmt-100", 1L)).thenReturn(chunk1Data);

    DatabricksSqlHelper helper =
        new DatabricksSqlHelper(client, "wh-1", new DatabricksChunkDownloader(), 0L);

    List<List<String>> streamed = new ArrayList<>();
    try (CloseableIterable<List<String>> rows = helper.executeSqlInCatalog(CATALOG, "SELECT 1")) {
      for (List<String> row : rows) {
        streamed.add(row);
      }
    }
    assertEquals(2, streamed.size());
    assertEquals("row0_col0", streamed.get(0).get(0));
    assertEquals("row1_col0", streamed.get(1).get(0));
  }

  @Test
  public void sqlHelper_cancelsStatementAndFailsWhenPollingTimesOut() {
    WorkspaceClient client = mock(WorkspaceClient.class);
    StatementExecutionAPI stmtApi = mock(StatementExecutionAPI.class);
    when(client.statementExecution()).thenReturn(stmtApi);

    StatementResponse runningResp =
        new StatementResponse()
            .setStatementId("stmt-200")
            .setStatus(new StatementStatus().setState(StatementState.RUNNING));

    when(stmtApi.executeStatement(any(ExecuteStatementRequest.class))).thenReturn(runningResp);

    DatabricksSqlHelper helper =
        new DatabricksSqlHelper(client, "wh-1", new DatabricksChunkDownloader(), 0L, Duration.ZERO);

    DatabricksSqlException thrown =
        assertThrows(
            DatabricksSqlException.class, () -> helper.executeSqlInCatalog(CATALOG, "SELECT 1"));

    assertTrue(thrown.getMessage().contains("Timed out waiting for SQL statement stmt-200"));
    verify(stmtApi).cancelExecution(any(CancelExecutionRequest.class));
  }

  @Test
  public void sqlHelper_failedStatement_throwsDatabricksSqlExceptionWithSqlStateInMessage() {
    WorkspaceClient client = mock(WorkspaceClient.class);
    StatementExecutionAPI stmtApi = mock(StatementExecutionAPI.class);
    when(client.statementExecution()).thenReturn(stmtApi);

    String sqlState = "42501";
    String errorMessage = "User does not have USE CATALOG on Catalog 'secret'";
    StatementResponse failedResp =
        new StatementResponse()
            .setStatementId("stmt-denied")
            .setStatus(
                new StatementStatus()
                    .setState(StatementState.FAILED)
                    .setSqlState(sqlState)
                    .setError(
                        new ServiceError()
                            .setErrorCode(ServiceErrorCode.BAD_REQUEST)
                            .setMessage(errorMessage)));

    when(stmtApi.executeStatement(any(ExecuteStatementRequest.class))).thenReturn(failedResp);

    DatabricksSqlHelper helper =
        new DatabricksSqlHelper(client, "wh-1", new DatabricksChunkDownloader(), 0L);

    DatabricksSqlException ex =
        assertThrows(
            DatabricksSqlException.class,
            () -> helper.executeSqlInCatalog(CATALOG, "SELECT * FROM secret"));
    assertEquals(
        String.format(
            "SQL statement stmt-denied failed in state %s (SQLSTATE: %s, errorCode: %s): %s",
            StatementState.FAILED, sqlState, ServiceErrorCode.BAD_REQUEST.name(), errorMessage),
        ex.getMessage());
  }

  @Test
  public void sqlHelper_returnsCellsOfFirstRowByColumnName() throws Exception {
    WorkspaceClient client = mock(WorkspaceClient.class);
    StatementExecutionAPI stmtApi = mock(StatementExecutionAPI.class);
    when(client.statementExecution()).thenReturn(stmtApi);
    String partitionColumns = "[\"region\",\"dt\"]";
    when(stmtApi.executeStatement(any(ExecuteStatementRequest.class)))
        .thenReturn(
            new StatementResponse()
                .setStatementId("stmt-detail")
                .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
                .setManifest(
                    new ResultManifest()
                        .setSchema(
                            new ResultSchema()
                                .setColumns(
                                    ImmutableList.of(
                                        new ColumnInfo().setName("format").setPosition(0L),
                                        new ColumnInfo().setName("description").setPosition(1L),
                                        new ColumnInfo()
                                            .setName("partitionColumns")
                                            .setPosition(2L)))))
                .setResult(
                    new ResultData()
                        .setDataArray(
                            ImmutableList.<Collection<String>>of(
                                Arrays.asList("delta", null, partitionColumns),
                                Arrays.asList("parquet", "second row", "[]")))));
    DatabricksSqlHelper helper =
        new DatabricksSqlHelper(client, "wh-1", new DatabricksChunkDownloader(), 0L);

    assertEquals(
        ImmutableMap.of("format", "delta", "partitionColumns", partitionColumns),
        helper.executeSqlInCatalogForFirstRow(CATALOG, "DESCRIBE DETAIL t"));
  }

  @Test
  public void sqlHelper_returnsEmptyMapWithoutFirstRow() throws Exception {
    WorkspaceClient client = mock(WorkspaceClient.class);
    StatementExecutionAPI stmtApi = mock(StatementExecutionAPI.class);
    when(client.statementExecution()).thenReturn(stmtApi);
    when(stmtApi.executeStatement(any(ExecuteStatementRequest.class)))
        .thenReturn(
            new StatementResponse()
                .setStatementId("stmt-empty")
                .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED)));
    DatabricksSqlHelper helper =
        new DatabricksSqlHelper(client, "wh-1", new DatabricksChunkDownloader(), 0L);

    assertEquals(
        ImmutableMap.of(), helper.executeSqlInCatalogForFirstRow(CATALOG, "DESCRIBE DETAIL t"));
  }

  @Test
  public void sqlHelper_tagsEachStatementAsAStatementOfTheDumper() throws Exception {
    WorkspaceClient client = mock(WorkspaceClient.class);
    StatementExecutionAPI stmtApi = mock(StatementExecutionAPI.class);
    when(client.statementExecution()).thenReturn(stmtApi);
    when(stmtApi.executeStatement(any(ExecuteStatementRequest.class)))
        .thenReturn(
            new StatementResponse()
                .setStatementId("stmt-tagged")
                .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED)));
    DatabricksSqlHelper helper =
        new DatabricksSqlHelper(client, "wh-1", new DatabricksChunkDownloader(), 0L);

    try (CloseableIterable<List<String>> rows = helper.executeSqlInCatalog(CATALOG, "SELECT 1")) {
      assertFalse(rows.iterator().hasNext());
    }

    ArgumentCaptor<ExecuteStatementRequest> request =
        ArgumentCaptor.forClass(ExecuteStatementRequest.class);
    verify(stmtApi).executeStatement(request.capture());
    QueryTag tag = getOnlyElement(request.getValue().getQueryTags());
    assertEquals(DatabricksSqlHelper.QUERY_TAG_KEY, tag.getKey());
    assertEquals(DatabricksSqlHelper.QUERY_TAG_VALUE, tag.getValue());
  }

  @Test
  public void sqlHelper_refreshesExpiredExternalLinkBeforeDownload() throws Exception {
    wireMock.stubFor(
        get(urlEqualTo("/fresh-chunk-0.json"))
            .willReturn(aResponse().withStatus(200).withBody("[[\"refreshed_row\"]]")));

    WorkspaceClient client = mock(WorkspaceClient.class);
    StatementExecutionAPI stmtApi = mock(StatementExecutionAPI.class);
    when(client.statementExecution()).thenReturn(stmtApi);

    ResultData expiredChunk =
        new ResultData()
            .setChunkIndex(0L)
            .setExternalLinks(
                Collections.singletonList(
                    new ExternalLink()
                        .setChunkIndex(0L)
                        .setExpiration("2020-01-01T00:00:00Z")
                        .setExternalLink(wireMock.baseUrl() + "/expired-chunk-0.json")));

    ResultData refreshedChunk =
        new ResultData()
            .setChunkIndex(0L)
            .setExternalLinks(
                Collections.singletonList(
                    new ExternalLink()
                        .setChunkIndex(0L)
                        .setExpiration("2099-01-01T00:00:00Z")
                        .setExternalLink(wireMock.baseUrl() + "/fresh-chunk-0.json")));

    StatementResponse resp =
        new StatementResponse()
            .setStatementId("stmt-expired")
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
            .setManifest(new ResultManifest().setTotalChunkCount(1L))
            .setResult(expiredChunk);

    when(stmtApi.executeStatement(any(ExecuteStatementRequest.class))).thenReturn(resp);
    when(stmtApi.getStatementResultChunkN("stmt-expired", 0L)).thenReturn(refreshedChunk);

    DatabricksSqlHelper helper =
        new DatabricksSqlHelper(client, "wh-1", new DatabricksChunkDownloader(), 0L);

    List<List<String>> rows = new ArrayList<>();
    try (CloseableIterable<List<String>> iter = helper.executeSqlInCatalog(CATALOG, "SELECT 1")) {
      for (List<String> row : iter) {
        rows.add(row);
      }
    }

    assertEquals(1, rows.size());
    assertEquals(Collections.singletonList("refreshed_row"), rows.get(0));
  }

  @Test
  public void sqlHelper_resubmitsAStatementWhoseSubmissionFailsWithAnInternalError()
      throws Exception {
    stubFailureThenSuccess(
        "POST", STATEMENTS_PATH, internalError(), okModel(succeeded("stmt-1", "first_schema")));
    List<Long> sleeps = new ArrayList<>();

    ImmutableList<List<String>> rows = readRows(newSqlHelper(sleeps), "SHOW SCHEMAS");

    assertEquals(ImmutableList.of(ImmutableList.of("first_schema")), rows);
    assertEquals(1, sleeps.size());
    wireMock.verify(2, postRequestedFor(urlPathEqualTo(STATEMENTS_PATH)));
  }

  @Test
  public void sqlHelper_pollsAgainWhenPollingFailsWithAnInternalError() throws Exception {
    String statementId = "stmt-2";
    String statementPath = STATEMENTS_PATH + "/" + statementId;
    wireMock.stubFor(
        post(urlPathEqualTo(STATEMENTS_PATH))
            .willReturn(
                okModel(
                    new StatementResponse()
                        .setStatementId(statementId)
                        .setStatus(new StatementStatus().setState(StatementState.PENDING)))));
    stubFailureThenSuccess(
        "GET", statementPath, internalError(), okModel(succeeded(statementId, "first_schema")));
    List<Long> sleeps = new ArrayList<>();

    ImmutableList<List<String>> rows = readRows(newSqlHelper(sleeps), "SHOW SCHEMAS");

    assertEquals(ImmutableList.of(ImmutableList.of("first_schema")), rows);
    assertEquals(1, sleeps.size());
    // The SDK doesn't retry this HTTP 500 itself, so each attempt sent one request.
    wireMock.verify(2, getRequestedFor(urlPathEqualTo(statementPath)));
  }

  @Test
  public void sqlHelper_fetchesAChunkAgainWhenFetchingFailsWithAnInternalError() throws Exception {
    String statementId = "stmt-3";
    String chunkPath = STATEMENTS_PATH + "/" + statementId + "/result/chunks/1";
    StatementResponse firstChunk = succeeded(statementId, "first_schema");
    firstChunk.getManifest().setTotalChunkCount(2L);
    firstChunk.getResult().setNextChunkIndex(1L);
    wireMock.stubFor(post(urlPathEqualTo(STATEMENTS_PATH)).willReturn(okModel(firstChunk)));
    stubFailureThenSuccess(
        "GET",
        chunkPath,
        internalError(),
        okModel(new ResultData().setChunkIndex(1L).setDataArray(singleCellRows("second_schema"))));
    List<Long> sleeps = new ArrayList<>();

    ImmutableList<List<String>> rows = readRows(newSqlHelper(sleeps), "SHOW SCHEMAS");

    assertEquals(
        ImmutableList.of(ImmutableList.of("first_schema"), ImmutableList.of("second_schema")),
        rows);
    assertEquals(1, sleeps.size());
    wireMock.verify(2, getRequestedFor(urlPathEqualTo(chunkPath)));
  }

  @Test
  public void sqlHelper_cancelsStatementWhenInterruptedWhileWaitingToPollAgain() {
    WorkspaceClient client = mock(WorkspaceClient.class);
    StatementExecutionAPI stmtApi = mock(StatementExecutionAPI.class);
    when(client.statementExecution()).thenReturn(stmtApi);
    String statementId = "stmt-interrupted";
    when(stmtApi.executeStatement(any(ExecuteStatementRequest.class)))
        .thenReturn(
            new StatementResponse()
                .setStatementId(statementId)
                .setStatus(new StatementStatus().setState(StatementState.RUNNING)));
    when(stmtApi.getStatement(statementId))
        .thenThrow(new DatabricksError("INTERNAL_ERROR", "The request failed.", 500));
    DatabricksSqlHelper helper =
        new DatabricksSqlHelper(
            client,
            "wh-1",
            new DatabricksChunkDownloader(),
            0L,
            Duration.ofMinutes(1),
            millis -> {
              throw new InterruptedException("Interrupted by the test.");
            });

    assertThrows(
        InterruptedIOException.class, () -> helper.executeSqlInCatalog(CATALOG, "SELECT 1"));

    assertTrue(Thread.interrupted());
    ArgumentCaptor<CancelExecutionRequest> cancel =
        ArgumentCaptor.forClass(CancelExecutionRequest.class);
    verify(stmtApi).cancelExecution(cancel.capture());
    assertEquals(statementId, cancel.getValue().getStatementId());
  }

  @Test
  public void sqlHelper_doesNotResubmitARejectedStatement() {
    wireMock.stubFor(
        post(urlPathEqualTo(STATEMENTS_PATH))
            .willReturn(error(400, "INVALID_PARAMETER_VALUE", "No such warehouse.")));
    List<Long> sleeps = new ArrayList<>();
    DatabricksSqlHelper helper = newSqlHelper(sleeps);

    DatabricksSqlException thrown =
        assertThrows(DatabricksSqlException.class, () -> readRows(helper, "SHOW SCHEMAS"));

    assertTrue(thrown.getMessage(), thrown.getMessage().contains("No such warehouse."));
    assertEquals(ImmutableList.of(), sleeps);
    wireMock.verify(1, postRequestedFor(urlPathEqualTo(STATEMENTS_PATH)));
  }

  @Test
  public void abbreviateStatement_putsTheStatementOnOneLineAndCutsItOff() {
    String longStatement = "SELECT\n  " + Strings.repeat("column_name,\n  ", 20) + "id\nFROM t";
    String longLine = "SELECT " + Strings.repeat("column_name, ", 20) + "id FROM t";

    String abbreviated = abbreviateStatement(longStatement);

    assertEquals(
        "DESCRIBE TABLE EXTENDED `db`.`t` AS JSON",
        abbreviateStatement("  DESCRIBE TABLE EXTENDED\n  `db`.`t`\tAS JSON\n"));
    assertTrue(abbreviated, abbreviated.endsWith("..."));
    String kept = abbreviated.substring(0, abbreviated.length() - "...".length());
    assertTrue(abbreviated, longLine.startsWith(kept) && kept.length() < longLine.length());
  }

  @Test
  public void quoteIdentifier_doublesBackticks() {
    assertEquals("`legacy`", DatabricksSqlHelper.quoteIdentifier("legacy"));
    assertEquals("`odd``name`", DatabricksSqlHelper.quoteIdentifier("odd`name"));
  }

  @Test
  public void quoteIdentifierIfNeeded_quotesOnlyIdentifiersThatAreNotPlainWords() {
    assertEquals("sales_2024", DatabricksSqlHelper.quoteIdentifierIfNeeded("sales_2024"));
    assertEquals("_1", DatabricksSqlHelper.quoteIdentifierIfNeeded("_1"));
    assertEquals("`2024`", DatabricksSqlHelper.quoteIdentifierIfNeeded("2024"));
    assertEquals("`my table`", DatabricksSqlHelper.quoteIdentifierIfNeeded("my table"));
    assertEquals("`a-b`", DatabricksSqlHelper.quoteIdentifierIfNeeded("a-b"));
    assertEquals("`odd``name`", DatabricksSqlHelper.quoteIdentifierIfNeeded("odd`name"));
    assertEquals("``", DatabricksSqlHelper.quoteIdentifierIfNeeded(""));
  }

  @Test
  public void quoteStringLiteral_escapesBackslashesAndSingleQuotes() {
    assertEquals("'plain'", DatabricksSqlHelper.quoteStringLiteral("plain"));
    assertEquals("'it\\'s C:\\\\tmp'", DatabricksSqlHelper.quoteStringLiteral("it's C:\\tmp"));
  }

  /** Returns a helper that sends its requests to the WireMock server, and records its retries. */
  private DatabricksSqlHelper newSqlHelper(List<Long> sleeps) {
    return new DatabricksSqlHelper(
        newWorkspaceClient(wireMock.baseUrl()),
        "wh-1",
        new DatabricksChunkDownloader(),
        0L,
        Duration.ofMinutes(1),
        sleeps::add);
  }

  /** Makes the first request to a path fail with {@code failure}, and the next ones succeed. */
  private void stubFailureThenSuccess(
      String method,
      String path,
      ResponseDefinitionBuilder failure,
      ResponseDefinitionBuilder success) {
    String failedOnce = "failed once";
    wireMock.stubFor(
        request(method, urlPathEqualTo(path))
            .inScenario(path)
            .whenScenarioStateIs(Scenario.STARTED)
            .willReturn(failure)
            .willSetStateTo(failedOnce));
    wireMock.stubFor(
        request(method, urlPathEqualTo(path))
            .inScenario(path)
            .whenScenarioStateIs(failedOnce)
            .willReturn(success));
  }

  /** Returns the HTTP 500 that Databricks sent during an outage. */
  private static ResponseDefinitionBuilder internalError() {
    return error(500, "INTERNAL_ERROR", "The request failed due to an unexpected condition.");
  }

  /** Returns a succeeded statement whose first chunk has one row with one cell. */
  private static StatementResponse succeeded(String statementId, String cell) {
    return new StatementResponse()
        .setStatementId(statementId)
        .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
        .setManifest(new ResultManifest().setTotalChunkCount(1L))
        .setResult(new ResultData().setChunkIndex(0L).setDataArray(singleCellRows(cell)));
  }

  private static Collection<Collection<String>> singleCellRows(String cell) {
    return ImmutableList.<Collection<String>>of(ImmutableList.of(cell));
  }

  private static ImmutableList<List<String>> readRows(DatabricksSqlHelper helper, String sql)
      throws IOException {
    ImmutableList.Builder<List<String>> rows = ImmutableList.builder();
    try (CloseableIterable<List<String>> iterable = helper.executeSqlInCatalog(CATALOG, sql)) {
      for (List<String> row : iterable) {
        rows.add(row);
      }
    }
    return rows.build();
  }
}
