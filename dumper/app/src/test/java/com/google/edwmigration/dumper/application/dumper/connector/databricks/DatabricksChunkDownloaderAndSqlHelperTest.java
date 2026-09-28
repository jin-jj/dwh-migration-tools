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
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksFilter.HIVE_METASTORE_CATALOG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.ExternalLink;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;
import com.databricks.sdk.service.sql.ServiceError;
import com.databricks.sdk.service.sql.ServiceErrorCode;
import com.databricks.sdk.service.sql.StatementExecutionAPI;
import com.databricks.sdk.service.sql.StatementResponse;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import com.google.common.collect.ImmutableMap;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksChunkDownloaderAndSqlHelperTest {

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
    try (CloseableIterable<List<String>> rows =
        helper.executeSqlInCatalog(HIVE_METASTORE_CATALOG, "SELECT 1")) {
      for (List<String> row : rows) {
        streamed.add(row);
      }
    }
    assertEquals(2, streamed.size());
    assertEquals("row0_col0", streamed.get(0).get(0));
    assertEquals("row1_col0", streamed.get(1).get(0));
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
            () -> helper.executeSqlInCatalog(HIVE_METASTORE_CATALOG, "SELECT * FROM secret"));
    assertEquals(
        String.format(
            "SQL statement stmt-denied failed in state %s (SQLSTATE: %s, errorCode: %s): %s",
            StatementState.FAILED, sqlState, ServiceErrorCode.BAD_REQUEST.name(), errorMessage),
        ex.getMessage());
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
    try (CloseableIterable<List<String>> iter =
        helper.executeSqlInCatalog(HIVE_METASTORE_CATALOG, "SELECT 1")) {
      for (List<String> row : iter) {
        rows.add(row);
      }
    }

    assertEquals(1, rows.size());
    assertEquals(Collections.singletonList("refreshed_row"), rows.get(0));
  }
}
