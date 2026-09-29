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
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.sql.CancelExecutionRequest;
import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.ExternalLink;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;
import com.databricks.sdk.service.sql.StatementExecutionAPI;
import com.databricks.sdk.service.sql.StatementResponse;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Duration;
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
    try (CloseableIterable<List<String>> rows =
        helper.executeSqlInCatalog("hive_metastore", "SELECT 1")) {
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
        new DatabricksSqlHelper(
            client, "wh-1", new DatabricksChunkDownloader(), 0L, Duration.ZERO);

    DatabricksSqlException thrown =
        assertThrows(
            DatabricksSqlException.class,
            () -> helper.executeSqlInCatalog("hive_metastore", "SELECT 1"));

    assertTrue(thrown.getMessage().contains("Timed out waiting for SQL statement stmt-200"));
    verify(stmtApi).cancelExecution(any(CancelExecutionRequest.class));
  }
}
