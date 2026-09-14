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
import static com.github.tomakehurst.wiremock.client.WireMock.exactly;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.ExternalLink;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.StatementExecutionAPI;
import com.databricks.sdk.service.sql.StatementResponse;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Covers the external-links result transport, where the rows are downloaded from a pre-signed URL
 * rather than embedded in the statement response.
 */
@RunWith(JUnit4.class)
public class DatabricksSqlHelperExternalLinksTest {

  private static final String CHUNK_PATH = "/results/chunk-0";
  private static final String DECRYPTION_HEADER = "x-amz-server-side-encryption-customer-key";
  private static final String RETRY_AFTER = "Retry-After";

  /** Mirrors the retry budget of the helper under test. */
  private static final int MAX_RETRIES = 5;

  private WireMockServer server;
  private StatementExecutionAPI statementApi;
  private DatabricksHandle handle;

  @Before
  public void setUp() {
    server = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
    server.start();
    WorkspaceClient client = mock(WorkspaceClient.class);
    statementApi = mock(StatementExecutionAPI.class);
    when(client.statementExecution()).thenReturn(statementApi);
    handle = new DatabricksHandle(client, "wh-1");
  }

  @After
  public void tearDown() {
    server.stop();
  }

  @Test
  public void executeBulkQueryOrThrow_readsRowsFromTheLink() throws Exception {
    server.stubFor(
        get(urlEqualTo(CHUNK_PATH))
            .willReturn(aResponse().withBody("[[\"cat\",\"schema\"],[\"cat\",\"other\"]]")));
    respondWithLink(new ExternalLink().setExternalLink(server.baseUrl() + CHUNK_PATH));

    assertEquals(
        Arrays.asList(Arrays.asList("cat", "schema"), Arrays.asList("cat", "other")), collect());
  }

  /** A SQL NULL arrives as a JSON null and must not become the four-character string "null". */
  @Test
  public void executeBulkQueryOrThrow_keepsNullDistinctFromItsSpelling() throws Exception {
    server.stubFor(
        get(urlEqualTo(CHUNK_PATH))
            .willReturn(aResponse().withBody("[[\"a\",null,\"null\",\"\"]]")));
    respondWithLink(new ExternalLink().setExternalLink(server.baseUrl() + CHUNK_PATH));

    assertEquals(Collections.singletonList(Arrays.asList("a", null, "null", "")), collect());
  }

  @Test
  public void executeBulkQueryOrThrow_decompressesAGzippedChunk() throws Exception {
    server.stubFor(
        get(urlEqualTo(CHUNK_PATH)).willReturn(aResponse().withBody(gzip("[[\"compressed\"]]"))));
    respondWithLink(new ExternalLink().setExternalLink(server.baseUrl() + CHUNK_PATH));

    assertEquals(Collections.singletonList(Collections.singletonList("compressed")), collect());
  }

  /** The headers the API attaches to a link carry its decryption key, so they must be sent. */
  @Test
  public void executeBulkQueryOrThrow_sendsTheHeadersOfTheLink() throws Exception {
    server.stubFor(get(urlEqualTo(CHUNK_PATH)).willReturn(aResponse().withBody("[[\"x\"]]")));
    respondWithLink(
        new ExternalLink()
            .setExternalLink(server.baseUrl() + CHUNK_PATH)
            .setHttpHeaders(Collections.singletonMap(DECRYPTION_HEADER, "secret")));

    collect();

    server.verify(
        getRequestedFor(urlEqualTo(CHUNK_PATH)).withHeader(DECRYPTION_HEADER, equalTo("secret")));
  }

  /** A rejected link will not start working, so the retry budget must not be spent on it. */
  @Test
  public void executeBulkQueryOrThrow_whenTheLinkHasExpired_throws() throws Exception {
    server.stubFor(get(urlEqualTo(CHUNK_PATH)).willReturn(aResponse().withStatus(403)));
    respondWithLink(new ExternalLink().setExternalLink(server.baseUrl() + CHUNK_PATH));

    SQLException thrown = assertThrows(SQLException.class, this::collect);
    assertEquals("Failed to read Databricks result chunk from external link", thrown.getMessage());
    server.verify(exactly(1), getRequestedFor(urlEqualTo(CHUNK_PATH)));
  }

  @Test
  public void executeBulkQueryOrThrow_followsTheChunkChain() throws Exception {
    String secondPath = "/results/chunk-1";
    server.stubFor(get(urlEqualTo(CHUNK_PATH)).willReturn(aResponse().withBody("[[\"first\"]]")));
    server.stubFor(get(urlEqualTo(secondPath)).willReturn(aResponse().withBody("[[\"second\"]]")));
    respondWithLink(
        new ExternalLink().setExternalLink(server.baseUrl() + CHUNK_PATH).setNextChunkIndex(1L));
    when(statementApi.getStatementResultChunkN("stmt-1", 1L))
        .thenReturn(
            new ResultData()
                .setExternalLinks(
                    Collections.singletonList(
                        new ExternalLink().setExternalLink(server.baseUrl() + secondPath))));

    assertEquals(
        Arrays.asList(Collections.singletonList("first"), Collections.singletonList("second")),
        collect());
  }

  /** Losing one chunk fails the whole query, so a transient refusal is worth waiting out. */
  @Test
  public void executeBulkQueryOrThrow_recoversFromATransientFailure() throws Exception {
    String scenario = "transient";
    server.stubFor(
        get(urlEqualTo(CHUNK_PATH))
            .inScenario(scenario)
            .whenScenarioStateIs(STARTED)
            .willReturn(aResponse().withStatus(503).withHeader(RETRY_AFTER, "0"))
            .willSetStateTo("recovered"));
    server.stubFor(
        get(urlEqualTo(CHUNK_PATH))
            .inScenario(scenario)
            .whenScenarioStateIs("recovered")
            .willReturn(aResponse().withBody("[[\"recovered\"]]")));
    respondWithLink(new ExternalLink().setExternalLink(server.baseUrl() + CHUNK_PATH));

    assertEquals(Collections.singletonList(Collections.singletonList("recovered")), collect());
    server.verify(exactly(2), getRequestedFor(urlEqualTo(CHUNK_PATH)));
  }

  /**
   * Asserts the header is consulted at all, by timing rather than by inspection.
   *
   * <p>Two retries told to wait zero seconds should cost nothing. Ignoring the header would fall
   * back to the jittered schedule, whose first two waits are at least 500ms and 1000ms, so the
   * margin between honoring and not honoring it is well over a second.
   */
  @Test
  public void executeBulkQueryOrThrow_honorsRetryAfter() throws Exception {
    String scenario = "throttled";
    server.stubFor(
        get(urlEqualTo(CHUNK_PATH))
            .inScenario(scenario)
            .whenScenarioStateIs(STARTED)
            .willReturn(aResponse().withStatus(429).withHeader(RETRY_AFTER, "0"))
            .willSetStateTo("second"));
    server.stubFor(
        get(urlEqualTo(CHUNK_PATH))
            .inScenario(scenario)
            .whenScenarioStateIs("second")
            .willReturn(aResponse().withStatus(429).withHeader(RETRY_AFTER, "0"))
            .willSetStateTo("third"));
    server.stubFor(
        get(urlEqualTo(CHUNK_PATH))
            .inScenario(scenario)
            .whenScenarioStateIs("third")
            .willReturn(aResponse().withBody("[[\"ok\"]]")));
    respondWithLink(new ExternalLink().setExternalLink(server.baseUrl() + CHUNK_PATH));

    long startedAt = System.nanoTime();
    collect();
    long elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000L;

    assertTrue(
        "Two zero-second waits took " + elapsedMillis + "ms, so Retry-After was ignored.",
        elapsedMillis < 1_000L);
  }

  @Test
  public void executeBulkQueryOrThrow_whenRetriesAreExhausted_throws() throws Exception {
    server.stubFor(
        get(urlEqualTo(CHUNK_PATH))
            .willReturn(aResponse().withStatus(503).withHeader(RETRY_AFTER, "0")));
    respondWithLink(new ExternalLink().setExternalLink(server.baseUrl() + CHUNK_PATH));

    assertThrows(SQLException.class, this::collect);
    server.verify(exactly(MAX_RETRIES), getRequestedFor(urlEqualTo(CHUNK_PATH)));
  }

  private void respondWithLink(ExternalLink link) {
    StatementResponse response =
        new StatementResponse()
            .setStatementId("stmt-1")
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
            .setResult(new ResultData().setExternalLinks(Collections.singletonList(link)));
    when(statementApi.executeStatement(any(ExecuteStatementRequest.class))).thenReturn(response);
  }

  private List<List<String>> collect() throws SQLException {
    List<List<String>> rows = new ArrayList<>();
    DatabricksSqlHelper.executeBulkQueryOrThrow(handle, "SELECT 1", rows::add);
    return rows;
  }

  private static byte[] gzip(String body) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (GZIPOutputStream out = new GZIPOutputStream(bytes)) {
      out.write(body.getBytes(StandardCharsets.UTF_8));
    }
    return bytes.toByteArray();
  }
}
