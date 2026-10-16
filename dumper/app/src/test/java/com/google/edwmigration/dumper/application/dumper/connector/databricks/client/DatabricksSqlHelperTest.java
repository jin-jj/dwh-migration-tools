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
package com.google.edwmigration.dumper.application.dumper.connector.databricks.client;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.utils.SerDeUtils;
import com.databricks.sdk.service.sql.ExternalLink;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;
import com.databricks.sdk.service.sql.StatementResponse;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper.SqlQuoting;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper.StatementResult;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksSqlHelperTest {

  private static final String CATALOG = "hive_metastore";
  private static final String STATEMENTS_PATH = "/api/2.0/sql/statements";
  private static final ObjectWriter WRITER = SerDeUtils.createMapper().writer();

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
  public void sqlHelper_retriesChunkDownloadAfter503AndSendsLinkHeaders() throws Exception {
    stubFailureThenSuccess(
        "/chunk/0",
        aResponse().withStatus(503),
        aResponse().withStatus(200).withBody("[[\"cat1\", null, \"null\"]]"));
    stubSucceededStatement(
        link("/chunk/0").setHttpHeaders(ImmutableMap.of("X-Custom-Cloud-Header", "sig-token")));
    assertEquals(
        ImmutableList.of(Arrays.asList("cat1", null, "null")),
        readRows(newSqlHelper(), "SELECT 1"));
    wireMock.verify(
        2,
        getRequestedFor(urlEqualTo("/chunk/0"))
            .withHeader("X-Custom-Cloud-Header", equalTo("sig-token")));
  }

  @Test
  public void sqlHelper_failsChunkDownloadOn404WithoutUrl() {
    wireMock.stubFor(get(urlPathEqualTo("/chunk/404")).willReturn(aResponse().withStatus(404)));
    stubSucceededStatement(link("/chunk/404?X-Amz-Signature=sig-value"));
    IOException thrown =
        assertThrows(IOException.class, () -> readRows(newSqlHelper(), "SELECT 1"));
    assertEquals("Failed to download external result chunk (HTTP 404).", thrown.getMessage());
    wireMock.verify(1, getRequestedFor(urlPathEqualTo("/chunk/404")));
  }

  @Test
  public void sqlHelper_pollsPendingStatementAndStreamsTwoChunks() throws Exception {
    wireMock.stubFor(
        get(urlEqualTo("/chunk/0"))
            .willReturn(aResponse().withStatus(200).withBody("[[\"r0\"], [\"r1\"]]")));
    wireMock.stubFor(
        get(urlEqualTo("/chunk/1")).willReturn(aResponse().withStatus(200).withBody("[[\"r2\"]]")));
    StatementResponse pendingResp =
        new StatementResponse()
            .setStatementId("stmt-100")
            .setStatus(new StatementStatus().setState(StatementState.PENDING));
    StatementResponse succeededResp = succeeded("stmt-100", link("/chunk/0"));
    succeededResp.getManifest().setTotalChunkCount(2L);
    wireMock.stubFor(post(urlPathEqualTo(STATEMENTS_PATH)).willReturn(okModel(pendingResp)));
    wireMock.stubFor(
        get(urlPathEqualTo(STATEMENTS_PATH + "/stmt-100")).willReturn(okModel(succeededResp)));
    wireMock.stubFor(
        get(urlPathEqualTo(STATEMENTS_PATH + "/stmt-100/result/chunks/1"))
            .willReturn(
                okModel(
                    new ResultData()
                        .setChunkIndex(1L)
                        .setExternalLinks(ImmutableList.of(link("/chunk/1"))))));
    assertEquals(
        ImmutableList.of(ImmutableList.of("r0"), ImmutableList.of("r1"), ImmutableList.of("r2")),
        readRows(newSqlHelper(), "SELECT 1"));
    wireMock.verify(
        postRequestedFor(urlPathEqualTo(STATEMENTS_PATH))
            .withRequestBody(
                matchingJsonPath("$.query_tags[0].key", equalTo(DatabricksSqlHelper.QUERY_TAG_KEY)))
            .withRequestBody(
                matchingJsonPath(
                    "$.query_tags[0].value", equalTo(DatabricksSqlHelper.QUERY_TAG_VALUE))));
  }

  @Test
  public void sqlHelper_enforcesStatementDeadlineAndCancelsTimedOutStatement() {
    StatementResponse runningResp =
        new StatementResponse()
            .setStatementId("stmt-200")
            .setStatus(new StatementStatus().setState(StatementState.RUNNING));
    wireMock.stubFor(post(urlPathEqualTo(STATEMENTS_PATH)).willReturn(okModel(runningResp)));
    wireMock.stubFor(
        get(urlPathEqualTo(STATEMENTS_PATH + "/stmt-200")).willReturn(okModel(runningResp)));
    wireMock.stubFor(
        post(urlPathEqualTo(STATEMENTS_PATH + "/stmt-200/cancel"))
            .willReturn(okModel(ImmutableMap.of())));
    DatabricksSqlHelper helper =
        new DatabricksSqlHelper(newWorkspaceClient(), "wh-1", Duration.ZERO, Duration.ZERO);
    IOException thrown =
        assertThrows(IOException.class, () -> helper.executeSqlInCatalog(CATALOG, "SELECT 1"));
    assertTrue(thrown.getMessage().contains("Timed out waiting for SQL statement stmt-200"));
    wireMock.verify(1, postRequestedFor(urlPathEqualTo(STATEMENTS_PATH + "/stmt-200/cancel")));
  }

  @Test
  public void sqlQuoting_quotesIdentifiersQualifiedNamesAndStringLiterals() {
    assertEquals("`legacy`", SqlQuoting.quoteIdentifier("legacy"));
    assertEquals("`odd``name`", SqlQuoting.quoteIdentifier("odd`name"));
    assertEquals("`sales`.`orders`", SqlQuoting.quoteQualifiedName("sales", "orders"));
    assertEquals("`s``1`.`t``2`", SqlQuoting.quoteQualifiedName("s`1", "t`2"));
    assertEquals("sales_2024", SqlQuoting.quoteIdentifierIfNeeded("sales_2024"));
    assertEquals("_1", SqlQuoting.quoteIdentifierIfNeeded("_1"));
    assertEquals("`2024`", SqlQuoting.quoteIdentifierIfNeeded("2024"));
    assertEquals("`my table`", SqlQuoting.quoteIdentifierIfNeeded("my table"));
    assertEquals("`a-b`", SqlQuoting.quoteIdentifierIfNeeded("a-b"));
    assertEquals("`odd``name`", SqlQuoting.quoteIdentifierIfNeeded("odd`name"));
    assertEquals("``", SqlQuoting.quoteIdentifierIfNeeded(""));
    assertEquals("'plain'", SqlQuoting.quoteStringLiteral("plain"));
    assertEquals("'it\\'s C:\\\\tmp'", SqlQuoting.quoteStringLiteral("it's C:\\tmp"));
  }

  private ExternalLink link(String path) {
    return new ExternalLink().setExternalLink(wireMock.baseUrl() + path);
  }

  private DatabricksSqlHelper newSqlHelper() {
    return new DatabricksSqlHelper(
        newWorkspaceClient(), "wh-1", Duration.ZERO, Duration.ofMinutes(1));
  }

  private WorkspaceClient newWorkspaceClient() {
    return new WorkspaceClient(
        new DatabricksConfig()
            .setHost(wireMock.baseUrl())
            .setToken("test-token")
            .setAuthType("pat"));
  }

  private static ResponseDefinitionBuilder okModel(Object model) {
    try {
      return okJson(WRITER.writeValueAsString(model));
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException("Failed to write a test response.", e);
    }
  }

  private void stubSucceededStatement(ExternalLink link) {
    wireMock.stubFor(
        post(urlPathEqualTo(STATEMENTS_PATH)).willReturn(okModel(succeeded("stmt-1", link))));
  }

  private void stubFailureThenSuccess(
      String path, ResponseDefinitionBuilder failure, ResponseDefinitionBuilder success) {
    String failedOnce = "failed once";
    wireMock.stubFor(
        get(urlPathEqualTo(path))
            .inScenario(path)
            .whenScenarioStateIs(Scenario.STARTED)
            .willReturn(failure)
            .willSetStateTo(failedOnce));
    wireMock.stubFor(
        get(urlPathEqualTo(path))
            .inScenario(path)
            .whenScenarioStateIs(failedOnce)
            .willReturn(success));
  }

  private static StatementResponse succeeded(String statementId, ExternalLink... links) {
    return new StatementResponse()
        .setStatementId(statementId)
        .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
        .setManifest(new ResultManifest().setTotalChunkCount(1L))
        .setResult(new ResultData().setChunkIndex(0L).setExternalLinks(Arrays.asList(links)));
  }

  private static ImmutableList<List<String>> readRows(DatabricksSqlHelper helper, String sql)
      throws IOException {
    ImmutableList.Builder<List<String>> rows = ImmutableList.builder();
    StatementResult result = helper.executeSqlInCatalog(CATALOG, sql);
    for (List<String> row = result.nextRow(); row != null; row = result.nextRow()) {
      rows.add(row);
    }
    return rows.build();
  }
}
