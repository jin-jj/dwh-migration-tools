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
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksLogsConnector.DatabricksLogsProperty.ALL_WORKSPACES;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksLogsConnector.DatabricksLogsProperty.WORKSPACE_ID;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.logs.QueryHistoryTask.REDACTED_STATEMENT_TEXT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ObjectArrays;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksLogsConnector.DatabricksLogsProperty;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.logs.QueryHistoryTask.DatabricksLogsHandle;
import java.io.IOException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksLogsConnectorTest {

  private static final String CONNECTOR_NAME = DatabricksLogsConnector.CONNECTOR_NAME;
  private static final String CURRENT_USER_PATH = "/api/2.0/preview/scim/v2/Me";
  private static final String STATEMENTS_PATH = "/api/2.0/sql/statements";
  private static final String URL = "https://workspace.cloud.databricks.com";
  private static final String WAREHOUSE = "wh-123";
  private static final String WORKSPACE = "1234567890123456";
  private static final String OTHER_WORKSPACE = "2222222222222222";
  private static final String PERMISSION_ERROR = "[INSUFFICIENT_PERMISSIONS] No USE SCHEMA.";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static WireMockServer server;

  private final DatabricksLogsConnector connector = new DatabricksLogsConnector();

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
  public void validate_checksWorkspaceIds() throws IOException {
    connector.validate(args(URL));
    connector.validate(
        args(
            URL,
            "--warehouse",
            WAREHOUSE,
            definition(WORKSPACE_ID, WORKSPACE + ", " + OTHER_WORKSPACE)));

    String invalidValue = WORKSPACE + ",";
    MetadataDumperUsageException exception =
        assertThrows(
            MetadataDumperUsageException.class,
            () ->
                connector.validate(
                    args(URL, "--warehouse", WAREHOUSE, definition(WORKSPACE_ID, invalidValue))));
    assertTrue(exception.getMessage().contains(invalidValue));

    MetadataDumperUsageException withoutWarehouse =
        assertThrows(
            MetadataDumperUsageException.class,
            () -> connector.validate(args(URL, definition(WORKSPACE_ID, WORKSPACE))));
    assertTrue(withoutWarehouse.getMessage().contains("--warehouse"));
  }

  @Test
  public void open_resolvesWorkspaceIds() throws Exception {
    stubWorkspaceIdHeader(WORKSPACE);
    stubProbeSucceeded(ImmutableList.of(probeRow("SELECT 1")));
    // By default, the --url workspace, whose ID is a header of the current user API.
    assertEquals(ImmutableList.of(WORKSPACE), openHandle("--warehouse", WAREHOUSE).workspaceIds());
    assertEquals(
        ImmutableList.of(WORKSPACE, OTHER_WORKSPACE),
        openHandle(
                "--warehouse",
                WAREHOUSE,
                definition(WORKSPACE_ID, WORKSPACE + "," + OTHER_WORKSPACE))
            .workspaceIds());
    assertEquals(
        ImmutableList.of(),
        openHandle("--warehouse", WAREHOUSE, definition(WORKSPACE_ID, ALL_WORKSPACES))
            .workspaceIds());

    // Without the header, the workspace IDs have to be given.
    server.stubFor(get(urlPathEqualTo(CURRENT_USER_PATH)).willReturn(okJson("{}")));
    assertEquals(
        ImmutableList.of(OTHER_WORKSPACE),
        openHandle("--warehouse", WAREHOUSE, definition(WORKSPACE_ID, OTHER_WORKSPACE))
            .workspaceIds());
    assertTrue(
        assertThrows(MetadataDumperUsageException.class, () -> openHandle("--warehouse", WAREHOUSE))
            .getMessage()
            .contains(WORKSPACE_ID.getName()));
  }

  @Test
  public void open_fallsBackToApiUnlessWorkspaceIdsAreSet() throws Exception {
    stubWorkspaceIdHeader(WORKSPACE);
    // Without a SQL warehouse, the query history API is read.
    assertNull(openHandle().sqlHelper());

    // The probe fails with an API error, such as a missing permission on the SQL warehouse.
    stubProbe(permissionDenied("No permission to use the SQL warehouse."));
    assertNull(openHandle("--warehouse", WAREHOUSE).sqlHelper());

    stubProbeSucceeded(ImmutableList.of(probeRow(REDACTED_STATEMENT_TEXT)));
    assertNull(openHandle("--warehouse", WAREHOUSE).sqlHelper());

    stubProbeSucceeded(ImmutableList.of());
    assertNotNull(openHandle("--warehouse", WAREHOUSE).sqlHelper());

    // With -Ddatabricks.logs.workspace-id, the query history API can't replace the system table.
    stubProbeFailed();
    assertTrue(
        assertThrows(
                IOException.class,
                () ->
                    openHandle("--warehouse", WAREHOUSE, definition(WORKSPACE_ID, ALL_WORKSPACES)))
            .getMessage()
            .contains(PERMISSION_ERROR));

    stubProbeSucceeded(ImmutableList.of(probeRow(REDACTED_STATEMENT_TEXT)));
    assertNotNull(
        openHandle("--warehouse", WAREHOUSE, definition(WORKSPACE_ID, WORKSPACE)).sqlHelper());
  }

  private DatabricksLogsHandle openHandle(String... extraArguments) throws IOException {
    return (DatabricksLogsHandle) connector.open(args(server.baseUrl(), extraArguments));
  }

  private static void stubWorkspaceIdHeader(String workspaceId) {
    server.stubFor(
        get(urlPathEqualTo(CURRENT_USER_PATH))
            .willReturn(
                okJson("{\"userName\": \"user@example.com\"}")
                    .withHeader("X-Databricks-Org-Id", workspaceId)));
  }

  private static void stubProbeSucceeded(Object rows) throws IOException {
    String chunkPath = "/chunk/probe";
    server.stubFor(get(urlEqualTo(chunkPath)).willReturn(okJson(MAPPER.writeValueAsString(rows))));
    stubProbe(
        okJson(
            String.format(
                "{\"statement_id\": \"stmt-probe\", \"status\": {\"state\": \"SUCCEEDED\"},"
                    + " \"manifest\": {\"total_chunk_count\": 1},"
                    + " \"result\": {\"external_links\": [{\"external_link\": \"%s\"}]}}",
                server.baseUrl() + chunkPath)));
  }

  private static void stubProbeFailed() {
    stubProbe(
        okJson(
            String.format(
                "{\"statement_id\": \"stmt-probe\","
                    + " \"status\": {\"state\": \"FAILED\", \"error\": {\"message\": \"%s\"}}}",
                PERMISSION_ERROR)));
  }

  private static void stubProbe(ResponseDefinitionBuilder response) {
    server.stubFor(
        post(urlPathEqualTo(STATEMENTS_PATH))
            .withRequestBody(matchingJsonPath("$.statement", containing(" LIMIT 1")))
            .willReturn(response));
  }

  private static ResponseDefinitionBuilder permissionDenied(String message) {
    return aResponse()
        .withStatus(403)
        .withHeader("Content-Type", "application/json")
        .withBody("{\"error_code\":\"PERMISSION_DENIED\",\"message\":\"" + message + "\"}");
  }

  // The probe reads only the statement text, which is the sixth column.
  private static ImmutableList<String> probeRow(String statementText) {
    return ImmutableList.of("", WORKSPACE, "stmt-1", "", "", statementText);
  }

  private static String definition(DatabricksLogsProperty property, String value) {
    return "-D" + property.getName() + "=" + value;
  }

  private static ConnectorArguments args(String url, String... extraArguments) throws IOException {
    return new ConnectorArguments(
        ObjectArrays.concat(
            new String[] {"--connector", CONNECTOR_NAME, "--url", url, "--password", "test-token"},
            extraArguments,
            String.class));
  }
}
