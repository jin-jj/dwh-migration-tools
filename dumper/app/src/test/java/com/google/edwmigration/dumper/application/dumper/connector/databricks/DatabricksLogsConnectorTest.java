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
import static com.github.tomakehurst.wiremock.client.WireMock.forbidden;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksLogsConnector.DatabricksLogsProperty.ALL_WORKSPACES;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksLogsConnector.DatabricksLogsProperty.SOURCE;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksLogsConnector.DatabricksLogsProperty.WAREHOUSE_TRANSPORT;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ObjectArrays;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksLogsConnector.DatabricksLogsProperty;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.logs.QueryHistoryTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.logs.QueryHistoryTask.DatabricksLogsHandle;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksLogsConnectorTest {

  private static final String CURRENT_USER_PATH = "/api/2.0/preview/scim/v2/Me";
  private static final String URL = "https://workspace.cloud.databricks.com";
  private static final String WAREHOUSE = "wh-123";
  private static final String CLUSTER = "0123-456789-abcdefgh";
  private static final String WORKSPACE = "1234567890123456";
  private static final String OTHER_WORKSPACE = "2222222222222222";
  private static final String PERMISSION_ERROR = "[INSUFFICIENT_PERMISSIONS] No USE SCHEMA.";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static WireMockServer server;

  private final DatabricksLogsConnector connector = new DatabricksLogsConnector();

  @BeforeClass
  public static void startServer() {
    server = new WireMockServer(wireMockConfig().dynamicPort());
    server.start();
  }

  @AfterClass
  public static void stopServer() {
    server.stop();
  }

  // The ID of the --url workspace is a header of the current user API.
  @Before
  public void resetServer() {
    server.resetAll();
    server.stubFor(
        get(urlPathEqualTo(CURRENT_USER_PATH))
            .willReturn(okJson("{}").withHeader("X-Databricks-Org-Id", WORKSPACE)));
  }

  @Test
  public void validate_checksSourceComputeTransportAndWorkspaceIdOption() {
    connector.validate(args(URL));
    connector.validate(args(URL, "--warehouse", WAREHOUSE, definition(WORKSPACE_ID, "1, 2")));
    connector.validate(args(URL, "--cluster", CLUSTER, definition(SOURCE, " System-Table ")));
    connector.validate(args(URL, "--warehouse", WAREHOUSE, definition(SOURCE, "api")));
    connector.validate(
        args(URL, "--warehouse", WAREHOUSE, definition(WAREHOUSE_TRANSPORT, " INLINE ")));

    assertValidateRejects("--warehouse", WAREHOUSE, "--cluster", CLUSTER);
    assertValidateRejects("--cluster", CLUSTER, definition(WAREHOUSE_TRANSPORT, "inline"));
    assertTrue(assertValidateRejects(definition(SOURCE, "sql")).contains("'system-table'"));
    // The usage errors show how to choose where the queries are read from.
    assertTrue(
        assertValidateRejects(definition(SOURCE, "system-table"))
            .contains("--warehouse <warehouse-id>"));
    assertTrue(
        assertValidateRejects(definition(WORKSPACE_ID, WORKSPACE)).contains("--query-log-days"));
    assertTrue(
        assertValidateRejects(
                "--cluster", CLUSTER, definition(SOURCE, "api"), definition(WORKSPACE_ID, "all"))
            .contains(SOURCE.getName() + "=api"));
    assertTrue(
        assertValidateRejects("--cluster", CLUSTER, definition(WORKSPACE_ID, "1,,2"))
            .contains("'1,,2'"));
  }

  @Test
  public void open_resolvesWorkspaceIds() throws Exception {
    stubProbeSucceeded(ImmutableList.of(probeRow("SELECT 1")));
    // By default, the --url workspace.
    assertEquals(ImmutableList.of(WORKSPACE), openWarehouseHandle().workspaceIds());
    assertEquals(
        ImmutableList.of(WORKSPACE, OTHER_WORKSPACE),
        openWarehouseHandle(definition(WORKSPACE_ID, WORKSPACE + "," + OTHER_WORKSPACE))
            .workspaceIds());
    assertEquals(
        ImmutableList.of(),
        openWarehouseHandle(definition(WORKSPACE_ID, ALL_WORKSPACES)).workspaceIds());

    // Without the header, the workspace IDs have to be given.
    server.stubFor(get(urlPathEqualTo(CURRENT_USER_PATH)).willReturn(okJson("{}")));
    assertEquals(
        ImmutableList.of(OTHER_WORKSPACE),
        openWarehouseHandle(definition(WORKSPACE_ID, OTHER_WORKSPACE)).workspaceIds());
    assertTrue(
        assertThrows(MetadataDumperUsageException.class, this::openWarehouseHandle)
            .getMessage()
            .contains(WORKSPACE_ID.getName()));
  }

  @Test
  public void open_readsTheSystemTableOrTheApi() throws Exception {
    // Without a SQL warehouse or cluster, or with -Ddatabricks.logs.source=api, the API is read.
    stubProbeSucceeded(ImmutableList.of());
    assertNull(openHandle().sqlRunner());
    assertNull(openWarehouseHandle(definition(SOURCE, "API")).sqlRunner());
    assertNotNull(openWarehouseHandle().sqlRunner());

    // The probe fails with an API error, such as a missing permission on the SQL warehouse.
    stubProbe(forbidden());
    assertNull(openWarehouseHandle().sqlRunner());

    // With -Ddatabricks.logs.source=system-table or -Ddatabricks.logs.workspace-id, the query
    // history API never replaces the system table.
    stubProbeSucceeded(ImmutableList.of(probeRow(REDACTED_STATEMENT_TEXT)));
    assertNull(openWarehouseHandle().sqlRunner());
    assertNotNull(openWarehouseHandle(definition(SOURCE, "system-table")).sqlRunner());
    assertNotNull(openWarehouseHandle(definition(WORKSPACE_ID, WORKSPACE)).sqlRunner());

    stubProbe(
        okJson(
            "{\"status\":{\"state\":\"FAILED\",\"error\":{\"message\":\""
                + PERMISSION_ERROR
                + "\"}}}"));
    assertTrue(openFailure(definition(WORKSPACE_ID, ALL_WORKSPACES)).contains(PERMISSION_ERROR));
    String systemTableFailure = openFailure(definition(SOURCE, "system-table"));
    assertTrue(systemTableFailure.contains(PERMISSION_ERROR));
    assertTrue(systemTableFailure.contains("SELECT on system.query.history"));
  }

  @Test
  public void addTasksTo_namesEachEntryAfterTheStartOfItsHour() {
    List<Task<?>> tasks = new ArrayList<>();
    connector.addTasksTo(
        tasks,
        args(
            URL,
            "--query-log-start",
            "2026-10-09 13:00:00",
            "--query-log-end",
            "2026-10-09 15:00:00"));

    List<String> entryNames = new ArrayList<>();
    for (Task<?> task : tasks) {
      if (task instanceof QueryHistoryTask) {
        entryNames.add(task.getTargetPath());
      }
    }

    assertEquals(
        ImmutableList.of(
            "query_history_2026-10-09T130000Z.csv", "query_history_2026-10-09T140000Z.csv"),
        entryNames);
  }

  private String assertValidateRejects(String... extraArguments) {
    return assertThrows(
            MetadataDumperUsageException.class, () -> connector.validate(args(URL, extraArguments)))
        .getMessage();
  }

  private String openFailure(String... extraArguments) {
    return assertThrows(IOException.class, () -> openWarehouseHandle(extraArguments)).getMessage();
  }

  private DatabricksLogsHandle openWarehouseHandle(String... extraArguments) throws IOException {
    return openHandle(
        ObjectArrays.concat(
            new String[] {"--warehouse", WAREHOUSE, definition(WAREHOUSE_TRANSPORT, "inline")},
            extraArguments,
            String.class));
  }

  /** Opens and closes the handle of a run, which keeps what the run resolved. */
  private DatabricksLogsHandle openHandle(String... extraArguments) throws IOException {
    try (Handle handle = connector.open(args(server.baseUrl(), extraArguments))) {
      return (DatabricksLogsHandle) handle;
    }
  }

  private static void stubProbeSucceeded(Object rows) throws IOException {
    stubProbe(
        okJson(
            "{\"status\":{\"state\":\"SUCCEEDED\"},\"result\":{\"data_array\":"
                + MAPPER.writeValueAsString(rows)
                + "}}"));
  }

  private static void stubProbe(ResponseDefinitionBuilder response) {
    server.stubFor(
        post(urlPathEqualTo("/api/2.0/sql/statements"))
            .withRequestBody(matchingJsonPath("$.statement", containing(" LIMIT 1")))
            .willReturn(response));
  }

  // The probe reads only the statement text, which is the sixth column.
  private static ImmutableList<String> probeRow(String statementText) {
    return ImmutableList.of("", WORKSPACE, "stmt-1", "", "", statementText);
  }

  private static String definition(DatabricksLogsProperty property, String value) {
    return "-D" + property.getName() + "=" + value;
  }

  private static ConnectorArguments args(String url, String... extraArguments) {
    return ConnectorArguments.create(
        ImmutableList.<String>builder()
            .add("--connector", DatabricksLogsConnector.CONNECTOR_NAME, "--url", url)
            .add("--password", "test-token")
            .add(extraArguments)
            .build());
  }
}
