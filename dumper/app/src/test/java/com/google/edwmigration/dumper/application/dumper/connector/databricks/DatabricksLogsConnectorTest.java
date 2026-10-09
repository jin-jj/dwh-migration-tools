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

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksLogsConnector.ALL_WORKSPACES;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksLogsConnector.DatabricksLogsProperty.QUERY;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksLogsConnector.DatabricksLogsProperty.STRATEGY;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksLogsConnector.DatabricksLogsProperty.WHERE;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksLogsConnector.DatabricksLogsProperty.WORKSPACE_ID;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestHelper.CURRENT_USER_PATH;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestHelper.WORKSPACE_ID_HEADER;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestStubs.UNTHROTTLED_REQUESTS_PER_SECOND;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestStubs.error;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestStubs.newWorkspaceClient;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.ConnectorRepository;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksLogsConnector.DatabricksLogsProperty;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksLogsConnector.Strategy;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksQueryHistoryTasks.DatabricksQueryHistoryTask;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.utils.ArchiveNameUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksLogsConnectorTest {

  private static final String CONNECTOR_NAME = DatabricksLogsConnector.CONNECTOR_NAME;
  private static final String URL = "https://workspace.cloud.databricks.com";
  private static final String WAREHOUSE = "wh-123";
  private static final String WORKSPACE = "1234567890123456";

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
  public void connector_isDiscoverableViaConnectorRepository() {
    assertNotNull(ConnectorRepository.getInstance().getByName(CONNECTOR_NAME));
  }

  @Test
  public void formatName_isTheLogsFormatOfDatabricks() {
    assertEquals(
        DatabricksMetadataConnector.CONNECTOR_NAME + ".logs.zip",
        DatabricksLogsDumpFormat.FORMAT_NAME);
  }

  @Test
  public void defaultFileName_isTheArchiveOfTheConnector() {
    assertEquals(
        ArchiveNameUtil.getFileName(CONNECTOR_NAME), connector.getDefaultFileName(false, null));
  }

  @Test
  public void properties_arePrefixedWithDatabricksLogs() {
    for (DatabricksLogsProperty property : DatabricksLogsProperty.values()) {
      assertTrue(property.getName(), property.getName().startsWith("databricks.logs."));
    }
  }

  @Test
  public void systemTableColumns_matchTheHeader() {
    assertEquals(
        DatabricksLogsDumpFormat.Header.values().length,
        DatabricksQueryHistoryTasks.SYSTEM_TABLE_COLUMNS.size());
  }

  @Test
  public void validate_rejectsAssessmentFlag() throws IOException {
    ConnectorArguments arguments = arguments("--assessment");

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> connector.validate(arguments));
    assertTrue(exception.getMessage().contains("--assessment"));
  }

  @Test
  public void validate_requiresUrl() throws IOException {
    ConnectorArguments arguments =
        new ConnectorArguments("--connector", CONNECTOR_NAME, "--warehouse", WAREHOUSE);

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> connector.validate(arguments));
    assertTrue(exception.getMessage().contains("--url"));
  }

  @Test
  public void validate_requiresWarehouseUnlessRestOnly() throws IOException {
    ConnectorArguments withoutWarehouse =
        new ConnectorArguments("--connector", CONNECTOR_NAME, "--url", URL);

    MetadataDumperUsageException exception =
        assertThrows(
            MetadataDumperUsageException.class, () -> connector.validate(withoutWarehouse));
    assertTrue(exception.getMessage().contains("--warehouse"));
    connector.validate(
        new ConnectorArguments(
            "--connector",
            CONNECTOR_NAME,
            "--url",
            URL,
            definition(STRATEGY, Strategy.REST_ONLY.toString())));
  }

  @Test
  public void validate_acceptsStrategiesInAnyCase() throws IOException {
    for (Strategy strategy : Strategy.values()) {
      connector.validate(
          arguments(definition(STRATEGY, strategy.toString().toUpperCase(Locale.ROOT))));
    }
  }

  @Test
  public void validate_rejectsUnknownStrategy() throws IOException {
    ConnectorArguments arguments = arguments(definition(STRATEGY, "fastest"));

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> connector.validate(arguments));
    assertTrue(exception.getMessage().contains(STRATEGY.getName()));
    for (Strategy strategy : Strategy.values()) {
      assertTrue(exception.getMessage().contains(strategy.toString()));
    }
  }

  @Test
  public void validate_rejectsWorkspaceIdsThatAreNotNumbers() throws IOException {
    for (String value : ImmutableList.of(WORKSPACE + ",prod", WORKSPACE + ",", "-1")) {
      ConnectorArguments arguments = arguments(definition(WORKSPACE_ID, value));

      MetadataDumperUsageException exception =
          assertThrows(MetadataDumperUsageException.class, () -> connector.validate(arguments));
      assertTrue(exception.getMessage().contains(WORKSPACE_ID.getName()));
      assertTrue(exception.getMessage().contains(value));
    }
  }

  @Test
  public void validate_rejectsTheSqlOptionsAndWorkspacesWithRestOnly() throws IOException {
    ImmutableList<String> definitions =
        ImmutableList.of(
            definition(WORKSPACE_ID, ALL_WORKSPACES),
            definition(
                QUERY,
                "SELECT * FROM system.query.history WHERE start_time >= '%s'"
                    + " AND start_time < '%s'"),
            definition(WHERE, "execution_status = 'FINISHED'"));
    for (String definition : definitions) {
      ConnectorArguments arguments =
          arguments(definition(STRATEGY, Strategy.REST_ONLY.toString()), definition);

      MetadataDumperUsageException exception =
          assertThrows(MetadataDumperUsageException.class, () -> connector.validate(arguments));
      assertTrue(exception.getMessage().contains(Strategy.REST_ONLY.toString()));
    }
  }

  @Test
  public void validate_rejectsQueryWithoutTwoPlaceholders() throws IOException {
    ConnectorArguments arguments =
        arguments(definition(QUERY, "SELECT * FROM system.query.history WHERE start_time >= '%s'"));

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> connector.validate(arguments));
    assertTrue(exception.getMessage().contains(QUERY.getName()));
  }

  @Test
  public void validate_rejectsQueryTogetherWithWhere() throws IOException {
    ConnectorArguments arguments =
        arguments(
            definition(QUERY, "SELECT * FROM t WHERE start_time >= '%s' AND start_time < '%s'"),
            definition(WHERE, "execution_status = 'FINISHED'"));

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> connector.validate(arguments));
    assertTrue(exception.getMessage().contains(QUERY.getName()));
    assertTrue(exception.getMessage().contains(WHERE.getName()));
  }

  @Test
  public void addTasksTo_addsOneQueryHistoryTaskPerHour() throws IOException {
    List<Task<?>> tasks = new ArrayList<>();

    connector.addTasksTo(
        tasks,
        arguments(
            "--query-log-start", "2026-10-09 10:00:00", "--query-log-end", "2026-10-09 13:00:00"));

    List<String> queryHistoryPaths = new ArrayList<>();
    for (Task<?> task : tasks) {
      if (task instanceof DatabricksQueryHistoryTask) {
        queryHistoryPaths.add(task.getTargetPath());
      }
    }
    String prefix = DatabricksLogsDumpFormat.ZIP_ENTRY_PREFIX;
    assertEquals(
        ImmutableList.of(
            prefix + "2026-10-09T100000Z.csv",
            prefix + "2026-10-09T110000Z.csv",
            prefix + "2026-10-09T120000Z.csv"),
        queryHistoryPaths);
    assertEquals(DumpMetadataTask.class, tasks.get(0).getClass());
    assertEquals(
        new FormatTask(DatabricksLogsDumpFormat.FORMAT_NAME).getTargetPath(),
        tasks.get(1).getTargetPath());
  }

  @Test
  public void createHandle_dumpsTheUrlWorkspaceByDefault() throws IOException {
    stubWorkspaceIdHeader(WORKSPACE);

    DatabricksLogsHandle handle = createHandle(URL, arguments());

    assertEquals(WORKSPACE, handle.urlWorkspaceId());
    assertEquals(ImmutableList.of(WORKSPACE), handle.workspaceIds());
  }

  @Test
  public void createHandle_dumpsTheListedWorkspaces() throws IOException {
    stubWorkspaceIdHeader(WORKSPACE);
    String otherWorkspace = "2222222222222222";

    DatabricksLogsHandle handle =
        createHandle(URL, arguments(definition(WORKSPACE_ID, WORKSPACE + ", " + otherWorkspace)));

    assertEquals(WORKSPACE, handle.urlWorkspaceId());
    assertEquals(ImmutableList.of(WORKSPACE, otherWorkspace), handle.workspaceIds());
  }

  @Test
  public void createHandle_readsTheWorkspaceIdOfAzureAndGoogleCloudHosts() throws IOException {
    server.stubFor(get(urlPathEqualTo(CURRENT_USER_PATH)).willReturn(okJson("{}")));
    ImmutableList<String> urls =
        ImmutableList.of(
            "https://adb-" + WORKSPACE + ".7.azuredatabricks.net",
            "https://" + WORKSPACE + ".7.gcp.databricks.com");

    for (String url : urls) {
      DatabricksLogsHandle handle = createHandle(url, arguments());

      assertEquals(url, WORKSPACE, handle.urlWorkspaceId());
      assertEquals(url, ImmutableList.of(WORKSPACE), handle.workspaceIds());
    }
  }

  @Test
  public void createHandle_needsTheWorkspaceIdUnlessEveryWorkspaceIsDumped() throws IOException {
    server.stubFor(
        get(urlPathEqualTo(CURRENT_USER_PATH))
            .willReturn(error(403, "PERMISSION_DENIED", "Forbidden.")));

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> createHandle(URL, arguments()));
    assertTrue(exception.getMessage().contains(URL));
    assertTrue(exception.getMessage().contains(WORKSPACE_ID.getName()));
    DatabricksLogsHandle handle =
        createHandle(URL, arguments(definition(WORKSPACE_ID, ALL_WORKSPACES)));
    assertNull(handle.urlWorkspaceId());
    assertEquals(ImmutableList.of(), handle.workspaceIds());
  }

  @Test
  public void createHandle_restOnlyDoesNotNeedTheWorkspaceId() throws IOException {
    server.stubFor(
        get(urlPathEqualTo(CURRENT_USER_PATH))
            .willReturn(error(403, "PERMISSION_DENIED", "Forbidden.")));

    DatabricksLogsHandle handle =
        createHandle(URL, arguments(definition(STRATEGY, Strategy.REST_ONLY.toString())));

    assertNull(handle.urlWorkspaceId());
  }

  private static DatabricksLogsHandle createHandle(String url, ConnectorArguments arguments)
      throws IOException {
    DatabricksRestHelper restHelper =
        new DatabricksRestHelper(
            newWorkspaceClient(server.baseUrl()), UNTHROTTLED_REQUESTS_PER_SECOND, millis -> {});
    return DatabricksLogsConnector.createHandle(null, restHelper, url, arguments);
  }

  private static void stubWorkspaceIdHeader(String workspaceId) {
    server.stubFor(
        get(urlPathEqualTo(CURRENT_USER_PATH))
            .willReturn(
                okJson("{\"userName\": \"user@example.com\"}")
                    .withHeader(WORKSPACE_ID_HEADER, workspaceId)));
  }

  private static String definition(DatabricksLogsProperty property, String value) {
    return "-D" + property.getName() + "=" + value;
  }

  private static ConnectorArguments arguments(String... extraArguments) throws IOException {
    List<String> allArguments = new ArrayList<>();
    allArguments.add("--connector");
    allArguments.add(CONNECTOR_NAME);
    allArguments.add("--url");
    allArguments.add(URL);
    allArguments.add("--warehouse");
    allArguments.add(WAREHOUSE);
    for (String argument : extraArguments) {
      allArguments.add(argument);
    }
    return new ConnectorArguments(allArguments.toArray(new String[0]));
  }
}
