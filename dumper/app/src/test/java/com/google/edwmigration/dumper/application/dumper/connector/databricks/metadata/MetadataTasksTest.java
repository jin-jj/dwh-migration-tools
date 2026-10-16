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
package com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata;

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
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.DatabricksFilter.HIVE_METASTORE_CATALOG;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.HIVE_METASTORE_COLUMNS_ENTRY_NAME;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.HIVE_METASTORE_TABLES_ENTRY_NAME;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.UNITY_CATALOG_CATALOGS_ENTRY_NAME;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.UNITY_CATALOG_SCHEMATA_ENTRY_NAME;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.UNITY_CATALOG_TABLE_CONSTRAINTS_ENTRY_NAME;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Resources;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksRestHelper;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.DatabricksHandle.MetadataTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.DatabricksFilter;
import com.google.edwmigration.dumper.application.dumper.task.MemoryByteSink;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests the Unity Catalog and the hive_metastore tasks with the recorded responses. */
@RunWith(JUnit4.class)
public class MetadataTasksTest {

  private static final String CATALOGS_PATH = "/api/2.1/unity-catalog/catalogs";
  private static final String SCHEMAS_PATH = "/api/2.1/unity-catalog/schemas";
  private static final String TABLES_PATH = "/api/2.1/unity-catalog/tables";
  private static final String STATEMENTS_PATH = "/api/2.0/sql/statements";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final DatabricksFilter ACCEPT_ALL =
      new DatabricksFilter(ImmutableList.of(), ImmutableList.of());

  private static WireMockServer server;

  @BeforeClass
  public static void startServer() {
    server = new WireMockServer(wireMockConfig().dynamicPort());
    server.start();
  }

  @AfterClass
  public static void stopServer() {
    server.stop();
  }

  /**
   * Stubs the GET Unity Catalog API calls, and the POST statements and GET chunks of the
   * hive_metastore SQL statements.
   */
  @Before
  public void stubRecordedResponses() throws IOException {
    server.resetAll();
    for (Map.Entry<String, JsonNode> entry : readFixture("unity-catalog-responses.json")) {
      server.stubFor(
          get(urlEqualTo(entry.getKey()))
              .atPriority(5)
              .willReturn(okJson(entry.getValue().toString())));
    }
    int index = 0;
    for (Map.Entry<String, JsonNode> entry : readFixture("hive-metastore-responses.json")) {
      JsonNode spec = entry.getValue();
      String rows =
          spec.has("json_cell")
              ? MAPPER.writeValueAsString(
                  ImmutableList.of(ImmutableList.of(spec.get("json_cell").toString())))
              : spec.path("rows").toString();
      stubStatement(entry.getKey(), "/chunk/" + index++, rows, spec.get("columns"), 5);
    }
  }

  @Test
  public void tasks_writeExpectedCsvsFromRecordedFixtures() throws Exception {
    try (DatabricksHandle handle = newHandle(ACCEPT_ALL)) {
      for (Task<?> task :
          Iterables.concat(UnityCatalogTasks.create(), HiveMetastoreTasks.create())) {
        assertMatchesExpectedCsv(task.getTargetPath(), runTask(task, handle));
      }
    }
    // Schemas are listed once per run; views are never fetched with tables.get.
    server.verify(2, getRequestedFor(urlPathEqualTo(CATALOGS_PATH)));
    server.verify(1, getRequestedFor(urlPathEqualTo(SCHEMAS_PATH)));
    server.verify(
        0,
        getRequestedFor(
            urlPathEqualTo(TABLES_PATH + "/dumper_test_uc.dumper_test_schema.items_view")));
    // 1 SHOW SCHEMAS + 1 SHOW TABLES + 3 DESCRIBE TABLE + 1 DESCRIBE DETAIL (cached across tasks)
    // = 6 statements total.
    server.verify(6, postRequestedFor(urlEqualTo(STATEMENTS_PATH)));
  }

  @Test
  public void catalogsAndSchemataTasks_applyCustomFilters() throws Exception {
    DatabricksFilter customFilter =
        new DatabricksFilter(
            ImmutableList.of("DUMPER_TEST_UC", "System", HIVE_METASTORE_CATALOG),
            ImmutableList.of("Dumper_Test_Schema", "information_schema"));
    // The listed system catalog is accepted. Its schemas.list isn't recorded, so it fails and is
    // skipped.
    assertEquals(
        "CatalogName,CatalogOwner\ndumper_test_uc,owner@example.com\nsystem,System user\n",
        runTask(UNITY_CATALOG_CATALOGS_ENTRY_NAME, customFilter));
    assertEquals(
        "CatalogName,SchemaName\ndumper_test_uc,dumper_test_schema\n",
        runTask(UNITY_CATALOG_SCHEMATA_ENTRY_NAME, customFilter));
  }

  @Test
  public void unityCatalogTasks_skipFailedObjectsAndFailWhenEveryObjectFails() throws Exception {
    // 1. One table's tables.get fails -> skipped while the other table's constraints are written.
    server.stubFor(
        get(urlPathEqualTo(TABLES_PATH + "/dumper_test_uc.dumper_test_schema.orgs"))
            .atPriority(1)
            .willReturn(permissionDenied("User does not have SELECT.")));
    String constraintsCsv = runTask(UNITY_CATALOG_TABLE_CONSTRAINTS_ENTRY_NAME, ACCEPT_ALL);
    assertTrue(constraintsCsv.contains("items_pk"));
    assertTrue(constraintsCsv.contains("items_org_fk"));
    assertFalse(constraintsCsv.contains("orgs_pk"));

    // 2. When every tables.get fails, or catalogs.list fails, the task fails.
    server.stubFor(
        get(urlPathMatching(TABLES_PATH + "/.+"))
            .atPriority(1)
            .willReturn(permissionDenied("No SELECT.")));
    assertTrue(
        assertThrows(
                IOException.class,
                () -> runTask(UNITY_CATALOG_TABLE_CONSTRAINTS_ENTRY_NAME, ACCEPT_ALL))
            .getMessage()
            .contains("tables.get failed for all"));

    server.stubFor(
        get(urlPathEqualTo(CATALOGS_PATH))
            .atPriority(1)
            .willReturn(permissionDenied("Not a metastore user.")));
    assertTrue(
        assertThrows(
                DatabricksException.class,
                () -> runTask(UNITY_CATALOG_CATALOGS_ENTRY_NAME, ACCEPT_ALL))
            .getMessage()
            .contains("Not a metastore user."));
  }

  @Test
  public void hiveMetastoreTasks_skipFailedObjectsAndFailWhenEveryShowTablesFails()
      throws Exception {
    stubStatement(
        "DESCRIBE TABLE EXTENDED `dumper_test_hms`.`parquet_items` AS JSON",
        "/chunk/not-json",
        "[[\"not a json object\"]]",
        null,
        1);
    // DESCRIBE DETAIL fails with an API error instead of a failed statement.
    server.stubFor(
        post(urlEqualTo(STATEMENTS_PATH))
            .atPriority(1)
            .withRequestBody(
                matchingJsonPath(
                    "$.statement", equalTo("DESCRIBE DETAIL `dumper_test_hms`.`delta_items`")))
            .willReturn(permissionDenied("No access to the SQL warehouse.")));

    // parquet_items gets a name-only row when DESCRIBE TABLE fails or returns non-JSON.
    assertTrue(
        runTask(HIVE_METASTORE_TABLES_ENTRY_NAME, ACCEPT_ALL)
            .contains("hive_metastore,dumper_test_hms,parquet_items,,,,\n"));
    String columnsCsv = runTask(HIVE_METASTORE_COLUMNS_ENTRY_NAME, ACCEPT_ALL);
    assertFalse(columnsCsv.contains("parquet_items"));
    // delta_items's dt PartitionIndex stays empty because DESCRIBE DETAIL failed.
    assertTrue(
        columnsCsv.contains(
            "hive_metastore,dumper_test_hms,delta_items,13,dt,DATE,date,YES,,,,,,,,,,,,,,\n"));

    // When every SHOW TABLES fails, the task fails.
    stubFailed("SHOW TABLES IN `dumper_test_hms`");
    assertTrue(
        assertThrows(IOException.class, () -> runTask(HIVE_METASTORE_TABLES_ENTRY_NAME, ACCEPT_ALL))
            .getMessage()
            .contains("SHOW TABLES"));
  }

  @Test
  public void getTableJson_escapesFullNameAsPathSegment() throws Exception {
    String path = TABLES_PATH + "/main.sales.my%20orders%231%3F%25";
    server.stubFor(get(urlPathEqualTo(path)).willReturn(okJson("{\"name\":\"my orders#1?%\"}")));
    DatabricksRestHelper restHelper = new DatabricksRestHelper(newWorkspaceClient());
    assertEquals(
        "my orders#1?%",
        restHelper.getTableJson("main", "sales", "my orders#1?%").path("name").asText());
  }

  private static String runTask(Task<?> task, DatabricksHandle handle) throws Exception {
    MemoryByteSink sink = new MemoryByteSink();
    ((MetadataTask<?>) task).doRun(null, sink, handle);
    return sink.openStream().toString();
  }

  /** Runs the task of {@code targetPath} as the only task of a new run. */
  private static String runTask(String targetPath, DatabricksFilter filter) throws Exception {
    try (DatabricksHandle handle = newHandle(filter)) {
      for (Task<?> task :
          Iterables.concat(UnityCatalogTasks.create(), HiveMetastoreTasks.create())) {
        if (targetPath.equals(task.getTargetPath())) {
          return runTask(task, handle);
        }
      }
    }
    throw new AssertionError("No task for " + targetPath);
  }

  private static void assertMatchesExpectedCsv(String entryName, String actualCsv)
      throws IOException {
    String expectedCsv =
        Resources.toString(
            Resources.getResource("connector/databricks/expected/" + entryName), UTF_8);
    assertEquals(entryName, expectedCsv.replace("\r\n", "\n"), actualCsv.replace("\r\n", "\n"));
  }

  private static DatabricksHandle newHandle(DatabricksFilter filter) {
    WorkspaceClient client = newWorkspaceClient();
    return new DatabricksHandle(
        new DatabricksSqlHelper(client, "wh-test"), new DatabricksRestHelper(client), filter);
  }

  private static WorkspaceClient newWorkspaceClient() {
    return new WorkspaceClient(
        new DatabricksConfig().setHost(server.baseUrl()).setToken("test-token").setAuthType("pat"));
  }

  private static Set<Map.Entry<String, JsonNode>> readFixture(String name) throws IOException {
    return MAPPER
        .readTree(Resources.toString(Resources.getResource("connector/databricks/" + name), UTF_8))
        .properties();
  }

  private static ResponseDefinitionBuilder permissionDenied(String message) {
    return aResponse()
        .withStatus(403)
        .withHeader("Content-Type", "application/json")
        .withBody(
            String.format("{\"error_code\": \"PERMISSION_DENIED\", \"message\": \"%s\"}", message));
  }

  /**
   * Stubs a statement that succeeds with one chunk of {@code rows}, a JSON array of rows. {@code
   * columns} is the JSON array of the result columns, or null.
   */
  private static void stubStatement(
      String sql, String chunkPath, String rows, JsonNode columns, int priority) {
    server.stubFor(get(urlEqualTo(chunkPath)).willReturn(okJson(rows)));
    server.stubFor(
        post(urlEqualTo(STATEMENTS_PATH))
            .atPriority(priority)
            .withRequestBody(matchingJsonPath("$.statement", equalTo(sql)))
            .willReturn(
                okJson(
                    String.format(
                        "{\"statement_id\": \"stmt-ok\", \"status\": {\"state\": \"SUCCEEDED\"},"
                            + " \"manifest\": {\"total_chunk_count\": 1, \"schema\": {\"columns\":"
                            + " %s}}, \"result\": {\"external_links\": [{\"external_link\":"
                            + " \"%s%s\"}]}}",
                        columns, server.baseUrl(), chunkPath))));
  }

  private static void stubFailed(String sql) {
    server.stubFor(
        post(urlEqualTo(STATEMENTS_PATH))
            .atPriority(1)
            .withRequestBody(matchingJsonPath("$.statement", equalTo(sql)))
            .willReturn(
                okJson(
                    "{\"statement_id\": \"stmt-failed\", \"status\": {\"state\": \"FAILED\","
                        + " \"error\": {\"message\": \"[INSUFFICIENT_PERMISSIONS] No access.\"}}}")));
  }
}
