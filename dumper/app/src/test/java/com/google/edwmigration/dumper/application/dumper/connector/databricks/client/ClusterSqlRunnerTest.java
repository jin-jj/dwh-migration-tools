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

import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ClusterSqlRunnerTest {

  private static final String CLUSTER_ID = "0123-456789-abcdefgh";
  private static final String CONTEXT_ID = "ctx-1";
  private static final String CATALOG = "hive_metastore";
  private static final String USE_CATALOG = "USE CATALOG `hive_metastore`";
  private static final String CONTEXTS_PATH = "/api/1.2/contexts";
  private static final String COMMANDS_PATH = "/api/1.2/commands";

  private WireMockServer wireMock;
  private int commandCount;

  @Before
  public void setUp() {
    wireMock = new WireMockServer(wireMockConfig().dynamicPort());
    wireMock.start();
    wireMock.stubFor(
        post(urlPathEqualTo(CONTEXTS_PATH + "/create"))
            .willReturn(okJson("{\"id\": \"" + CONTEXT_ID + "\"}")));
    wireMock.stubFor(
        get(urlPathEqualTo(CONTEXTS_PATH + "/status"))
            .willReturn(okJson("{\"id\": \"" + CONTEXT_ID + "\", \"status\": \"Running\"}")));
    wireMock.stubFor(post(urlPathEqualTo(CONTEXTS_PATH + "/destroy")).willReturn(okJson("{}")));
    stubCommand(USE_CATALOG, "{\"resultType\": \"table\", \"data\": [], \"schema\": []}");
  }

  @After
  public void tearDown() {
    wireMock.stop();
  }

  @Test
  public void clusterRunner_readsCellsAsTextAndSetsCatalogOnce() throws Exception {
    stubCommand(
        "SHOW TABLES IN `s`",
        "{\"resultType\": \"table\", \"truncated\": false, \"schema\": [{\"name\": \"database\"},"
            + " {\"name\": \"tableName\"}, {\"name\": \"isTemporary\"}],"
            + " \"data\": [[\"s\", \"orders\", false]]}");
    stubCommand(
        "DESCRIBE DETAIL `s`.`orders`",
        "{\"resultType\": \"table\", \"schema\": [{\"name\": \"partitionColumns\"},"
            + " {\"name\": \"sizeInBytes\"}, {\"name\": \"properties\"}, {\"name\": \"location\"}],"
            + " \"data\": [[[\"dt\"], 1024, {\"a\": \"b\"}, null]]}");

    try (ClusterSqlRunner runner = newRunner()) {
      StatementResult tables = runner.executeSqlInCatalog(CATALOG, "SHOW TABLES IN `s`");
      assertEquals(ImmutableList.of("database", "tableName", "isTemporary"), tables.columnNames());
      assertEquals(ImmutableList.of("s", "orders", "false"), tables.nextRow());
      assertNull(tables.nextRow());
      // An array or a map is JSON, as a SQL warehouse prints it.
      assertEquals(
          Arrays.asList("[\"dt\"]", "1024", "{\"a\":\"b\"}", null),
          runner.executeSqlInCatalog(CATALOG, "DESCRIBE DETAIL `s`.`orders`").nextRow());
    }

    wireMock.verify(
        1,
        postRequestedFor(urlPathEqualTo(COMMANDS_PATH + "/execute"))
            .withRequestBody(matchingJsonPath("$.command", equalTo(USE_CATALOG))));
    wireMock.verify(
        1,
        postRequestedFor(urlPathEqualTo(CONTEXTS_PATH + "/create"))
            .withRequestBody(matchingJsonPath("$.clusterId", equalTo(CLUSTER_ID)))
            .withRequestBody(matchingJsonPath("$.language", equalTo("sql"))));
    wireMock.verify(
        1,
        postRequestedFor(urlPathEqualTo(CONTEXTS_PATH + "/destroy"))
            .withRequestBody(matchingJsonPath("$.contextId", equalTo(CONTEXT_ID))));
  }

  @Test
  public void clusterRunner_failsOnError() {
    String describe = "DESCRIBE TABLE EXTENDED `s`.`t` AS JSON";
    stubCommand(
        describe,
        "{\"resultType\": \"error\", \"summary\": \"[PARSE_SYNTAX_ERROR] Syntax error at or near"
            + " 'JSON': extra input 'JSON'.\", \"cause\": \"\\u001b[0;31m-----\\u001b[0m\\n"
            + "\\u001b[0;31mParseException\\u001b[0m   Traceback\"}");

    try (ClusterSqlRunner runner = newRunner()) {
      // The error class stays in the message, so that HiveMetastoreTasks can recognize it.
      IOException error =
          assertThrows(IOException.class, () -> runner.executeSqlInCatalog(CATALOG, describe));
      assertTrue(error.getMessage(), error.getMessage().contains("[PARSE_SYNTAX_ERROR]"));
      // The first line of the cause, without the separator line and the ANSI colors.
      assertTrue(error.getMessage(), error.getMessage().endsWith("(ParseException Traceback)"));
    }
  }

  @Test
  public void clusterRunner_readsTruncatedResultInPages() throws Exception {
    String show = "SHOW TABLES IN `big`";
    stubCommand(
        show,
        "{\"resultType\": \"table\", \"truncated\": true, \"data\": [[\"big\", \"t1\", false]]}");
    // A Python command stores the result in a temporary view, which SQL commands read in pages.
    wireMock.stubFor(
        post(urlPathEqualTo(COMMANDS_PATH + "/execute"))
            .withRequestBody(matchingJsonPath("$.language", equalTo("python")))
            .willReturn(okJson("{\"id\": \"cmd-python\"}")));
    stubStatus("cmd-python", "{\"resultType\": \"text\", \"data\": \"\"}");
    String schema =
        "\"schema\": [{\"name\": \"database\"}, {\"name\": \"tableName\"},"
            + " {\"name\": \"isTemporary\"}]";
    stubCommand(
        pageQuery(2, 0),
        "{\"resultType\": \"table\", "
            + schema
            + ", \"data\": [[\"big\", \"t1\", false], [\"big\", \"t2\", false]]}");
    stubCommand(
        pageQuery(2, 2),
        "{\"resultType\": \"table\", " + schema + ", \"data\": [[\"big\", \"t3\", false]]}");
    stubCommand(
        "DROP VIEW IF EXISTS dwh_dumper_result", "{\"resultType\": \"table\", \"data\": []}");

    ImmutableList.Builder<List<String>> rows = ImmutableList.builder();
    try (ClusterSqlRunner runner = newRunner(2)) {
      StatementResult tables = runner.executeSqlInCatalog(CATALOG, show);
      assertEquals(ImmutableList.of("database", "tableName", "isTemporary"), tables.columnNames());
      for (List<String> row = tables.nextRow(); row != null; row = tables.nextRow()) {
        rows.add(row);
      }
    }

    assertEquals(
        ImmutableList.of(
            ImmutableList.of("big", "t1", "false"),
            ImmutableList.of("big", "t2", "false"),
            ImmutableList.of("big", "t3", "false")),
        rows.build());
    // The statement is passed to spark.sql() as a Python string literal.
    wireMock.verify(
        1,
        postRequestedFor(urlPathEqualTo(COMMANDS_PATH + "/execute"))
            .withRequestBody(
                matchingJsonPath("$.command", containing("spark.sql(\"SHOW TABLES IN `big`\")"))));
    wireMock.verify(
        1,
        postRequestedFor(urlPathEqualTo(COMMANDS_PATH + "/execute"))
            .withRequestBody(
                matchingJsonPath("$.command", equalTo("DROP VIEW IF EXISTS dwh_dumper_result"))));
  }

  @Test
  public void clusterRunner_closeWithoutStatementCreatesNoContext() {
    newRunner().close();

    wireMock.verify(0, postRequestedFor(urlPathEqualTo(CONTEXTS_PATH + "/create")));
    wireMock.verify(0, postRequestedFor(urlPathEqualTo(CONTEXTS_PATH + "/destroy")));
  }

  /** Stubs a command that finishes with {@code results}, the JSON object of its results. */
  private void stubCommand(String sql, String results) {
    String commandId = "cmd-" + ++commandCount;
    wireMock.stubFor(
        post(urlPathEqualTo(COMMANDS_PATH + "/execute"))
            .withRequestBody(matchingJsonPath("$.command", equalTo(sql)))
            .willReturn(okJson("{\"id\": \"" + commandId + "\"}")));
    stubStatus(commandId, results);
  }

  private void stubStatus(String commandId, String results) {
    wireMock.stubFor(
        get(urlPathEqualTo(COMMANDS_PATH + "/status"))
            .withQueryParam("commandId", equalTo(commandId))
            .willReturn(
                okJson(
                    String.format(
                        "{\"id\": \"%s\", \"status\": \"Finished\", \"results\": %s}",
                        commandId, results))));
  }

  private static String pageQuery(int pageRows, int offset) {
    return String.format(
        "SELECT * EXCEPT (_dwh_dumper_row) FROM dwh_dumper_result ORDER BY _dwh_dumper_row"
            + " LIMIT %d OFFSET %d",
        pageRows, offset);
  }

  private ClusterSqlRunner newRunner() {
    return new ClusterSqlRunner(newClient(), CLUSTER_ID);
  }

  private ClusterSqlRunner newRunner(int pageRows) {
    return new ClusterSqlRunner(newClient(), CLUSTER_ID, Duration.ofMinutes(1), pageRows);
  }

  private WorkspaceClient newClient() {
    return new WorkspaceClient(
        new DatabricksConfig()
            .setHost(wireMock.baseUrl())
            .setToken("test-token")
            .setAuthType("pat"));
  }
}
