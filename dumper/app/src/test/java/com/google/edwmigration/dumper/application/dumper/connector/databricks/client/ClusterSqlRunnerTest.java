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
import java.util.Arrays;
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
  public void clusterRunner_failsOnErrorAndTruncatedResults() {
    String describe = "DESCRIBE TABLE EXTENDED `s`.`t` AS JSON";
    stubCommand(
        describe,
        "{\"resultType\": \"error\", \"summary\": \"ParseException\", \"cause\": \"[PARSE_SYNTAX_ERROR]"
            + " Syntax error at or near 'JSON': extra input 'JSON'.\"}");
    stubCommand(
        "SHOW TABLES IN `big`",
        "{\"resultType\": \"table\", \"truncated\": true, \"data\": [[\"big\", \"t1\", false]]}");

    try (ClusterSqlRunner runner = newRunner()) {
      // The error class stays in the message, so that HiveMetastoreTasks can recognize it.
      IOException error =
          assertThrows(IOException.class, () -> runner.executeSqlInCatalog(CATALOG, describe));
      assertTrue(error.getMessage(), error.getMessage().contains("[PARSE_SYNTAX_ERROR]"));
      IOException truncated =
          assertThrows(
              IOException.class, () -> runner.executeSqlInCatalog(CATALOG, "SHOW TABLES IN `big`"));
      assertTrue(truncated.getMessage(), truncated.getMessage().contains("truncated"));
    }
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
    wireMock.stubFor(
        get(urlPathEqualTo(COMMANDS_PATH + "/status"))
            .withQueryParam("commandId", equalTo(commandId))
            .willReturn(
                okJson(
                    String.format(
                        "{\"id\": \"%s\", \"status\": \"Finished\", \"results\": %s}",
                        commandId, results))));
  }

  private ClusterSqlRunner newRunner() {
    return new ClusterSqlRunner(
        new WorkspaceClient(
            new DatabricksConfig()
                .setHost(wireMock.baseUrl())
                .setToken("test-token")
                .setAuthType("pat")),
        CLUSTER_ID);
  }
}
