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

import static com.github.tomakehurst.wiremock.client.WireMock.absent;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestHelper.CURRENT_USER_PATH;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestHelper.INITIAL_BACKOFF_MILLIS;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestHelper.MAX_ATTEMPTS;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestHelper.MAX_BACKOFF_MILLIS;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestHelper.WORKSPACE_ID_HEADER;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestHelper.backoffMillis;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestHelper.isRetryable;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestStubs.CATALOGS_PATH;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestStubs.FUNCTIONS_PATH;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestStubs.QUERY_HISTORY_PATH;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestStubs.SCHEMAS_PATH;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestStubs.TABLES_PATH;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestStubs.UNTHROTTLED_REQUESTS_PER_SECOND;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestStubs.error;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestStubs.newWorkspaceClient;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestStubs.okModel;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestStubs.throttled;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.databricks.sdk.service.catalog.CatalogInfo;
import com.databricks.sdk.service.catalog.FunctionInfo;
import com.databricks.sdk.service.catalog.ListCatalogsResponse;
import com.databricks.sdk.service.catalog.ListFunctionsResponse;
import com.databricks.sdk.service.catalog.ListSchemasResponse;
import com.databricks.sdk.service.catalog.ListTablesResponse;
import com.databricks.sdk.service.catalog.SchemaInfo;
import com.databricks.sdk.service.catalog.TableInfo;
import com.databricks.sdk.service.sql.ListQueriesResponse;
import com.databricks.sdk.service.sql.QueryInfo;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksChunkDownloader.Sleeper;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksRestHelperTest {

  private static final String CATALOG = "main";
  private static final String SCHEMA = "sales";
  private static final String TABLE = "orders";
  private static final String TABLE_PATH = TABLES_PATH + "/main.sales.orders";

  /** The backoffs sampled per attempt, because a backoff is random. */
  private static final int BACKOFF_SAMPLES = 100;

  private static WireMockServer server;

  /** The backoffs that the helper waited for, in milliseconds. */
  private final List<Long> sleeps = new ArrayList<>();

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
  public void listCatalogs_followsPageTokens() throws Exception {
    String secondPageToken = "page-2";
    server.stubFor(
        get(urlPathEqualTo(CATALOGS_PATH))
            .withQueryParam("page_token", absent())
            .willReturn(
                okModel(
                    new ListCatalogsResponse()
                        .setCatalogs(ImmutableList.of(new CatalogInfo().setName(CATALOG)))
                        .setNextPageToken(secondPageToken))));
    server.stubFor(
        get(urlPathEqualTo(CATALOGS_PATH))
            .withQueryParam("page_token", equalTo(secondPageToken))
            .willReturn(
                okModel(
                    new ListCatalogsResponse()
                        .setCatalogs(ImmutableList.of(new CatalogInfo().setName("finance"))))));

    ImmutableList<CatalogInfo> catalogs = newRestHelper().listCatalogs();

    List<String> catalogNames = new ArrayList<>();
    for (CatalogInfo catalog : catalogs) {
      catalogNames.add(catalog.getName());
    }

    assertEquals(ImmutableList.of(CATALOG, "finance"), catalogNames);
    server.verify(
        2,
        getRequestedFor(urlPathEqualTo(CATALOGS_PATH))
            .withQueryParam("include_browse", equalTo("true"))
            .withQueryParam("max_results", equalTo("0")));
    assertEquals(ImmutableList.of(), sleeps);
  }

  @Test
  public void listSchemas_requestsTheSchemasOfTheCatalog() throws Exception {
    server.stubFor(
        get(urlPathEqualTo(SCHEMAS_PATH))
            .willReturn(
                okModel(
                    new ListSchemasResponse()
                        .setSchemas(ImmutableList.of(new SchemaInfo().setName(SCHEMA))))));

    ImmutableList<SchemaInfo> schemas = newRestHelper().listSchemas(CATALOG);

    assertEquals(SCHEMA, getOnlyElement(schemas).getName());
    server.verify(
        getRequestedFor(urlPathEqualTo(SCHEMAS_PATH))
            .withQueryParam("catalog_name", equalTo(CATALOG))
            .withQueryParam("include_browse", equalTo("true"))
            .withQueryParam("max_results", equalTo("0")));
  }

  @Test
  public void listTables_requestsTheTablesOfTheSchemaWithTheirManifests() throws Exception {
    server.stubFor(
        get(urlPathEqualTo(TABLES_PATH))
            .willReturn(
                okModel(
                    new ListTablesResponse()
                        .setTables(ImmutableList.of(new TableInfo().setName(TABLE))))));

    ImmutableList<TableInfo> tables = newRestHelper().listTables(CATALOG, SCHEMA);

    assertEquals(TABLE, getOnlyElement(tables).getName());
    server.verify(
        getRequestedFor(urlPathEqualTo(TABLES_PATH))
            .withQueryParam("catalog_name", equalTo(CATALOG))
            .withQueryParam("schema_name", equalTo(SCHEMA))
            .withQueryParam("include_browse", equalTo("true"))
            .withQueryParam("include_manifest_capabilities", equalTo("true"))
            .withQueryParam("max_results", equalTo("0")));
  }

  @Test
  public void listFunctions_requestsTheFunctionsOfTheSchema() throws Exception {
    String functionName = "add_tax";
    server.stubFor(
        get(urlPathEqualTo(FUNCTIONS_PATH))
            .willReturn(
                okModel(
                    new ListFunctionsResponse()
                        .setFunctions(
                            ImmutableList.of(new FunctionInfo().setName(functionName))))));

    ImmutableList<FunctionInfo> functions = newRestHelper().listFunctions(CATALOG, SCHEMA);

    assertEquals(functionName, getOnlyElement(functions).getName());
    server.verify(
        getRequestedFor(urlPathEqualTo(FUNCTIONS_PATH))
            .withQueryParam("catalog_name", equalTo(CATALOG))
            .withQueryParam("schema_name", equalTo(SCHEMA))
            .withQueryParam("include_browse", equalTo("true"))
            .withQueryParam("max_results", equalTo("0")));
  }

  @Test
  public void getTable_requestsTheTableByItsFullName() throws Exception {
    server.stubFor(
        get(urlPathEqualTo(TABLE_PATH)).willReturn(okModel(new TableInfo().setName(TABLE))));

    TableInfo table = newRestHelper().getTable(CATALOG, SCHEMA, TABLE);

    assertEquals(TABLE, table.getName());
    server.verify(
        getRequestedFor(urlPathEqualTo(TABLE_PATH))
            .withQueryParam("include_browse", equalTo("true")));
  }

  @Test
  public void listCatalogs_rejectsARepeatedPageToken() {
    String pageToken = "same-page";
    server.stubFor(
        get(urlPathEqualTo(CATALOGS_PATH))
            .willReturn(okModel(new ListCatalogsResponse().setNextPageToken(pageToken))));
    DatabricksRestHelper restHelper = newRestHelper();

    IOException exception = assertThrows(IOException.class, restHelper::listCatalogs);

    assertTrue(exception.getMessage(), exception.getMessage().contains(pageToken));
  }

  @Test
  public void listCatalogs_retriesAThrottledRequestAfterTheSdkGivesUp() throws Exception {
    server.stubFor(get(urlPathEqualTo(CATALOGS_PATH)).willReturn(throttled()));
    DatabricksRestHelper restHelper =
        newRestHelper(
            millis -> {
              sleeps.add(millis);
              server.stubFor(
                  get(urlPathEqualTo(CATALOGS_PATH))
                      .willReturn(
                          okModel(
                              new ListCatalogsResponse()
                                  .setCatalogs(
                                      ImmutableList.of(new CatalogInfo().setName(CATALOG))))));
            });

    ImmutableList<CatalogInfo> catalogs = restHelper.listCatalogs();

    assertEquals(CATALOG, getOnlyElement(catalogs).getName());
    long backoff = getOnlyElement(sleeps);
    assertTrue(
        String.valueOf(backoff),
        backoff >= INITIAL_BACKOFF_MILLIS / 2 && backoff <= INITIAL_BACKOFF_MILLIS);
  }

  @Test
  public void listCatalogs_failsWhenTheLastAttemptIsThrottled() {
    server.stubFor(get(urlPathEqualTo(CATALOGS_PATH)).willReturn(throttled()));
    DatabricksRestHelper restHelper = newRestHelper();

    IOException exception = assertThrows(IOException.class, restHelper::listCatalogs);

    assertEquals(MAX_ATTEMPTS - 1, sleeps.size());
    assertTrue(isRetryable(exception));
    assertTrue(
        exception.getMessage(), exception.getMessage().startsWith("Listing catalogs failed"));
  }

  @Test
  public void getTable_doesNotRetryAPermissionError() {
    server.stubFor(
        get(urlPathEqualTo(TABLE_PATH))
            .willReturn(error(403, "PERMISSION_DENIED", "User does not have USE SCHEMA.")));
    DatabricksRestHelper restHelper = newRestHelper();

    IOException exception =
        assertThrows(IOException.class, () -> restHelper.getTable(CATALOG, SCHEMA, TABLE));

    assertEquals(ImmutableList.of(), sleeps);
    server.verify(1, getRequestedFor(urlPathEqualTo(TABLE_PATH)));
    assertFalse(isRetryable(exception));
    assertTrue(exception.getMessage(), exception.getMessage().contains("`main`.`sales`.`orders`"));
  }

  @Test
  public void listCatalogs_stopsWhenInterruptedWhileWaitingToRetry() {
    server.stubFor(get(urlPathEqualTo(CATALOGS_PATH)).willReturn(throttled()));
    DatabricksRestHelper restHelper =
        newRestHelper(
            millis -> {
              throw new InterruptedException("Interrupted by the test.");
            });

    assertThrows(InterruptedIOException.class, restHelper::listCatalogs);
    assertTrue(Thread.interrupted());
  }

  @Test
  public void findWorkspaceId_readsTheWorkspaceIdHeader() throws Exception {
    String workspaceId = "1234567890123456";
    server.stubFor(
        get(urlPathEqualTo(CURRENT_USER_PATH))
            .willReturn(
                okJson("{\"userName\": \"user@example.com\"}")
                    .withHeader(WORKSPACE_ID_HEADER, workspaceId)));

    assertEquals(workspaceId, newRestHelper().findWorkspaceId());
  }

  @Test
  public void findWorkspaceId_returnsNullWithoutTheHeader() throws Exception {
    server.stubFor(get(urlPathEqualTo(CURRENT_USER_PATH)).willReturn(okJson("{}")));

    assertNull(newRestHelper().findWorkspaceId());
  }

  @Test
  public void listQueryHistory_requestsTheEndedQueriesOfTheRangePageByPage() throws Exception {
    long startTimeMillis = Instant.parse("2026-10-09T13:00:00Z").toEpochMilli();
    long endTimeMillis = startTimeMillis + Duration.ofHours(1).toMillis();
    String secondPageToken = "page-2";
    server.stubFor(
        get(urlPathEqualTo(QUERY_HISTORY_PATH))
            .withQueryParam("page_token", absent())
            .willReturn(
                okModel(
                    new ListQueriesResponse()
                        .setRes(ImmutableList.of(new QueryInfo().setQueryId("q-1")))
                        .setHasNextPage(true)
                        .setNextPageToken(secondPageToken))));
    server.stubFor(
        get(urlPathEqualTo(QUERY_HISTORY_PATH))
            .withQueryParam("page_token", equalTo(secondPageToken))
            .willReturn(
                okModel(
                    new ListQueriesResponse()
                        .setRes(ImmutableList.of(new QueryInfo().setQueryId("q-2")))
                        .setHasNextPage(false)
                        .setNextPageToken("page-3"))));
    List<List<String>> pages = new ArrayList<>();

    newRestHelper()
        .listQueryHistory(
            startTimeMillis,
            endTimeMillis,
            queries -> {
              List<String> queryIds = new ArrayList<>();
              for (QueryInfo query : queries) {
                queryIds.add(query.getQueryId());
              }
              pages.add(queryIds);
            });

    assertEquals(ImmutableList.of(ImmutableList.of("q-1"), ImmutableList.of("q-2")), pages);
    List<LoggedRequest> requests =
        server.findAll(getRequestedFor(urlPathEqualTo(QUERY_HISTORY_PATH)));
    assertEquals(2, requests.size());
    for (LoggedRequest request : requests) {
      assertEquals(
          String.valueOf(startTimeMillis),
          request.queryParameter("filter_by.query_start_time_range.start_time_ms").firstValue());
      assertEquals(
          String.valueOf(endTimeMillis),
          request.queryParameter("filter_by.query_start_time_range.end_time_ms").firstValue());
      assertEquals(
          ImmutableSet.of("FINISHED", "FAILED", "CANCELED"),
          ImmutableSet.copyOf(request.queryParameter("filter_by.statuses").values()));
      assertEquals("true", request.queryParameter("include_metrics").firstValue());
    }
  }

  @Test
  public void backoffMillis_isBetweenHalfAndAllOfACeilingThatDoubles() {
    long ceiling = INITIAL_BACKOFF_MILLIS;
    for (int attempt = 1; attempt < MAX_ATTEMPTS; attempt++) {
      assertBackoffsBetween(attempt, ceiling / 2, ceiling);
      ceiling = Math.min(2 * ceiling, MAX_BACKOFF_MILLIS);
    }
  }

  @Test
  public void backoffMillis_doesNotExceedTheMaximum() {
    assertBackoffsBetween(Integer.MAX_VALUE, MAX_BACKOFF_MILLIS / 2, MAX_BACKOFF_MILLIS);
  }

  @Test
  public void backoffMillis_rejectsAnAttemptBeforeTheFirst() {
    assertThrows(IllegalArgumentException.class, () -> backoffMillis(0));
  }

  private static void assertBackoffsBetween(int attempt, long minMillis, long maxMillis) {
    for (int sample = 0; sample < BACKOFF_SAMPLES; sample++) {
      long backoff = backoffMillis(attempt);
      assertTrue(
          String.format(
              "Backoff %d of attempt %d is not in [%d, %d].",
              backoff, attempt, minMillis, maxMillis),
          backoff >= minMillis && backoff <= maxMillis);
    }
  }

  private DatabricksRestHelper newRestHelper() {
    return newRestHelper(sleeps::add);
  }

  private static DatabricksRestHelper newRestHelper(Sleeper sleeper) {
    return new DatabricksRestHelper(
        newWorkspaceClient(server.baseUrl()), UNTHROTTLED_REQUESTS_PER_SECOND, sleeper);
  }
}
