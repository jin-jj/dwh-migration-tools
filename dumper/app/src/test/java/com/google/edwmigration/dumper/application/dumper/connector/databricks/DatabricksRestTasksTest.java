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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.CatalogInfo;
import com.databricks.sdk.service.catalog.CatalogsAPI;
import com.databricks.sdk.service.catalog.CatalogsService;
import com.databricks.sdk.service.catalog.ColumnInfo;
import com.databricks.sdk.service.catalog.DataSourceFormat;
import com.databricks.sdk.service.catalog.ListCatalogsRequest;
import com.databricks.sdk.service.catalog.ListCatalogsResponse;
import com.databricks.sdk.service.catalog.ListSchemasResponse;
import com.databricks.sdk.service.catalog.ListTablesRequest;
import com.databricks.sdk.service.catalog.ListTablesResponse;
import com.databricks.sdk.service.catalog.SchemaInfo;
import com.databricks.sdk.service.catalog.SchemasAPI;
import com.databricks.sdk.service.catalog.SchemasService;
import com.databricks.sdk.service.catalog.TableInfo;
import com.databricks.sdk.service.catalog.TableType;
import com.databricks.sdk.service.catalog.TablesAPI;
import com.databricks.sdk.service.catalog.TablesService;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.task.MemoryByteSink;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

/**
 * Covers the Unity Catalog REST fallback tier, and in particular the metastore walk that the
 * tables, columns, views and constraints tasks share.
 */
@RunWith(JUnit4.class)
public class DatabricksRestTasksTest {

  private static final String MAIN = "main";
  private static final String EXCLUDED = "excluded_catalog";
  private static final String SALES = "sales";

  private TablesService tablesService;
  private DatabricksHandle handle;
  private TaskRunContext context;

  @Before
  public void setUp() {
    WorkspaceClient client = mock(WorkspaceClient.class);

    CatalogsService catalogsService = mock(CatalogsService.class);
    CatalogsAPI catalogsApi = mock(CatalogsAPI.class);
    when(catalogsApi.impl()).thenReturn(catalogsService);
    when(client.catalogs()).thenReturn(catalogsApi);
    when(catalogsService.list(any(ListCatalogsRequest.class)))
        .thenReturn(
            new ListCatalogsResponse()
                .setCatalogs(
                    Arrays.asList(
                        new CatalogInfo().setName(MAIN),
                        new CatalogInfo().setName(EXCLUDED),
                        // hive_metastore is not a Unity Catalog securable, but an
                        // under-provisioned workspace can still surface it here.
                        new CatalogInfo().setName("hive_metastore"))));

    SchemasService schemasService = mock(SchemasService.class);
    SchemasAPI schemasApi = mock(SchemasAPI.class);
    when(schemasApi.impl()).thenReturn(schemasService);
    when(client.schemas()).thenReturn(schemasApi);
    when(schemasService.list(
            argThat(request -> request != null && MAIN.equals(request.getCatalogName()))))
        .thenReturn(
            new ListSchemasResponse()
                .setSchemas(
                    Arrays.asList(
                        new SchemaInfo().setName(SALES),
                        new SchemaInfo().setName("information_schema"))));
    when(schemasService.list(
            argThat(request -> request != null && !MAIN.equals(request.getCatalogName()))))
        .thenReturn(new ListSchemasResponse());

    tablesService = mock(TablesService.class);
    TablesAPI tablesApi = mock(TablesAPI.class);
    when(tablesApi.impl()).thenReturn(tablesService);
    when(client.tables()).thenReturn(tablesApi);
    when(tablesService.list(any(ListTablesRequest.class))).thenReturn(new ListTablesResponse());
    when(tablesService.list(
            argThat(
                request ->
                    request != null
                        && MAIN.equals(request.getCatalogName())
                        && SALES.equals(request.getSchemaName()))))
        .thenReturn(new ListTablesResponse().setTables(Arrays.asList(orders(), orderSummary())));

    handle = new DatabricksHandle(client, "warehouse123");
    context = mock(TaskRunContext.class);
  }

  @After
  public void tearDown() throws IOException {
    handle.close();
  }

  /** A managed table with two columns, one of which is the partition key. */
  private static TableInfo orders() {
    return new TableInfo()
        .setCatalogName(MAIN)
        .setSchemaName(SALES)
        .setName("orders")
        .setFullName(MAIN + "." + SALES + ".orders")
        .setTableType(TableType.MANAGED)
        .setDataSourceFormat(DataSourceFormat.DELTA)
        .setStorageLocation("s3://bucket/orders")
        .setComment("All orders")
        .setOwner("alice@example.com")
        .setCreatedAt(1600000000000L)
        .setUpdatedAt(1700000000000L)
        .setColumns(
            Arrays.asList(
                new ColumnInfo()
                    .setName("order_id")
                    .setTypeText("bigint")
                    .setNullable(false)
                    .setPosition(0L),
                new ColumnInfo()
                    .setName("order_date")
                    .setTypeText("date")
                    .setNullable(true)
                    .setPosition(1L)
                    .setPartitionIndex(0L)));
  }

  /** A view over {@link #orders()}, with no columns reported. */
  private static TableInfo orderSummary() {
    return new TableInfo()
        .setCatalogName(MAIN)
        .setSchemaName(SALES)
        .setName("order_summary")
        .setFullName(MAIN + "." + SALES + ".order_summary")
        .setTableType(TableType.VIEW)
        .setViewDefinition("SELECT count(1) FROM main.sales.orders");
  }

  private static List<String> readLines(MemoryByteSink sink) throws IOException {
    String content = sink.openStream().toString();
    if (content.isEmpty()) {
      return Collections.emptyList();
    }
    return Arrays.asList(content.split("\\r?\\n"));
  }

  /**
   * Restricts the walk to {@code main.sales}, and separately excludes a catalog, so that both the
   * inclusion and the exclusion halves of the filter are covered.
   */
  private static DatabricksFilter scopedFilter() {
    return new DatabricksFilter(
        ImmutableList.of(MAIN), ImmutableList.of(SALES), ImmutableList.of(EXCLUDED));
  }

  private DatabricksRestTablesTask tablesTask() {
    return new DatabricksRestTablesTask(scopedFilter());
  }

  @Test
  public void tablesTask_writesOneRecordPerTable() throws Exception {
    MemoryByteSink sink = new MemoryByteSink();
    tablesTask().doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(3, lines.size());
    assertEquals(
        "TableCatalog,TableSchema,TableName,TableType,DataSourceFormat,StorageLocation,Comment,"
            + "Owner,CreatedAt,UpdatedAt",
        lines.get(0));
    assertEquals(
        "main,sales,orders,MANAGED,DELTA,s3://bucket/orders,All orders,alice@example.com,"
            + "1600000000000,1700000000000",
        lines.get(1));
    assertTrue(lines.get(2), lines.get(2).startsWith("main,sales,order_summary,VIEW,"));
  }

  @Test
  public void tablesTask_skipsFilteredCatalogsAndSchemas() throws Exception {
    tablesTask().doRun(context, new MemoryByteSink(), handle);

    ArgumentCaptor<ListTablesRequest> requests = ArgumentCaptor.forClass(ListTablesRequest.class);
    verify(tablesService, times(1)).list(requests.capture());
    ListTablesRequest request = requests.getValue();
    assertEquals(MAIN, request.getCatalogName());
    assertEquals(SALES, request.getSchemaName());
  }

  @Test
  public void columnsTask_writesOneRecordPerColumn() throws Exception {
    MemoryByteSink sink = new MemoryByteSink();
    new DatabricksRestColumnsTask(DatabricksFilter.all()).doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(3, lines.size());
    assertEquals(
        "TableCatalog,TableSchema,TableName,OrdinalPosition,ColumnName,DataType,IsNullable,Comment,"
            + "PartitionIndex",
        lines.get(0));
    // The REST API reports a 0-based position, which is republished 1-based.
    assertEquals("main,sales,orders,1,order_id,bigint,false,,", lines.get(1));
    assertEquals("main,sales,orders,2,order_date,date,true,,0", lines.get(2));
  }

  @Test
  public void viewsTask_writesOnlyViews() throws Exception {
    MemoryByteSink sink = new MemoryByteSink();
    new DatabricksRestViewsTask(DatabricksFilter.all()).doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(2, lines.size());
    assertEquals("TableCatalog,TableSchema,TableName,ViewDefinition", lines.get(0));
    assertEquals("main,sales,order_summary,SELECT count(1) FROM main.sales.orders", lines.get(1));
  }

  /** The point of the shared listing: three tasks, one walk of the metastore. */
  @Test
  public void tasksSharingAHandle_walkTheMetastoreOnce() throws Exception {
    tablesTask().doRun(context, new MemoryByteSink(), handle);
    new DatabricksRestColumnsTask(DatabricksFilter.all())
        .doRun(context, new MemoryByteSink(), handle);
    new DatabricksRestViewsTask(DatabricksFilter.all())
        .doRun(context, new MemoryByteSink(), handle);

    verify(tablesService, times(1)).list(any(ListTablesRequest.class));
  }

  @Test
  public void rawTablesTask_writesOneJsonDocumentPerTable() throws Exception {
    MemoryByteSink sink = new MemoryByteSink();
    new DatabricksRestRawTablesTask(scopedFilter()).doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(2, lines.size());

    ObjectMapper mapper =
        new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    TableInfo first = mapper.readValue(lines.get(0), TableInfo.class);
    assertEquals(orders().getFullName(), first.getFullName());
    assertEquals(orders().getTableType(), first.getTableType());
    assertEquals(
        mapper.readValue(lines.get(1), TableInfo.class).getFullName(),
        orderSummary().getFullName());
  }

  /**
   * The reason the raw entry exists: the CSV projection drops fields the API returned.
   *
   * <p>Columns are the cheapest thing to point at, since {@code tables.csv} has no column for them
   * at all, and a consumer reading only the CSVs would have to join two files to recover what one
   * line of this entry already carries.
   */
  @Test
  public void rawTablesTask_keepsFieldsTheCsvProjectionDrops() throws Exception {
    MemoryByteSink sink = new MemoryByteSink();
    new DatabricksRestRawTablesTask(scopedFilter()).doRun(context, sink, handle);

    TableInfo table =
        new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .readValue(readLines(sink).get(0), TableInfo.class);
    assertEquals(orders().getColumns().size(), table.getColumns().size());
    assertEquals("order_id", table.getColumns().iterator().next().getName());
  }

  /** The raw entry has to be free: it republishes the walk, it must not provoke another. */
  @Test
  public void rawTablesTask_reusesTheWalkOfTheOtherRestTasks() throws Exception {
    tablesTask().doRun(context, new MemoryByteSink(), handle);
    new DatabricksRestRawTablesTask(scopedFilter()).doRun(context, new MemoryByteSink(), handle);

    verify(tablesService, times(1)).list(any(ListTablesRequest.class));
  }

  /**
   * Table properties are the headline of what REST can see and SQL cannot, so the walk must not ask
   * the API to omit them.
   */
  @Test
  public void metastoreWalk_requestsTableProperties() throws Exception {
    tablesTask().doRun(context, new MemoryByteSink(), handle);

    ArgumentCaptor<ListTablesRequest> requests = ArgumentCaptor.forClass(ListTablesRequest.class);
    verify(tablesService, times(1)).list(requests.capture());
    assertFalse(Boolean.TRUE.equals(requests.getValue().getOmitProperties()));
  }

  @Test
  public void close_deletesTheTableListing() throws Exception {
    Path listing = handle.tableListing(file -> {});
    assertTrue(Files.exists(listing));

    handle.close();

    assertFalse(Files.exists(listing));
  }
}
