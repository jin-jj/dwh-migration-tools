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
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksFilter.HIVE_METASTORE_CATALOG;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestStubs.CATALOGS_PATH;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestStubs.FUNCTIONS_PATH;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestStubs.SCHEMAS_PATH;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestStubs.TABLES_PATH;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestStubs.UNTHROTTLED_REQUESTS_PER_SECOND;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestStubs.error;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestStubs.newWorkspaceClient;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestStubs.okModel;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.quoteIdentifier;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.acceptAllFilter;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.failed;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.newHandle;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.readRecords;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.succeededRows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.CatalogInfo;
import com.databricks.sdk.service.catalog.CatalogType;
import com.databricks.sdk.service.catalog.ColumnInfo;
import com.databricks.sdk.service.catalog.ColumnMask;
import com.databricks.sdk.service.catalog.DataSourceFormat;
import com.databricks.sdk.service.catalog.ForeignKeyConstraint;
import com.databricks.sdk.service.catalog.FunctionInfo;
import com.databricks.sdk.service.catalog.FunctionInfoRoutineBody;
import com.databricks.sdk.service.catalog.FunctionInfoSecurityType;
import com.databricks.sdk.service.catalog.FunctionInfoSqlDataAccess;
import com.databricks.sdk.service.catalog.FunctionParameterInfo;
import com.databricks.sdk.service.catalog.FunctionParameterInfos;
import com.databricks.sdk.service.catalog.FunctionParameterMode;
import com.databricks.sdk.service.catalog.ListCatalogsResponse;
import com.databricks.sdk.service.catalog.ListFunctionsResponse;
import com.databricks.sdk.service.catalog.ListSchemasResponse;
import com.databricks.sdk.service.catalog.ListTablesResponse;
import com.databricks.sdk.service.catalog.PolicyFunctionArgument;
import com.databricks.sdk.service.catalog.PrimaryKeyConstraint;
import com.databricks.sdk.service.catalog.SchemaInfo;
import com.databricks.sdk.service.catalog.SecurableKind;
import com.databricks.sdk.service.catalog.SecurableKindManifest;
import com.databricks.sdk.service.catalog.SecurableType;
import com.databricks.sdk.service.catalog.TableConstraint;
import com.databricks.sdk.service.catalog.TableInfo;
import com.databricks.sdk.service.catalog.TableRowFilter;
import com.databricks.sdk.service.catalog.TableType;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import com.google.common.io.Files;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.CatalogsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.ColumnsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.FunctionsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.SchemataHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.TableConstraintsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.TablesHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.ViewsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.TaskBody;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogTasks.DatabricksUnityCatalogCatalogsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogTasks.DatabricksUnityCatalogColumnsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogTasks.DatabricksUnityCatalogFunctionsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogTasks.DatabricksUnityCatalogSchemataTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogTasks.DatabricksUnityCatalogTableConstraintsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogTasks.DatabricksUnityCatalogTablesTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogTasks.DatabricksUnityCatalogViewsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogTasks.UnityCatalogSessionCache;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVRecord;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksUnityCatalogTasksTest {

  private static final String CATALOG = "main";
  private static final String SCHEMA = "sales";
  private static final String STATEMENTS_PATH = "/api/2.0/sql/statements";
  private static final long CREATED_AT = 1_758_000_000_000L;
  private static final long UPDATED_AT = 1_759_000_000_123L;
  private static final ObjectMapper JSON = new ObjectMapper();

  private static WireMockServer server;

  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

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
  public void catalogsTask_writesCatalogsExceptHiveMetastoreAndDatabricksManaged()
      throws Exception {
    String storageRoot = "s3://lake/main";
    String storageLocation = storageRoot + "/__unitystorage/catalogs/1";
    ImmutableMap<String, String> properties = ImmutableMap.of("team", "sales", "domain", "retail");
    ImmutableMap<String, String> options = ImmutableMap.of("database", "crm");
    stubCatalogs(
        catalog(HIVE_METASTORE_CATALOG),
        catalog(CATALOG)
            .setSecurableType(SecurableType.CATALOG)
            .setCatalogType(CatalogType.MANAGED_CATALOG)
            .setComment("Main data")
            .setOwner("admins")
            .setCreatedAt(CREATED_AT)
            .setUpdatedAt(UPDATED_AT)
            .setStorageRoot(storageRoot)
            .setStorageLocation(storageLocation)
            .setProperties(properties),
        catalog("crm")
            .setCatalogType(CatalogType.FOREIGN_CATALOG)
            .setConnectionName("postgres_crm")
            .setOptions(options),
        catalog("partner")
            .setCatalogType(CatalogType.DELTASHARING_CATALOG)
            .setProviderName("acme")
            .setShareName("orders_share"),
        catalog("system").setCatalogType(CatalogType.SYSTEM_CATALOG),
        catalog("samples"),
        catalog("__databricks_internal").setCatalogType(CatalogType.INTERNAL_CATALOG));

    List<CSVRecord> records =
        runTask(
            (sink, handle) ->
                new DatabricksUnityCatalogCatalogsTask(new UnityCatalogSessionCache())
                    .doRun(null, sink, handle));

    assertEquals(
        ImmutableList.of(CATALOG, "crm", "partner"),
        columnValues(records, CatalogsHeader.CatalogName));
    CSVRecord managed = records.get(0);
    assertEquals(SecurableType.CATALOG.name(), managed.get(CatalogsHeader.SecurableType));
    assertEquals(CatalogType.MANAGED_CATALOG.name(), managed.get(CatalogsHeader.CatalogType));
    assertEquals("Main data", managed.get(CatalogsHeader.Comment));
    assertEquals("admins", managed.get(CatalogsHeader.Owner));
    assertEquals(
        Instant.ofEpochMilli(CREATED_AT).toString(), managed.get(CatalogsHeader.CreatedAt));
    assertEquals(
        Instant.ofEpochMilli(UPDATED_AT).toString(), managed.get(CatalogsHeader.UpdatedAt));
    assertEquals(storageRoot, managed.get(CatalogsHeader.StorageRoot));
    assertEquals(storageLocation, managed.get(CatalogsHeader.StorageLocation));
    assertEquals("", managed.get(CatalogsHeader.Options));
    assertEquals(properties, parseJsonObject(managed.get(CatalogsHeader.Properties)));
    CSVRecord foreign = records.get(1);
    assertEquals(CatalogType.FOREIGN_CATALOG.name(), foreign.get(CatalogsHeader.CatalogType));
    assertEquals("postgres_crm", foreign.get(CatalogsHeader.ConnectionName));
    assertEquals(options, parseJsonObject(foreign.get(CatalogsHeader.Options)));
    CSVRecord shared = records.get(2);
    assertEquals("acme", shared.get(CatalogsHeader.ProviderName));
    assertEquals("orders_share", shared.get(CatalogsHeader.ShareName));
  }

  @Test
  public void schemataTask_skipsInformationSchemaAndCatalogsWhoseSchemasFailToList()
      throws Exception {
    String storageRoot = "s3://lake/sales";
    String storageLocation = storageRoot + "/__unitystorage/schemas/2";
    ImmutableMap<String, String> properties = ImmutableMap.of("team", "sales");
    stubCatalogs(catalog(CATALOG), catalog("restricted"));
    stubSchemas(
        CATALOG,
        schema(SCHEMA)
            .setComment("Sales data")
            .setOwner("alice")
            .setCreatedAt(CREATED_AT)
            .setUpdatedAt(UPDATED_AT)
            .setStorageRoot(storageRoot)
            .setStorageLocation(storageLocation)
            .setProperties(properties),
        schema("information_schema"));
    server.stubFor(
        get(urlPathEqualTo(SCHEMAS_PATH))
            .withQueryParam("catalog_name", equalTo("restricted"))
            .willReturn(error(403, "PERMISSION_DENIED", "User does not have USE CATALOG.")));

    List<CSVRecord> records =
        runTask(
            (sink, handle) ->
                new DatabricksUnityCatalogSchemataTask(new UnityCatalogSessionCache())
                    .doRun(null, sink, handle));

    CSVRecord record = getOnlyElement(records);
    assertEquals(CATALOG, record.get(SchemataHeader.CatalogName));
    assertEquals(SCHEMA, record.get(SchemataHeader.SchemaName));
    assertEquals("Sales data", record.get(SchemataHeader.Comment));
    assertEquals("alice", record.get(SchemataHeader.Owner));
    assertEquals(Instant.ofEpochMilli(CREATED_AT).toString(), record.get(SchemataHeader.CreatedAt));
    assertEquals(Instant.ofEpochMilli(UPDATED_AT).toString(), record.get(SchemataHeader.UpdatedAt));
    assertEquals(storageRoot, record.get(SchemataHeader.StorageRoot));
    assertEquals(storageLocation, record.get(SchemataHeader.StorageLocation));
    assertEquals(properties, parseJsonObject(record.get(SchemataHeader.Properties)));
  }

  @Test
  public void tablesTask_writesTablesAndViews() throws Exception {
    String storageLocation = "s3://lake/sales/orders";
    ImmutableMap<String, String> properties =
        ImmutableMap.of("delta.minReaderVersion", "3", "delta.enableRowTracking", "true");
    String filterFunction = "main.security.region_filter";
    stubTables(
        table("orders")
            .setTableType(TableType.MANAGED)
            .setSecurableKindManifest(
                new SecurableKindManifest()
                    .setSecurableType(SecurableType.TABLE)
                    .setSecurableKind(SecurableKind.TABLE_DELTA))
            .setDataSourceFormat(DataSourceFormat.DELTA)
            .setStorageLocation(storageLocation)
            .setComment("All orders")
            .setOwner("alice")
            .setCreatedAt(CREATED_AT)
            .setUpdatedAt(UPDATED_AT)
            .setProperties(properties)
            .setRowFilter(
                new TableRowFilter()
                    .setFunctionName(filterFunction)
                    .setInputColumnNames(ImmutableList.of("region"))
                    .setInputArguments(
                        ImmutableList.of(
                            new PolicyFunctionArgument().setColumn("region"),
                            new PolicyFunctionArgument().setConstant("'admin'")))),
        table("recent_orders").setTableType(TableType.VIEW).setViewDefinition("SELECT 1"));

    List<CSVRecord> records =
        runTask(
            (sink, handle) ->
                new DatabricksUnityCatalogTablesTask(new UnityCatalogSessionCache())
                    .doRun(null, sink, handle));

    assertEquals(2, records.size());
    CSVRecord table = records.get(0);
    assertEquals(CATALOG, table.get(TablesHeader.TableCatalog));
    assertEquals(SCHEMA, table.get(TablesHeader.TableSchema));
    assertEquals("orders", table.get(TablesHeader.TableName));
    assertEquals(TableType.MANAGED.name(), table.get(TablesHeader.TableType));
    assertEquals(SecurableType.TABLE.name(), table.get(TablesHeader.SecurableType));
    assertEquals(SecurableKind.TABLE_DELTA.name(), table.get(TablesHeader.SecurableKind));
    assertEquals(DataSourceFormat.DELTA.name(), table.get(TablesHeader.DataSourceFormat));
    assertEquals(storageLocation, table.get(TablesHeader.StorageLocation));
    assertEquals("All orders", table.get(TablesHeader.Comment));
    assertEquals("alice", table.get(TablesHeader.Owner));
    assertEquals(Instant.ofEpochMilli(CREATED_AT).toString(), table.get(TablesHeader.CreatedAt));
    assertEquals(Instant.ofEpochMilli(UPDATED_AT).toString(), table.get(TablesHeader.UpdatedAt));
    assertEquals(properties, parseJsonObject(table.get(TablesHeader.TableProperties)));
    for (TablesHeader hiveColumn :
        ImmutableList.of(
            TablesHeader.SerdeLibrary,
            TablesHeader.InputFormat,
            TablesHeader.OutputFormat,
            TablesHeader.StorageProperties,
            TablesHeader.NumBuckets,
            TablesHeader.BucketColumns,
            TablesHeader.SortColumns)) {
      assertEquals(hiveColumn.name(), "", table.get(hiveColumn));
    }
    assertEquals(
        ImmutableMap.of(
            "function",
            filterFunction,
            "inputColumns",
            ImmutableList.of("region"),
            "inputArguments",
            ImmutableList.of(
                ImmutableMap.of("column", "region"), ImmutableMap.of("constant", "'admin'"))),
        parseJsonObject(table.get(TablesHeader.RowFilter)));

    CSVRecord view = records.get(1);
    assertEquals("recent_orders", view.get(TablesHeader.TableName));
    assertEquals(TableType.VIEW.name(), view.get(TablesHeader.TableType));
    assertEquals("", view.get(TablesHeader.SecurableType));
    assertEquals("", view.get(TablesHeader.RowFilter));
  }

  @Test
  public void columnsTask_writesColumnsInPositionOrderWithTheirClauses() throws Exception {
    String maskFunction = "main.security.mask_status";
    ColumnInfo id =
        new ColumnInfo()
            .setName("id")
            .setPosition(0L)
            .setTypeText("bigint")
            .setNullable(false)
            .setComment("Order ID")
            .setTypeJson(
                typeJson(
                    "id",
                    "long",
                    ImmutableMap.of(
                        "delta.identity.start", 1,
                        "delta.identity.step", 2,
                        "delta.identity.allowExplicitInsert", false)));
    ColumnInfo orderDate =
        new ColumnInfo()
            .setName("order_date")
            .setPosition(1L)
            .setTypeText("date")
            .setPartitionIndex(0L)
            .setTypeJson(
                typeJson(
                    "order_date",
                    "date",
                    ImmutableMap.of("delta.generationExpression", "CAST(ordered_at AS DATE)")));
    ColumnInfo status =
        new ColumnInfo()
            .setName("status")
            .setPosition(2L)
            .setTypeText("string")
            .setTypeJson(typeJson("status", "string", ImmutableMap.of("CURRENT_DEFAULT", "'NEW'")))
            .setMask(
                new ColumnMask()
                    .setFunctionName(maskFunction)
                    .setUsingColumnNames(ImmutableList.of("region")));
    stubTables(
        table("orders")
            .setTableType(TableType.MANAGED)
            .setColumns(ImmutableList.of(status, id, orderDate)));

    List<CSVRecord> records =
        runTask(
            (sink, handle) ->
                new DatabricksUnityCatalogColumnsTask(new UnityCatalogSessionCache())
                    .doRun(null, sink, handle));

    assertEquals(
        ImmutableList.of("id", "order_date", "status"),
        columnValues(records, ColumnsHeader.ColumnName));
    assertEquals(
        ImmutableList.of("0", "1", "2"), columnValues(records, ColumnsHeader.OrdinalPosition));
    CSVRecord identity = records.get(0);
    assertEquals("orders", identity.get(ColumnsHeader.TableName));
    assertEquals("bigint", identity.get(ColumnsHeader.DataType));
    assertEquals("NO", identity.get(ColumnsHeader.IsNullable));
    assertEquals("Order ID", identity.get(ColumnsHeader.Comment));
    assertEquals("", identity.get(ColumnsHeader.PartitionIndex));
    assertEquals("ALWAYS", identity.get(ColumnsHeader.IdentityGeneration));
    assertEquals("1", identity.get(ColumnsHeader.IdentityStart));
    assertEquals("2", identity.get(ColumnsHeader.IdentityIncrement));
    assertEquals("", identity.get(ColumnsHeader.Mask));
    CSVRecord generated = records.get(1);
    assertEquals("YES", generated.get(ColumnsHeader.IsNullable));
    assertEquals("0", generated.get(ColumnsHeader.PartitionIndex));
    assertEquals("CAST(ordered_at AS DATE)", generated.get(ColumnsHeader.GenerationExpression));
    assertEquals("", generated.get(ColumnsHeader.IdentityGeneration));
    CSVRecord masked = records.get(2);
    assertEquals("'NEW'", masked.get(ColumnsHeader.ColumnDefault));
    assertEquals(
        ImmutableMap.of("function", maskFunction, "usingColumns", ImmutableList.of("region")),
        parseJsonObject(masked.get(ColumnsHeader.Mask)));
  }

  @Test
  public void viewsTask_writesViewsAndTablesWithADefiningQuery() throws Exception {
    String viewDefinition = "SELECT id FROM orders";
    String streamingDefinition = "SELECT * FROM STREAM read_files('s3://lake/events')";
    String sqlPath = "main.sales";
    stubTables(
        table("recent_orders")
            .setTableType(TableType.VIEW)
            .setViewDefinition(viewDefinition)
            .setSqlPath(sqlPath)
            .setProperties(
                ImmutableMap.<String, String>builder()
                    .put("view.catalogAndNamespace.numParts", "2")
                    .put("view.catalogAndNamespace.part.0", CATALOG)
                    .put("view.catalogAndNamespace.part.1", "sales-eu")
                    .put("view.query.out.numCols", "1")
                    .put("view.query.out.col.0", "id")
                    .put("view.schemaMode", "BINDING")
                    .build()),
        table("orders").setTableType(TableType.MANAGED),
        table("daily_totals").setTableType(TableType.MATERIALIZED_VIEW),
        table("events")
            .setTableType(TableType.STREAMING_TABLE)
            .setViewDefinition(streamingDefinition));

    List<CSVRecord> records =
        runTask(
            (sink, handle) ->
                new DatabricksUnityCatalogViewsTask(new UnityCatalogSessionCache())
                    .doRun(null, sink, handle));

    assertEquals(
        ImmutableList.of("recent_orders", "daily_totals", "events"),
        columnValues(records, ViewsHeader.TableName));
    CSVRecord view = records.get(0);
    assertEquals(CATALOG, view.get(ViewsHeader.TableCatalog));
    assertEquals(SCHEMA, view.get(ViewsHeader.TableSchema));
    assertEquals(viewDefinition, view.get(ViewsHeader.ViewDefinition));
    assertEquals("main.`sales-eu`", view.get(ViewsHeader.ViewCatalogAndNamespace));
    assertEquals(
        ImmutableList.of("id"), parseJsonArray(view.get(ViewsHeader.ViewQueryOutputColumns)));
    assertEquals("BINDING", view.get(ViewsHeader.ViewSchemaMode));
    assertEquals(sqlPath, view.get(ViewsHeader.SqlPath));
    assertEquals("", records.get(1).get(ViewsHeader.ViewDefinition));
    assertEquals(streamingDefinition, records.get(2).get(ViewsHeader.ViewDefinition));
  }

  @Test
  public void tableConstraintsTask_writesCheckConstraintsAndThenKeys() throws Exception {
    stubTables(
        table("orders")
            .setTableType(TableType.MANAGED)
            .setProperties(ImmutableMap.of("delta.constraints.positive_amount", "amount > 0")),
        table("customers").setTableType(TableType.MANAGED),
        table("line_items").setTableType(TableType.MANAGED),
        table("recent_orders").setTableType(TableType.VIEW));
    stubTablesWithKeys(CATALOG, "orders", "line_items");
    stubTable(
        table("orders")
            .setTableConstraints(
                ImmutableList.of(
                    new TableConstraint()
                        .setPrimaryKeyConstraint(
                            new PrimaryKeyConstraint()
                                .setName("orders_pk")
                                .setChildColumns(ImmutableList.of("id"))
                                .setRely(true)))));
    stubTable(
        table("line_items")
            .setTableConstraints(
                ImmutableList.of(
                    new TableConstraint()
                        .setForeignKeyConstraint(
                            new ForeignKeyConstraint()
                                .setName("line_items_orders_fk")
                                .setChildColumns(ImmutableList.of("order_id"))
                                .setParentTable("main.sales.orders")
                                .setParentColumns(ImmutableList.of("id"))))));

    List<CSVRecord> records =
        runTask(
            (sink, handle) ->
                new DatabricksUnityCatalogTableConstraintsTask(new UnityCatalogSessionCache())
                    .doRun(null, sink, handle));

    assertEquals(
        ImmutableList.of("positive_amount", "orders_pk", "line_items_orders_fk"),
        columnValues(records, TableConstraintsHeader.ConstraintName));
    CSVRecord check = records.get(0);
    assertEquals("orders", check.get(TableConstraintsHeader.TableName));
    assertEquals("CHECK", check.get(TableConstraintsHeader.ConstraintType));
    assertEquals(
        ImmutableMap.of("expression", "amount > 0"),
        parseJsonObject(check.get(TableConstraintsHeader.ConstraintDetails)));
    CSVRecord primaryKey = records.get(1);
    assertEquals("orders", primaryKey.get(TableConstraintsHeader.TableName));
    assertEquals("PRIMARY KEY", primaryKey.get(TableConstraintsHeader.ConstraintType));
    assertEquals(
        ImmutableMap.of("columns", ImmutableList.of("id"), "rely", true),
        parseJsonObject(primaryKey.get(TableConstraintsHeader.ConstraintDetails)));
    CSVRecord foreignKey = records.get(2);
    assertEquals("line_items", foreignKey.get(TableConstraintsHeader.TableName));
    assertEquals("FOREIGN KEY", foreignKey.get(TableConstraintsHeader.ConstraintType));
    assertEquals(
        ImmutableMap.of(
            "columns",
            ImmutableList.of("order_id"),
            "referencedTable",
            "main.sales.orders",
            "referencedColumns",
            ImmutableList.of("id")),
        parseJsonObject(foreignKey.get(TableConstraintsHeader.ConstraintDetails)));
    server.verify(0, getRequestedFor(urlPathEqualTo(tablePath(table("customers")))));
    server.verify(0, getRequestedFor(urlPathEqualTo(tablePath(table("recent_orders")))));
  }

  @Test
  public void tableConstraintsTask_keepsTheCheckConstraintsOfATableThatFailsToLoad()
      throws Exception {
    stubTables(
        table("orders")
            .setTableType(TableType.MANAGED)
            .setProperties(ImmutableMap.of("delta.constraints.positive_amount", "amount > 0")),
        table("customers").setTableType(TableType.MANAGED));
    stubTablesWithKeys(CATALOG, "orders", "customers");
    server.stubFor(
        get(urlPathEqualTo(tablePath(table("orders"))))
            .willReturn(error(404, "TABLE_DOES_NOT_EXIST", "Table 'orders' was dropped.")));
    stubTable(table("customers"));

    List<CSVRecord> records =
        runTask(
            (sink, handle) ->
                new DatabricksUnityCatalogTableConstraintsTask(new UnityCatalogSessionCache())
                    .doRun(null, sink, handle));

    CSVRecord check = getOnlyElement(records);
    assertEquals("orders", check.get(TableConstraintsHeader.TableName));
    assertEquals("CHECK", check.get(TableConstraintsHeader.ConstraintType));
  }

  @Test
  public void tableConstraintsTask_requestsEachTableOfACatalogWhoseInformationSchemaFails()
      throws Exception {
    String crmCatalog = "crm";
    stubCatalogs(catalog(CATALOG), catalog(crmCatalog));
    stubSchemas(CATALOG, schema(SCHEMA));
    stubSchemas(crmCatalog, schema(SCHEMA));
    stubTableListing(
        CATALOG,
        table("orders").setTableType(TableType.MANAGED),
        table("customers").setTableType(TableType.MANAGED));
    stubTableListing(
        crmCatalog,
        table(crmCatalog, "accounts").setTableType(TableType.MANAGED),
        table(crmCatalog, "contacts").setTableType(TableType.MANAGED));
    stubTablesWithKeys(CATALOG, "orders");
    stubTablesWithKeysQuery(
        crmCatalog,
        okModel(failed("[TABLE_OR_VIEW_NOT_FOUND] The table or view cannot be found.")));
    stubTable(table("orders").setTableConstraints(primaryKey("orders_pk", "id")));
    stubTable(table(crmCatalog, "accounts").setTableConstraints(primaryKey("accounts_pk", "id")));
    stubTable(table(crmCatalog, "contacts"));

    List<CSVRecord> records =
        runTask(
            (sink, handle) ->
                new DatabricksUnityCatalogTableConstraintsTask(new UnityCatalogSessionCache())
                    .doRun(null, sink, handle));

    assertEquals(
        ImmutableList.of("orders_pk", "accounts_pk"),
        columnValues(records, TableConstraintsHeader.ConstraintName));
    assertEquals(
        ImmutableList.of(CATALOG, crmCatalog),
        columnValues(records, TableConstraintsHeader.TableCatalog));
    server.verify(0, getRequestedFor(urlPathEqualTo(tablePath(table("customers")))));
    server.verify(1, getRequestedFor(urlPathEqualTo(tablePath(table(crmCatalog, "contacts")))));
  }

  @Test
  public void tableConstraintsTask_matchesTheLowercaseNamesOfInformationSchema() throws Exception {
    stubTables(
        table("Orders").setTableType(TableType.MANAGED),
        table("Customers").setTableType(TableType.MANAGED));
    stubTablesWithKeys(CATALOG, "orders");
    stubTable(table("Orders").setTableConstraints(primaryKey("orders_pk", "id")));

    List<CSVRecord> records =
        runTask(
            (sink, handle) ->
                new DatabricksUnityCatalogTableConstraintsTask(new UnityCatalogSessionCache())
                    .doRun(null, sink, handle));

    assertEquals("orders_pk", getOnlyElement(records).get(TableConstraintsHeader.ConstraintName));
    server.verify(0, getRequestedFor(urlPathEqualTo(tablePath(table("Customers")))));
  }

  @Test
  public void tableConstraintsTask_queriesNoInformationSchemaForACatalogOfViews() throws Exception {
    stubTables(
        table("recent_orders").setTableType(TableType.VIEW),
        table("revenue").setTableType(TableType.METRIC_VIEW));

    List<CSVRecord> records =
        runTask(
            (sink, handle) ->
                new DatabricksUnityCatalogTableConstraintsTask(new UnityCatalogSessionCache())
                    .doRun(null, sink, handle));

    assertEquals(0, records.size());
    server.verify(0, postRequestedFor(urlPathEqualTo(STATEMENTS_PATH)));
    server.verify(0, getRequestedFor(urlPathEqualTo(tablePath(table("recent_orders")))));
    server.verify(0, getRequestedFor(urlPathEqualTo(tablePath(table("revenue")))));
  }

  @Test
  public void functionsTask_writesSqlAndPythonFunctions() throws Exception {
    String sqlPath = "main.sales";
    ImmutableMap<String, String> properties =
        ImmutableMap.of("sqlConfig.spark.sql.ansi.enabled", "true", "owner_team", "finance");
    String pythonBody = "class SplitWords:\n  def eval(self, text):\n    yield from text.split()";
    stubFunctions(
        function("add_tax")
            .setFullDataType("DECIMAL(10,2)")
            .setInputParams(
                new FunctionParameterInfos()
                    .setParameters(
                        ImmutableList.of(
                            new FunctionParameterInfo()
                                .setName("rate")
                                .setTypeText("DECIMAL(4,2)")
                                .setPosition(1L)
                                .setParameterMode(FunctionParameterMode.IN)
                                .setParameterDefault("0.08")
                                .setComment("Tax rate"),
                            new FunctionParameterInfo()
                                .setName("amount")
                                .setTypeText("DECIMAL(10,2)")
                                .setPosition(0L)
                                .setParameterMode(FunctionParameterMode.IN))))
            .setRoutineBody(FunctionInfoRoutineBody.SQL)
            .setRoutineDefinition("amount * (1 + rate)")
            .setComment("Adds tax")
            .setOwner("alice")
            .setIsDeterministic(true)
            .setSqlDataAccess(FunctionInfoSqlDataAccess.CONTAINS_SQL)
            .setSecurityType(FunctionInfoSecurityType.DEFINER)
            .setSqlPath(sqlPath)
            .setCreatedAt(CREATED_AT)
            .setUpdatedAt(UPDATED_AT)
            .setProperties(JSON.writeValueAsString(properties)),
        function("split_words")
            .setInputParams(
                new FunctionParameterInfos()
                    .setParameters(
                        ImmutableList.of(
                            new FunctionParameterInfo()
                                .setName("text")
                                .setTypeText("STRING")
                                .setPosition(0L))))
            .setReturnParams(
                new FunctionParameterInfos()
                    .setParameters(
                        ImmutableList.of(
                            new FunctionParameterInfo()
                                .setName("word")
                                .setTypeText("STRING")
                                .setPosition(0L))))
            .setRoutineBody(FunctionInfoRoutineBody.EXTERNAL)
            .setExternalLanguage("Python")
            .setExternalName("SplitWords")
            .setRoutineDefinition(pythonBody)
            .setIsDeterministic(false)
            .setSqlDataAccess(FunctionInfoSqlDataAccess.NO_SQL));

    List<CSVRecord> records =
        runTask(
            (sink, handle) ->
                new DatabricksUnityCatalogFunctionsTask(new UnityCatalogSessionCache())
                    .doRun(null, sink, handle));

    assertEquals(2, records.size());
    CSVRecord sql = records.get(0);
    assertEquals(CATALOG, sql.get(FunctionsHeader.FunctionCatalog));
    assertEquals(SCHEMA, sql.get(FunctionsHeader.FunctionSchema));
    assertEquals("add_tax", sql.get(FunctionsHeader.FunctionName));
    assertEquals("DECIMAL(10,2)", sql.get(FunctionsHeader.DataType));
    assertEquals(
        ImmutableList.of(
            ImmutableMap.of("name", "amount", "type", "DECIMAL(10,2)", "mode", "IN"),
            ImmutableMap.of(
                "name", "rate",
                "type", "DECIMAL(4,2)",
                "mode", "IN",
                "default", "0.08",
                "comment", "Tax rate")),
        parseJsonArray(sql.get(FunctionsHeader.InputParams)));
    assertEquals("", sql.get(FunctionsHeader.ReturnParams));
    assertEquals("amount * (1 + rate)", sql.get(FunctionsHeader.RoutineDefinition));
    assertEquals("SQL", sql.get(FunctionsHeader.RoutineLanguage));
    assertEquals("Adds tax", sql.get(FunctionsHeader.Comment));
    assertEquals("alice", sql.get(FunctionsHeader.Owner));
    assertEquals("true", sql.get(FunctionsHeader.IsDeterministic));
    assertEquals("CONTAINS SQL", sql.get(FunctionsHeader.SqlDataAccess));
    assertEquals(FunctionInfoSecurityType.DEFINER.name(), sql.get(FunctionsHeader.SecurityType));
    assertEquals(sqlPath, sql.get(FunctionsHeader.SqlPath));
    assertEquals(Instant.ofEpochMilli(CREATED_AT).toString(), sql.get(FunctionsHeader.CreatedAt));
    assertEquals(Instant.ofEpochMilli(UPDATED_AT).toString(), sql.get(FunctionsHeader.UpdatedAt));
    assertEquals("", sql.get(FunctionsHeader.ClassName));
    assertEquals("", sql.get(FunctionsHeader.Handler));
    Map<String, Object> writtenProperties = parseJsonObject(sql.get(FunctionsHeader.Properties));
    assertEquals(properties, writtenProperties);
    assertEquals(
        Ordering.natural().sortedCopy(properties.keySet()),
        ImmutableList.copyOf(writtenProperties.keySet()));

    CSVRecord python = records.get(1);
    assertEquals("split_words", python.get(FunctionsHeader.FunctionName));
    assertEquals("TABLE(`word` STRING)", python.get(FunctionsHeader.DataType));
    assertEquals(
        ImmutableList.of(ImmutableMap.of("name", "text", "type", "STRING")),
        parseJsonArray(python.get(FunctionsHeader.InputParams)));
    assertEquals(
        ImmutableList.of(ImmutableMap.of("name", "word", "type", "STRING")),
        parseJsonArray(python.get(FunctionsHeader.ReturnParams)));
    assertEquals(pythonBody, python.get(FunctionsHeader.RoutineDefinition));
    assertEquals("PYTHON", python.get(FunctionsHeader.RoutineLanguage));
    assertEquals("false", python.get(FunctionsHeader.IsDeterministic));
    assertEquals("NO SQL", python.get(FunctionsHeader.SqlDataAccess));
    assertEquals("SplitWords", python.get(FunctionsHeader.Handler));
    assertEquals("", python.get(FunctionsHeader.Properties));
  }

  @Test
  public void tasks_shareOneListingOfTheTables() throws Exception {
    stubTables(
        table("orders")
            .setTableType(TableType.MANAGED)
            .setColumns(
                ImmutableList.of(
                    new ColumnInfo().setName("id").setPosition(0L).setTypeText("bigint"))));
    stubTablesWithKeys(CATALOG);
    UnityCatalogSessionCache cache = new UnityCatalogSessionCache();

    try (DatabricksHandle handle = newRestHandle()) {
      assertEquals(
          1,
          runTask(
                  handle,
                  (sink, taskHandle) ->
                      new DatabricksUnityCatalogTablesTask(cache).doRun(null, sink, taskHandle))
              .size());
      assertEquals(
          1,
          runTask(
                  handle,
                  (sink, taskHandle) ->
                      new DatabricksUnityCatalogColumnsTask(cache).doRun(null, sink, taskHandle))
              .size());
      assertEquals(
          0,
          runTask(
                  handle,
                  (sink, taskHandle) ->
                      new DatabricksUnityCatalogViewsTask(cache).doRun(null, sink, taskHandle))
              .size());
      assertEquals(
          0,
          runTask(
                  handle,
                  (sink, taskHandle) ->
                      new DatabricksUnityCatalogTableConstraintsTask(cache)
                          .doRun(null, sink, taskHandle))
              .size());
    }

    server.verify(1, getRequestedFor(urlPathEqualTo(CATALOGS_PATH)));
    server.verify(1, getRequestedFor(urlPathEqualTo(SCHEMAS_PATH)));
    server.verify(1, getRequestedFor(urlPathEqualTo(TABLES_PATH)));
  }

  @Test
  public void tasks_doNotRepeatAFailedListingOfTheTables() throws Exception {
    stubCatalogs(catalog(CATALOG));
    stubSchemas(CATALOG, schema(SCHEMA));
    server.stubFor(
        get(urlPathEqualTo(TABLES_PATH))
            .willReturn(error(403, "PERMISSION_DENIED", "User does not have USE SCHEMA.")));
    UnityCatalogSessionCache cache = new UnityCatalogSessionCache();

    try (DatabricksHandle handle = newRestHandle()) {
      assertThrows(
          IOException.class,
          () ->
              runTask(
                  handle,
                  (sink, taskHandle) ->
                      new DatabricksUnityCatalogTablesTask(cache).doRun(null, sink, taskHandle)));
      IOException exception =
          assertThrows(
              IOException.class,
              () ->
                  runTask(
                      handle,
                      (sink, taskHandle) ->
                          new DatabricksUnityCatalogColumnsTask(cache)
                              .doRun(null, sink, taskHandle)));
      assertTrue(exception.getMessage(), exception.getMessage().contains("failed earlier"));
    }

    server.verify(1, getRequestedFor(urlPathEqualTo(TABLES_PATH)));
  }

  private List<CSVRecord> runTask(TaskBody body) throws Exception {
    try (DatabricksHandle handle = newRestHandle()) {
      return runTask(handle, body);
    }
  }

  private List<CSVRecord> runTask(DatabricksHandle handle, TaskBody body) throws Exception {
    File output = tempFolder.newFile();
    body.run(Files.asByteSink(output), handle);
    return readRecords(output);
  }

  private static DatabricksHandle newRestHandle() {
    WorkspaceClient client = newWorkspaceClient(server.baseUrl());
    DatabricksRestHelper restHelper =
        new DatabricksRestHelper(client, UNTHROTTLED_REQUESTS_PER_SECOND, millis -> {});
    return newHandle(client, restHelper, acceptAllFilter());
  }

  private static CatalogInfo catalog(String name) {
    return new CatalogInfo().setName(name);
  }

  private static SchemaInfo schema(String name) {
    return new SchemaInfo().setCatalogName(CATALOG).setName(name);
  }

  private static TableInfo table(String name) {
    return table(CATALOG, name);
  }

  private static TableInfo table(String catalogName, String name) {
    return new TableInfo().setCatalogName(catalogName).setSchemaName(SCHEMA).setName(name);
  }

  private static FunctionInfo function(String name) {
    return new FunctionInfo().setCatalogName(CATALOG).setSchemaName(SCHEMA).setName(name);
  }

  /** Returns the constraints of a table that has only a primary key on one column. */
  private static ImmutableList<TableConstraint> primaryKey(String name, String column) {
    return ImmutableList.of(
        new TableConstraint()
            .setPrimaryKeyConstraint(
                new PrimaryKeyConstraint()
                    .setName(name)
                    .setChildColumns(ImmutableList.of(column))));
  }

  private static void stubCatalogs(CatalogInfo... catalogs) {
    server.stubFor(
        get(urlPathEqualTo(CATALOGS_PATH))
            .willReturn(
                okModel(new ListCatalogsResponse().setCatalogs(ImmutableList.copyOf(catalogs)))));
  }

  private static void stubSchemas(String catalogName, SchemaInfo... schemas) {
    server.stubFor(
        get(urlPathEqualTo(SCHEMAS_PATH))
            .withQueryParam("catalog_name", equalTo(catalogName))
            .willReturn(
                okModel(new ListSchemasResponse().setSchemas(ImmutableList.copyOf(schemas)))));
  }

  /** Stubs one catalog with one schema, and the listing of the tables of the schema. */
  private static void stubTables(TableInfo... tables) {
    stubCatalogs(catalog(CATALOG));
    stubSchemas(CATALOG, schema(SCHEMA));
    stubTableListing(CATALOG, tables);
  }

  /** Stubs the listing of the tables of {@link #SCHEMA} in a catalog. */
  private static void stubTableListing(String catalogName, TableInfo... tables) {
    server.stubFor(
        get(urlPathEqualTo(TABLES_PATH))
            .withQueryParam("catalog_name", equalTo(catalogName))
            .withQueryParam("schema_name", equalTo(SCHEMA))
            .willReturn(okModel(new ListTablesResponse().setTables(ImmutableList.copyOf(tables)))));
  }

  /** Stubs the request for one table, which returns its primary and foreign keys. */
  private static void stubTable(TableInfo table) {
    server.stubFor(get(urlPathEqualTo(tablePath(table))).willReturn(okModel(table)));
  }

  /**
   * Stubs the information_schema query of a catalog to return the tables of {@link #SCHEMA} that
   * have primary or foreign keys.
   */
  private static void stubTablesWithKeys(String catalogName, String... tableNames) {
    ImmutableList.Builder<List<String>> rows = ImmutableList.builder();
    for (String tableName : tableNames) {
      rows.add(ImmutableList.of(SCHEMA, tableName));
    }
    stubTablesWithKeysQuery(catalogName, okModel(succeededRows(rows.build())));
  }

  /**
   * Stubs the information_schema query of a catalog for its tables with primary or foreign keys.
   */
  private static void stubTablesWithKeysQuery(
      String catalogName, ResponseDefinitionBuilder response) {
    server.stubFor(
        post(urlPathEqualTo(STATEMENTS_PATH))
            .withRequestBody(matchingJsonPath("$.catalog", equalTo(catalogName)))
            .withRequestBody(
                matchingJsonPath(
                    "$.statement",
                    containing(
                        quoteIdentifier(catalogName) + ".information_schema.table_constraints")))
            .willReturn(response));
  }

  /** Stubs one catalog with one schema, and the listing of the functions of the schema. */
  private static void stubFunctions(FunctionInfo... functions) {
    stubCatalogs(catalog(CATALOG));
    stubSchemas(CATALOG, schema(SCHEMA));
    server.stubFor(
        get(urlPathEqualTo(FUNCTIONS_PATH))
            .withQueryParam("catalog_name", equalTo(CATALOG))
            .withQueryParam("schema_name", equalTo(SCHEMA))
            .willReturn(
                okModel(
                    new ListFunctionsResponse().setFunctions(ImmutableList.copyOf(functions)))));
  }

  private static String tablePath(TableInfo table) {
    return TABLES_PATH
        + "/"
        + table.getCatalogName()
        + "."
        + table.getSchemaName()
        + "."
        + table.getName();
  }

  /** Returns the {@code type_json} of a column, which has the clauses of the column. */
  private static String typeJson(String name, String type, Map<String, ?> metadata)
      throws IOException {
    return JSON.writeValueAsString(
        ImmutableMap.of("name", name, "type", type, "nullable", true, "metadata", metadata));
  }

  private static ImmutableList<String> columnValues(List<CSVRecord> records, Enum<?> column) {
    ImmutableList.Builder<String> values = ImmutableList.builder();
    for (CSVRecord record : records) {
      values.add(record.get(column));
    }
    return values.build();
  }

  private static Map<String, Object> parseJsonObject(String json) throws IOException {
    return JSON.readValue(json, new TypeReference<Map<String, Object>>() {});
  }

  private static List<Object> parseJsonArray(String json) throws IOException {
    return JSON.readValue(json, new TypeReference<List<Object>>() {});
  }
}
