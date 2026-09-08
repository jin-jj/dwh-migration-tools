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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksError;
import com.databricks.sdk.service.catalog.CatalogInfo;
import com.databricks.sdk.service.catalog.CatalogsAPI;
import com.databricks.sdk.service.catalog.ColumnInfo;
import com.databricks.sdk.service.catalog.DataSourceFormat;
import com.databricks.sdk.service.catalog.ForeignKeyConstraint;
import com.databricks.sdk.service.catalog.FunctionInfo;
import com.databricks.sdk.service.catalog.FunctionsAPI;
import com.databricks.sdk.service.catalog.ListCatalogsRequest;
import com.databricks.sdk.service.catalog.PrimaryKeyConstraint;
import com.databricks.sdk.service.catalog.SchemaInfo;
import com.databricks.sdk.service.catalog.SchemasAPI;
import com.databricks.sdk.service.catalog.TableConstraint;
import com.databricks.sdk.service.catalog.TableInfo;
import com.databricks.sdk.service.catalog.TableType;
import com.databricks.sdk.service.catalog.TablesAPI;
import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.StatementExecutionAPI;
import com.databricks.sdk.service.sql.StatementResponse;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import com.google.edwmigration.dumper.application.dumper.task.MemoryByteSink;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksTasksTest {

  private WorkspaceClient workspaceClient;
  private CatalogsAPI catalogsAPI;
  private SchemasAPI schemasAPI;
  private TablesAPI tablesAPI;
  private FunctionsAPI functionsAPI;
  private StatementExecutionAPI statementExecutionAPI;
  private DatabricksHandle handle;
  private TaskRunContext context;

  @Before
  public void setUp() {
    workspaceClient = mock(WorkspaceClient.class);
    catalogsAPI = mock(CatalogsAPI.class);
    schemasAPI = mock(SchemasAPI.class);
    tablesAPI = mock(TablesAPI.class);
    functionsAPI = mock(FunctionsAPI.class);
    statementExecutionAPI = mock(StatementExecutionAPI.class);

    when(workspaceClient.catalogs()).thenReturn(catalogsAPI);
    when(workspaceClient.schemas()).thenReturn(schemasAPI);
    when(workspaceClient.tables()).thenReturn(tablesAPI);
    when(workspaceClient.functions()).thenReturn(functionsAPI);
    when(workspaceClient.statementExecution()).thenReturn(statementExecutionAPI);

    handle = new DatabricksHandle(workspaceClient, "test-warehouse-id");
    context = mock(TaskRunContext.class);
  }

  private static List<String> readLines(MemoryByteSink sink) {
    String content = sink.openStream().toString();
    if (content.isEmpty()) {
      return Collections.emptyList();
    }
    return Arrays.asList(content.split("\\r?\\n"));
  }

  @Test
  public void catalogsTask_writesExpectedCsv() throws Exception {
    CatalogInfo cat1 =
        new CatalogInfo()
            .setName("my_catalog")
            .setComment("test catalog")
            .setOwner("alice")
            .setCreatedAt(1000L)
            .setUpdatedAt(2000L);
    when(catalogsAPI.list(any(ListCatalogsRequest.class)))
        .thenReturn(Collections.singletonList(cat1));

    DatabricksCatalogsTask task = new DatabricksCatalogsTask(c -> true);
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(2, lines.size());
    assertEquals("CatalogName,Comment,Owner,CreatedAt,UpdatedAt", lines.get(0));
    assertEquals("my_catalog,test catalog,alice,1000,2000", lines.get(1));
  }

  @Test
  public void schemasTask_writesExpectedCsv() throws Exception {
    CatalogInfo cat = new CatalogInfo().setName("my_catalog");
    when(catalogsAPI.list(any(ListCatalogsRequest.class)))
        .thenReturn(Collections.singletonList(cat));

    SchemaInfo schema =
        new SchemaInfo()
            .setCatalogName("my_catalog")
            .setName("my_schema")
            .setComment("schema comment")
            .setOwner("bob")
            .setCreatedAt(1100L)
            .setUpdatedAt(2100L);
    when(schemasAPI.list("my_catalog")).thenReturn(Collections.singletonList(schema));

    DatabricksSchemasTask task = new DatabricksSchemasTask(c -> true, s -> true);
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(2, lines.size());
    assertEquals("CatalogName,SchemaName,Comment,Owner,CreatedAt,UpdatedAt", lines.get(0));
    assertEquals("my_catalog,my_schema,schema comment,bob,1100,2100", lines.get(1));
  }

  @Test
  public void tablesTask_writesExpectedCsv() throws Exception {
    CatalogInfo cat = new CatalogInfo().setName("my_catalog");
    when(catalogsAPI.list(any(ListCatalogsRequest.class)))
        .thenReturn(Collections.singletonList(cat));
    SchemaInfo schema = new SchemaInfo().setCatalogName("my_catalog").setName("my_schema");
    when(schemasAPI.list("my_catalog")).thenReturn(Collections.singletonList(schema));

    TableInfo table =
        new TableInfo()
            .setCatalogName("my_catalog")
            .setSchemaName("my_schema")
            .setName("orders")
            .setTableType(TableType.MANAGED)
            .setDataSourceFormat(DataSourceFormat.DELTA)
            .setStorageLocation("s3://warehouse/orders")
            .setComment("orders table")
            .setOwner("charlie")
            .setCreatedAt(1200L)
            .setUpdatedAt(2200L);
    when(tablesAPI.list("my_catalog", "my_schema")).thenReturn(Collections.singletonList(table));

    DatabricksTablesTask task = new DatabricksTablesTask(c -> true, s -> true);
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(2, lines.size());
    assertEquals(
        "TableCatalog,TableSchema,TableName,TableType,DataSourceFormat,StorageLocation,Comment,Owner,CreatedAt,UpdatedAt",
        lines.get(0));
    assertEquals(
        "my_catalog,my_schema,orders,MANAGED,DELTA,s3://warehouse/orders,orders table,charlie,1200,2200",
        lines.get(1));
  }

  @Test
  public void columnsTask_writesExpectedCsv() throws Exception {
    CatalogInfo cat = new CatalogInfo().setName("my_catalog");
    when(catalogsAPI.list(any(ListCatalogsRequest.class)))
        .thenReturn(Collections.singletonList(cat));
    SchemaInfo schema = new SchemaInfo().setCatalogName("my_catalog").setName("my_schema");
    when(schemasAPI.list("my_catalog")).thenReturn(Collections.singletonList(schema));

    ColumnInfo col1 =
        new ColumnInfo()
            .setPosition(1L)
            .setName("order_id")
            .setTypeText("bigint")
            .setNullable(false)
            .setComment("primary id");
    ColumnInfo col2 =
        new ColumnInfo()
            .setPosition(2L)
            .setName("details")
            .setTypeText("struct<item:string,qty:int>")
            .setNullable(true);
    TableInfo table =
        new TableInfo()
            .setCatalogName("my_catalog")
            .setSchemaName("my_schema")
            .setName("orders")
            .setColumns(Arrays.asList(col1, col2));
    when(tablesAPI.list("my_catalog", "my_schema")).thenReturn(Collections.singletonList(table));

    DatabricksColumnsTask task = new DatabricksColumnsTask(c -> true, s -> true);
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(3, lines.size());
    assertEquals(
        "TableCatalog,TableSchema,TableName,OrdinalPosition,ColumnName,DataType,IsNullable,Comment,PartitionIndex",
        lines.get(0));
    assertEquals("my_catalog,my_schema,orders,1,order_id,bigint,false,primary id,", lines.get(1));
    assertEquals(
        "my_catalog,my_schema,orders,2,details,\"struct<item:string,qty:int>\",true,,",
        lines.get(2));
  }

  @Test
  public void viewsTask_writesExpectedCsv() throws Exception {
    CatalogInfo cat = new CatalogInfo().setName("my_catalog");
    when(catalogsAPI.list(any(ListCatalogsRequest.class)))
        .thenReturn(Collections.singletonList(cat));
    SchemaInfo schema = new SchemaInfo().setCatalogName("my_catalog").setName("my_schema");
    when(schemasAPI.list("my_catalog")).thenReturn(Collections.singletonList(schema));

    TableInfo view =
        new TableInfo()
            .setCatalogName("my_catalog")
            .setSchemaName("my_schema")
            .setName("active_orders")
            .setViewDefinition("SELECT * FROM orders WHERE status = 'ACTIVE'");
    when(tablesAPI.list("my_catalog", "my_schema")).thenReturn(Collections.singletonList(view));

    DatabricksViewsTask task = new DatabricksViewsTask(c -> true, s -> true);
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(2, lines.size());
    assertEquals("TableCatalog,TableSchema,TableName,ViewDefinition", lines.get(0));
    assertEquals(
        "my_catalog,my_schema,active_orders,SELECT * FROM orders WHERE status = 'ACTIVE'",
        lines.get(1));
  }

  @Test
  public void tableConstraintsTask_writesExpectedCsv() throws Exception {
    CatalogInfo cat = new CatalogInfo().setName("my_catalog");
    when(catalogsAPI.list(any(ListCatalogsRequest.class)))
        .thenReturn(Collections.singletonList(cat));
    SchemaInfo schema = new SchemaInfo().setCatalogName("my_catalog").setName("my_schema");
    when(schemasAPI.list("my_catalog")).thenReturn(Collections.singletonList(schema));

    PrimaryKeyConstraint pk =
        new PrimaryKeyConstraint()
            .setName("pk_orders")
            .setChildColumns(Collections.singletonList("order_id"));
    ForeignKeyConstraint fk =
        new ForeignKeyConstraint()
            .setName("fk_customer")
            .setChildColumns(Collections.singletonList("customer_id"))
            .setParentTable("customers")
            .setParentColumns(Collections.singletonList("id"));
    TableConstraint tc1 = new TableConstraint().setPrimaryKeyConstraint(pk);
    TableConstraint tc2 = new TableConstraint().setForeignKeyConstraint(fk);

    TableInfo tableSummary =
        new TableInfo().setCatalogName("my_catalog").setSchemaName("my_schema").setName("orders");
    TableInfo tableDetail =
        new TableInfo()
            .setCatalogName("my_catalog")
            .setSchemaName("my_schema")
            .setName("orders")
            .setTableConstraints(Arrays.asList(tc1, tc2));
    when(tablesAPI.list("my_catalog", "my_schema"))
        .thenReturn(Collections.singletonList(tableSummary));
    when(tablesAPI.get("my_catalog.my_schema.orders")).thenReturn(tableDetail);

    DatabricksTableConstraintsTask task = new DatabricksTableConstraintsTask(c -> true, s -> true);
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(3, lines.size());
    assertEquals(
        "TableCatalog,TableSchema,TableName,ConstraintName,ConstraintType,ConstraintDetails",
        lines.get(0));
    assertEquals("my_catalog,my_schema,orders,pk_orders,PRIMARY KEY,order_id", lines.get(1));
    assertEquals(
        "my_catalog,my_schema,orders,fk_customer,FOREIGN KEY,customer_id -> customers(id)",
        lines.get(2));
  }

  @Test
  public void functionsTask_writesExpectedCsv() throws Exception {
    CatalogInfo cat = new CatalogInfo().setName("my_catalog");
    when(catalogsAPI.list(any(ListCatalogsRequest.class)))
        .thenReturn(Collections.singletonList(cat));
    SchemaInfo schema = new SchemaInfo().setCatalogName("my_catalog").setName("my_schema");
    when(schemasAPI.list("my_catalog")).thenReturn(Collections.singletonList(schema));

    FunctionInfo fn =
        new FunctionInfo()
            .setCatalogName("my_catalog")
            .setSchemaName("my_schema")
            .setName("add_one")
            .setFullDataType("int")
            .setRoutineDefinition("RETURN x + 1")
            .setExternalLanguage("SQL")
            .setComment("adds one")
            .setOwner("david");
    when(functionsAPI.list("my_catalog", "my_schema")).thenReturn(Collections.singletonList(fn));

    DatabricksFunctionsTask task = new DatabricksFunctionsTask(c -> true, s -> true);
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(2, lines.size());
    assertEquals(
        "FunctionCatalog,FunctionSchema,FunctionName,DataType,InputParams,RoutineDefinition,RoutineLanguage,Comment,Owner",
        lines.get(0));
    assertEquals("my_catalog,my_schema,add_one,int,,RETURN x + 1,SQL,adds one,david", lines.get(1));
  }

  @Test
  public void catalogsTask_filtersSamplesAndSystemCatalogs() throws Exception {
    CatalogInfo cat1 = new CatalogInfo().setName("my_catalog");
    CatalogInfo cat2 = new CatalogInfo().setName("samples");
    CatalogInfo cat3 = new CatalogInfo().setName("system");
    when(catalogsAPI.list(any(ListCatalogsRequest.class)))
        .thenReturn(Arrays.asList(cat1, cat2, cat3));

    DatabricksCatalogsTask task = new DatabricksCatalogsTask(c -> true);
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(2, lines.size());
    assertEquals("CatalogName,Comment,Owner,CreatedAt,UpdatedAt", lines.get(0));
    assertEquals("my_catalog,,,,", lines.get(1));
  }

  @Test
  public void tableConstraintsTask_skipsViews() throws Exception {
    CatalogInfo cat = new CatalogInfo().setName("my_catalog");
    when(catalogsAPI.list(any(ListCatalogsRequest.class)))
        .thenReturn(Collections.singletonList(cat));
    SchemaInfo schema = new SchemaInfo().setCatalogName("my_catalog").setName("my_schema");
    when(schemasAPI.list("my_catalog")).thenReturn(Collections.singletonList(schema));

    TableInfo viewSummary =
        new TableInfo()
            .setCatalogName("my_catalog")
            .setSchemaName("my_schema")
            .setName("v_orders")
            .setTableType(TableType.VIEW);
    when(tablesAPI.list("my_catalog", "my_schema"))
        .thenReturn(Collections.singletonList(viewSummary));

    DatabricksTableConstraintsTask task = new DatabricksTableConstraintsTask(c -> true, s -> true);
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    verify(tablesAPI, never()).get(anyString());
    List<String> lines = readLines(sink);
    assertEquals(1, lines.size());
    assertEquals(
        "TableCatalog,TableSchema,TableName,ConstraintName,ConstraintType,ConstraintDetails",
        lines.get(0));
  }

  @Test
  public void functionsTask_retriesOnRateLimit() throws Exception {
    System.setProperty(AbstractDatabricksTask.RETRY_BACKOFF_PROPERTY, "10");
    try {
      CatalogInfo cat = new CatalogInfo().setName("my_catalog");
      when(catalogsAPI.list(any(ListCatalogsRequest.class)))
          .thenReturn(Collections.singletonList(cat));
      SchemaInfo schema = new SchemaInfo().setCatalogName("my_catalog").setName("my_schema");
      when(schemasAPI.list("my_catalog")).thenReturn(Collections.singletonList(schema));

      FunctionInfo fn =
          new FunctionInfo()
              .setCatalogName("my_catalog")
              .setSchemaName("my_schema")
              .setName("add_one")
              .setFullDataType("int")
              .setRoutineDefinition("RETURN x + 1")
              .setExternalLanguage("SQL")
              .setComment("adds one")
              .setOwner("david");
      when(functionsAPI.list("my_catalog", "my_schema"))
          .thenThrow(
              new DatabricksError("TOO_MANY_REQUESTS", "Current request has to be retried", 429))
          .thenReturn(Collections.singletonList(fn));

      DatabricksFunctionsTask task = new DatabricksFunctionsTask(c -> true, s -> true);
      MemoryByteSink sink = new MemoryByteSink();
      task.doRun(context, sink, handle);

      List<String> lines = readLines(sink);
      assertEquals(2, lines.size());
      assertEquals(
          "FunctionCatalog,FunctionSchema,FunctionName,DataType,InputParams,RoutineDefinition,RoutineLanguage,Comment,Owner",
          lines.get(0));
      assertEquals(
          "my_catalog,my_schema,add_one,int,,RETURN x + 1,SQL,adds one,david", lines.get(1));
    } finally {
      System.clearProperty(AbstractDatabricksTask.RETRY_BACKOFF_PROPERTY);
    }
  }

  @Test
  public void hiveMetastoreSchemataTask_writesExpectedCsv() throws Exception {
    StatementResponse resp =
        new StatementResponse()
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
            .setResult(
                new ResultData()
                    .setDataArray(
                        Collections.singletonList(Collections.singletonList("sales_db"))));
    when(statementExecutionAPI.executeStatement(any(ExecuteStatementRequest.class)))
        .thenReturn(resp);

    DatabricksHiveMetastoreSchemataTask task = new DatabricksHiveMetastoreSchemataTask(s -> true);
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(2, lines.size());
    assertEquals("CatalogName,SchemaName,Comment,Owner,CreatedAt,UpdatedAt", lines.get(0));
    assertEquals("hive_metastore,sales_db,,,,", lines.get(1));
  }

  @Test
  public void hiveMetastoreTablesTask_writesExpectedCsv() throws Exception {
    StatementResponse respSchemas =
        new StatementResponse()
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
            .setResult(
                new ResultData()
                    .setDataArray(
                        Collections.singletonList(Collections.singletonList("sales_db"))));
    StatementResponse respTables =
        new StatementResponse()
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
            .setResult(
                new ResultData()
                    .setDataArray(
                        Arrays.asList(
                            Arrays.asList("sales_db", "transactions", "false"),
                            Arrays.asList("sales_db", "v_active_orders", "false"),
                            Arrays.asList("sales_db", "temp_orders", "true"))));
    StatementResponse respViews =
        new StatementResponse()
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
            .setResult(
                new ResultData()
                    .setDataArray(
                        Collections.singletonList(
                            Arrays.asList("sales_db", "v_active_orders", "false"))));

    when(statementExecutionAPI.executeStatement(
            argThat(r -> r != null && r.getStatement().contains("SCHEMAS"))))
        .thenReturn(respSchemas);
    when(statementExecutionAPI.executeStatement(
            argThat(r -> r != null && r.getStatement().contains("VIEWS"))))
        .thenReturn(respViews);
    when(statementExecutionAPI.executeStatement(
            argThat(r -> r != null && r.getStatement().contains("TABLES"))))
        .thenReturn(respTables);

    DatabricksHiveMetastoreTablesTask task = new DatabricksHiveMetastoreTablesTask(s -> true);
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(4, lines.size());
    assertEquals(
        "TableCatalog,TableSchema,TableName,TableType,DataSourceFormat,StorageLocation,Comment,Owner,CreatedAt,UpdatedAt",
        lines.get(0));
    assertEquals("hive_metastore,sales_db,transactions,MANAGED,,,,,,", lines.get(1));
    assertEquals("hive_metastore,sales_db,v_active_orders,VIEW,,,,,,", lines.get(2));
    assertEquals("hive_metastore,sales_db,temp_orders,TEMPORARY,,,,,,", lines.get(3));
  }

  @Test
  public void hiveMetastoreViewsTask_writesExpectedCsv() throws Exception {
    StatementResponse respSchemas =
        new StatementResponse()
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
            .setResult(
                new ResultData()
                    .setDataArray(
                        Collections.singletonList(Collections.singletonList("sales_db"))));
    StatementResponse respViews =
        new StatementResponse()
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
            .setResult(
                new ResultData()
                    .setDataArray(
                        Collections.singletonList(
                            Arrays.asList("sales_db", "v_active_orders", "false"))));
    StatementResponse respCreateTable =
        new StatementResponse()
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
            .setResult(
                new ResultData()
                    .setDataArray(
                        Collections.singletonList(
                            Collections.singletonList(
                                "CREATE VIEW v_active_orders AS SELECT * FROM orders WHERE status = 'ACTIVE'"))));

    when(statementExecutionAPI.executeStatement(
            argThat(r -> r != null && r.getStatement().contains("SCHEMAS"))))
        .thenReturn(respSchemas);
    when(statementExecutionAPI.executeStatement(
            argThat(r -> r != null && r.getStatement().contains("SHOW VIEWS"))))
        .thenReturn(respViews);
    when(statementExecutionAPI.executeStatement(
            argThat(r -> r != null && r.getStatement().contains("SHOW CREATE TABLE"))))
        .thenReturn(respCreateTable);

    DatabricksHiveMetastoreViewsTask task = new DatabricksHiveMetastoreViewsTask(s -> true);
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(2, lines.size());
    assertEquals("TableCatalog,TableSchema,TableName,ViewDefinition", lines.get(0));
    assertEquals(
        "hive_metastore,sales_db,v_active_orders,CREATE VIEW v_active_orders AS SELECT * FROM orders WHERE status = 'ACTIVE'",
        lines.get(1));
  }

  @Test
  public void hiveMetastoreColumnsTask_writesExpectedCsv() throws Exception {
    StatementResponse respSchemas =
        new StatementResponse()
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
            .setResult(
                new ResultData()
                    .setDataArray(
                        Collections.singletonList(Collections.singletonList("sales_db"))));
    StatementResponse respTables =
        new StatementResponse()
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
            .setResult(
                new ResultData()
                    .setDataArray(
                        Collections.singletonList(
                            Arrays.asList("sales_db", "transactions", "false"))));
    StatementResponse respDescribe =
        new StatementResponse()
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
            .setResult(
                new ResultData()
                    .setDataArray(
                        Arrays.asList(
                            Arrays.asList("order_id", "bigint", "primary id"),
                            Arrays.asList("amount", "decimal(10,2)", null),
                            Arrays.asList("", "", ""),
                            Arrays.asList("# Partition Information", "", ""),
                            Arrays.asList("# col_name", "data_type", "comment"),
                            Arrays.asList("dt", "date", "partition date"),
                            Arrays.asList("# Detailed Table Information", "", ""),
                            Arrays.asList("Database", "sales_db", ""))));

    when(statementExecutionAPI.executeStatement(
            argThat(r -> r != null && r.getStatement().contains("SCHEMAS"))))
        .thenReturn(respSchemas);
    when(statementExecutionAPI.executeStatement(
            argThat(r -> r != null && r.getStatement().contains("TABLES"))))
        .thenReturn(respTables);
    when(statementExecutionAPI.executeStatement(
            argThat(r -> r != null && r.getStatement().contains("DESCRIBE TABLE"))))
        .thenReturn(respDescribe);

    DatabricksHiveMetastoreColumnsTask task = new DatabricksHiveMetastoreColumnsTask(s -> true);
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(4, lines.size());
    assertEquals(
        "TableCatalog,TableSchema,TableName,OrdinalPosition,ColumnName,DataType,IsNullable,Comment,PartitionIndex",
        lines.get(0));
    assertEquals(
        "hive_metastore,sales_db,transactions,1,order_id,bigint,true,primary id,", lines.get(1));
    assertEquals(
        "hive_metastore,sales_db,transactions,2,amount,\"decimal(10,2)\",true,,", lines.get(2));
    assertEquals(
        "hive_metastore,sales_db,transactions,3,dt,date,true,partition date,1", lines.get(3));
  }
}
