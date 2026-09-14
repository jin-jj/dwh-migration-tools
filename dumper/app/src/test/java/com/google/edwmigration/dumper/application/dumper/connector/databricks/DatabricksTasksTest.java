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
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.StatementExecutionAPI;
import com.databricks.sdk.service.sql.StatementResponse;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import com.google.edwmigration.dumper.application.dumper.task.MemoryByteSink;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksTasksTest {

  private WorkspaceClient client;
  private StatementExecutionAPI statementAPI;
  private DatabricksHandle handle;
  private TaskRunContext context;

  @Before
  public void setUp() {
    client = mock(WorkspaceClient.class);
    statementAPI = mock(StatementExecutionAPI.class);
    when(client.statementExecution()).thenReturn(statementAPI);
    handle = new DatabricksHandle(client, "warehouse123");
    context = mock(TaskRunContext.class);
  }

  private void mockSqlQuery(String expectedSqlSubstring, List<List<String>> rows) {
    StatementResponse response = new StatementResponse();
    response.setStatementId("stmt-" + Math.abs(expectedSqlSubstring.hashCode()));
    response.setStatus(new StatementStatus().setState(StatementState.SUCCEEDED));
    ResultData resultData = new ResultData();
    List<Collection<String>> collRows = new ArrayList<>();
    for (List<String> row : rows) {
      collRows.add(row);
    }
    resultData.setDataArray(collRows);
    response.setResult(resultData);

    when(statementAPI.executeStatement(
            org.mockito.ArgumentMatchers.argThat(
                req ->
                    req != null
                        && req.getStatement() != null
                        && req.getStatement().contains(expectedSqlSubstring))))
        .thenReturn(response);
  }

  private static List<String> readLines(MemoryByteSink sink) throws IOException {
    String content = sink.openStream().toString();
    if (content.isEmpty()) {
      return Collections.emptyList();
    }
    return Arrays.asList(content.split("\\r?\\n"));
  }

  @Test
  public void catalogsTask_writesExpectedCsv() throws Exception {
    mockSqlQuery(
        "SHOW CATALOGS",
        Arrays.asList(
            Collections.singletonList("my_catalog"), Collections.singletonList("samples")));
    mockSqlQuery(
        "DESCRIBE CATALOG EXTENDED `my_catalog`",
        Arrays.asList(
            Arrays.asList("Comment", "Production data"),
            Arrays.asList("Owner", "alice@example.com")));

    DatabricksSqlCatalogsTask task =
        new DatabricksSqlCatalogsTask(c -> !c.equalsIgnoreCase("samples"));
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(2, lines.size());
    assertEquals("CatalogName,Comment,Owner,CreatedAt,UpdatedAt", lines.get(0));
    assertEquals("my_catalog,Production data,alice@example.com,,", lines.get(1));
  }

  @Test
  public void schemataTask_writesExpectedCsv() throws Exception {
    mockSqlQuery(
        "SHOW CATALOGS", Collections.singletonList(Collections.singletonList("my_catalog")));
    mockSqlQuery(
        "information_schema.schemata",
        Collections.singletonList(
            Arrays.asList(
                "my_catalog",
                "my_schema",
                "Main schema",
                "bob@example.com",
                "1600000000000",
                "1700000000000")));

    DatabricksSqlSchemataTask task = new DatabricksSqlSchemataTask(c -> true, s -> true);
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(2, lines.size());
    assertEquals("CatalogName,SchemaName,Comment,Owner,CreatedAt,UpdatedAt", lines.get(0));
    assertEquals(
        "my_catalog,my_schema,Main schema,bob@example.com,1600000000000,1700000000000",
        lines.get(1));
  }

  @Test
  public void tablesTask_writesExpectedCsv() throws Exception {
    mockSqlQuery(
        "SHOW CATALOGS", Collections.singletonList(Collections.singletonList("my_catalog")));
    mockSqlQuery(
        "information_schema.tables",
        Collections.singletonList(
            Arrays.asList(
                "my_catalog",
                "my_schema",
                "orders",
                "MANAGED",
                "DELTA",
                "s3://warehouse/orders",
                "orders table",
                "charlie",
                "1200",
                "2200")));

    DatabricksSqlTablesTask task = new DatabricksSqlTablesTask(c -> true, s -> true);
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
    mockSqlQuery(
        "SHOW CATALOGS", Collections.singletonList(Collections.singletonList("my_catalog")));
    mockSqlQuery(
        "information_schema.columns",
        Arrays.asList(
            Arrays.asList(
                "my_catalog",
                "my_schema",
                "orders",
                "1",
                "order_id",
                "bigint",
                "false",
                "primary id",
                ""),
            Arrays.asList(
                "my_catalog",
                "my_schema",
                "orders",
                "2",
                "details",
                "struct<item:string,qty:int>",
                "true",
                "",
                "")));

    DatabricksSqlColumnsTask task = new DatabricksSqlColumnsTask(c -> true, s -> true);
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
    mockSqlQuery(
        "SHOW CATALOGS", Collections.singletonList(Collections.singletonList("my_catalog")));
    mockSqlQuery(
        "information_schema.views",
        Collections.singletonList(
            Arrays.asList("my_catalog", "my_schema", "v_orders", "SELECT * FROM orders")));

    DatabricksSqlViewsTask task = new DatabricksSqlViewsTask(c -> true, s -> true);
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(2, lines.size());
    assertEquals("TableCatalog,TableSchema,TableName,ViewDefinition", lines.get(0));
    assertEquals("my_catalog,my_schema,v_orders,SELECT * FROM orders", lines.get(1));
  }

  @Test
  public void tableConstraintsTask_writesExpectedCsv() throws Exception {
    mockSqlQuery(
        "SHOW CATALOGS", Collections.singletonList(Collections.singletonList("my_catalog")));
    mockSqlQuery(
        "PRIMARY KEY",
        Collections.singletonList(
            Arrays.asList("my_catalog", "my_schema", "orders", "pk_orders", "order_id")));
    mockSqlQuery(
        "FOREIGN KEY",
        Collections.singletonList(
            Arrays.asList(
                "my_catalog",
                "my_schema",
                "orders",
                "fk_customers",
                "customer_id",
                "customers",
                "id")));
    mockSqlQuery("NOT IN ('PRIMARY KEY', 'FOREIGN KEY')", Collections.emptyList());

    DatabricksSqlTableConstraintsTask task =
        new DatabricksSqlTableConstraintsTask(c -> true, s -> true);
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(3, lines.size());
    assertEquals(
        "TableCatalog,TableSchema,TableName,ConstraintName,ConstraintType,ConstraintDetails",
        lines.get(0));
    assertEquals("my_catalog,my_schema,orders,pk_orders,PRIMARY KEY,order_id", lines.get(1));
    assertEquals(
        "my_catalog,my_schema,orders,fk_customers,FOREIGN KEY,customer_id -> customers(id)",
        lines.get(2));
  }

  @Test
  public void functionsTask_writesExpectedCsv() throws Exception {
    mockSqlQuery(
        "SHOW CATALOGS", Collections.singletonList(Collections.singletonList("my_catalog")));
    mockSqlQuery(
        "information_schema.parameters",
        Collections.singletonList(Arrays.asList("my_schema", "add_one", "x", "int")));
    mockSqlQuery(
        "information_schema.routines",
        Collections.singletonList(
            Arrays.asList(
                "my_catalog",
                "my_schema",
                "add_one",
                "int",
                "RETURN x + 1",
                "SQL",
                "adds one",
                "eve")));

    DatabricksSqlFunctionsTask task = new DatabricksSqlFunctionsTask(c -> true, s -> true);
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(2, lines.size());
    assertEquals(
        "FunctionCatalog,FunctionSchema,FunctionName,DataType,InputParams,RoutineDefinition,RoutineLanguage,Comment,Owner",
        lines.get(0));
    assertEquals(
        "my_catalog,my_schema,add_one,int,x int,RETURN x + 1,SQL,adds one,eve", lines.get(1));
  }

  @Test
  public void hiveMetastoreSchemataTask_writesExpectedCsv() throws Exception {
    mockSqlQuery(
        "SHOW SCHEMAS IN hive_metastore",
        Collections.singletonList(Collections.singletonList("hms_schema")));
    mockSqlQuery(
        "DESCRIBE SCHEMA EXTENDED hive_metastore.`hms_schema`",
        Arrays.asList(
            Arrays.asList("Database Name", "hms_schema"),
            Arrays.asList("Comment", "legacy sales data"),
            Arrays.asList("Owner", "alice@example.com")));

    DatabricksHiveMetastoreSchemataTask task = new DatabricksHiveMetastoreSchemataTask(s -> true);
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(2, lines.size());
    assertEquals("CatalogName,SchemaName,Comment,Owner,CreatedAt,UpdatedAt", lines.get(0));
    assertEquals("hive_metastore,hms_schema,legacy sales data,alice@example.com,,", lines.get(1));
  }

  @Test
  public void hiveMetastoreCatalogsTask_writesTheLegacyCatalog() throws Exception {
    DatabricksHiveMetastoreCatalogsTask task = new DatabricksHiveMetastoreCatalogsTask();
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(2, lines.size());
    assertEquals("CatalogName,Comment,Owner,CreatedAt,UpdatedAt", lines.get(0));
    assertTrue(lines.get(1).startsWith("hive_metastore,"));
  }

  @Test
  public void hiveMetastoreTablesTask_writesExpectedCsv() throws Exception {
    mockHiveMetastoreSchema();

    DatabricksHiveMetastoreTablesTask task = new DatabricksHiveMetastoreTablesTask(s -> true);
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(3, lines.size());
    assertEquals(
        "TableCatalog,TableSchema,TableName,TableType,DataSourceFormat,StorageLocation,Comment,Owner,CreatedAt,UpdatedAt",
        lines.get(0));
    assertEquals(
        "hive_metastore,hms_schema,hms_table,EXTERNAL,parquet,s3://bucket/hms_table,orders table,"
            + "alice@example.com,"
            + expectedCreatedAtMillis()
            + ",",
        lines.get(1));
    assertEquals("hive_metastore,hms_schema,v_table,VIEW,,,,,,", lines.get(2));
  }

  @Test
  public void hiveMetastoreColumnsTask_writesExpectedCsv() throws Exception {
    mockHiveMetastoreSchema();

    DatabricksHiveMetastoreColumnsTask task = new DatabricksHiveMetastoreColumnsTask(s -> true);
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(5, lines.size());
    assertEquals(
        "TableCatalog,TableSchema,TableName,OrdinalPosition,ColumnName,DataType,IsNullable,Comment,PartitionIndex",
        lines.get(0));
    assertEquals("hive_metastore,hms_schema,hms_table,1,id,BIGINT,false,,", lines.get(1));
    // 'country' partitions the table, so it carries a partition index.
    assertEquals("hive_metastore,hms_schema,hms_table,2,country,STRING,true,,1", lines.get(2));
    // A nested struct keeps its field list rather than degrading to the bare word 'struct'.
    assertEquals(
        "hive_metastore,hms_schema,hms_table,3,addr,\"STRUCT<city: STRING, zip: INT>\",true,,",
        lines.get(3));
    assertEquals("hive_metastore,hms_schema,v_table,1,one,INT,false,,", lines.get(4));
  }

  @Test
  public void hiveMetastoreViewsTask_writesExpectedCsv() throws Exception {
    mockHiveMetastoreSchema();

    DatabricksHiveMetastoreViewsTask task = new DatabricksHiveMetastoreViewsTask(s -> true);
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(2, lines.size());
    assertEquals("TableCatalog,TableSchema,TableName,ViewDefinition", lines.get(0));
    assertEquals("hive_metastore,hms_schema,v_table,SELECT 1 AS one", lines.get(1));
  }

  private static final String HMS_CREATED_TIME = "Thu Jan 01 00:00:00 UTC 2015";

  /** One table and one view, described the way SHOW TABLE EXTENDED describes them. */
  private void mockHiveMetastoreSchema() {
    mockSqlQuery(
        "SHOW SCHEMAS IN hive_metastore",
        Collections.singletonList(Collections.singletonList("hms_schema")));
    mockSqlQuery(
        "SHOW TABLE EXTENDED IN hive_metastore.`hms_schema`",
        Arrays.asList(
            Arrays.asList(
                "hms_schema",
                "hms_table",
                "false",
                "Catalog: hive_metastore\n"
                    + "Database: hms_schema\n"
                    + "Table: hms_table\n"
                    + "Owner: alice@example.com\n"
                    + "Created Time: "
                    + HMS_CREATED_TIME
                    + "\n"
                    + "Last Access: UNKNOWN\n"
                    + "Created By: Spark 3.4.1\n"
                    + "Type: EXTERNAL\n"
                    + "Provider: parquet\n"
                    + "Comment: orders table\n"
                    + "Location: s3://bucket/hms_table\n"
                    + "Partition Provider: Catalog\n"
                    + "Partition Columns: [`country`]\n"
                    + "Schema: root\n"
                    + " |-- id: long (nullable = false)\n"
                    + " |-- country: string (nullable = true)\n"
                    + " |-- addr: struct (nullable = true)\n"
                    + " |    |-- city: string (nullable = true)\n"
                    + " |    |-- zip: integer (nullable = true)\n"),
            Arrays.asList(
                "hms_schema",
                "v_table",
                "false",
                "Catalog: hive_metastore\n"
                    + "Database: hms_schema\n"
                    + "Table: v_table\n"
                    + "Type: VIEW\n"
                    + "View Text: SELECT 1 AS one\n"
                    + "Schema: root\n"
                    + " |-- one: integer (nullable = false)\n")));
  }

  private static long expectedCreatedAtMillis() throws ParseException {
    return new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ROOT)
        .parse(HMS_CREATED_TIME)
        .getTime();
  }

  @Test
  public void systemCatalogsTask_writesExpectedCsv() throws Exception {
    mockSqlQuery(
        "system.information_schema.catalogs",
        Collections.singletonList(
            Arrays.asList(
                "my_catalog",
                "Production data",
                "alice@example.com",
                "1600000000000",
                "1700000000000")));

    DatabricksSystemSqlCatalogsTask task =
        new DatabricksSystemSqlCatalogsTask(c -> !c.equalsIgnoreCase("samples"));
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(2, lines.size());
    assertEquals("CatalogName,Comment,Owner,CreatedAt,UpdatedAt", lines.get(0));
    assertEquals(
        "my_catalog,Production data,alice@example.com,1600000000000,1700000000000", lines.get(1));
  }

  @Test
  public void systemSchemataTask_writesExpectedCsv() throws Exception {
    mockSqlQuery(
        "system.information_schema.schemata",
        Collections.singletonList(
            Arrays.asList(
                "my_catalog",
                "my_schema",
                "Main schema",
                "bob@example.com",
                "1600000000000",
                "1700000000000")));

    DatabricksSystemSqlSchemataTask task =
        new DatabricksSystemSqlSchemataTask(c -> true, s -> true);
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(2, lines.size());
    assertEquals("CatalogName,SchemaName,Comment,Owner,CreatedAt,UpdatedAt", lines.get(0));
    assertEquals(
        "my_catalog,my_schema,Main schema,bob@example.com,1600000000000,1700000000000",
        lines.get(1));
  }

  @Test
  public void systemTablesTask_writesExpectedCsv() throws Exception {
    mockSqlQuery(
        "system.information_schema.tables",
        Collections.singletonList(
            Arrays.asList(
                "my_catalog",
                "my_schema",
                "orders",
                "MANAGED",
                "DELTA",
                "s3://warehouse/orders",
                "orders table",
                "charlie",
                "1200",
                "2200")));

    DatabricksSystemSqlTablesTask task = new DatabricksSystemSqlTablesTask(c -> true, s -> true);
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
  public void systemColumnsTask_writesExpectedCsv() throws Exception {
    mockSqlQuery(
        "system.information_schema.columns",
        Arrays.asList(
            Arrays.asList(
                "my_catalog",
                "my_schema",
                "orders",
                "1",
                "order_id",
                "bigint",
                "false",
                "primary id",
                ""),
            Arrays.asList(
                "my_catalog",
                "my_schema",
                "orders",
                "2",
                "details",
                "struct<item:string,qty:int>",
                "true",
                "",
                "")));

    DatabricksSystemSqlColumnsTask task = new DatabricksSystemSqlColumnsTask(c -> true, s -> true);
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
  public void systemViewsTask_writesExpectedCsv() throws Exception {
    mockSqlQuery(
        "system.information_schema.views",
        Collections.singletonList(
            Arrays.asList("my_catalog", "my_schema", "v_orders", "SELECT * FROM orders")));

    DatabricksSystemSqlViewsTask task = new DatabricksSystemSqlViewsTask(c -> true, s -> true);
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(2, lines.size());
    assertEquals("TableCatalog,TableSchema,TableName,ViewDefinition", lines.get(0));
    assertEquals("my_catalog,my_schema,v_orders,SELECT * FROM orders", lines.get(1));
  }

  @Test
  public void systemTableConstraintsTask_writesExpectedCsv() throws Exception {
    mockSqlQuery(
        "tc.constraint_type = 'PRIMARY KEY'",
        Collections.singletonList(
            Arrays.asList("my_catalog", "my_schema", "orders", "pk_orders", "order_id")));
    mockSqlQuery(
        "tc.constraint_type = 'FOREIGN KEY'",
        Collections.singletonList(
            Arrays.asList(
                "my_catalog",
                "my_schema",
                "orders",
                "fk_customers",
                "customer_id",
                "customers",
                "id")));
    mockSqlQuery("NOT IN ('PRIMARY KEY', 'FOREIGN KEY')", Collections.emptyList());

    DatabricksSystemSqlTableConstraintsTask task =
        new DatabricksSystemSqlTableConstraintsTask(c -> true, s -> true);
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(3, lines.size());
    assertEquals(
        "TableCatalog,TableSchema,TableName,ConstraintName,ConstraintType,ConstraintDetails",
        lines.get(0));
    assertEquals("my_catalog,my_schema,orders,pk_orders,PRIMARY KEY,order_id", lines.get(1));
    assertEquals(
        "my_catalog,my_schema,orders,fk_customers,FOREIGN KEY,customer_id -> customers(id)",
        lines.get(2));
  }

  @Test
  public void systemFunctionsTask_writesExpectedCsv() throws Exception {
    mockSqlQuery(
        "system.information_schema.parameters",
        Collections.singletonList(Arrays.asList("my_catalog", "my_schema", "add_one", "x", "int")));
    mockSqlQuery(
        "system.information_schema.routines",
        Collections.singletonList(
            Arrays.asList(
                "my_catalog",
                "my_schema",
                "add_one",
                "int",
                "RETURN x + 1",
                "SQL",
                "adds one",
                "eve")));

    DatabricksSystemSqlFunctionsTask task =
        new DatabricksSystemSqlFunctionsTask(c -> true, s -> true);
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(2, lines.size());
    assertEquals(
        "FunctionCatalog,FunctionSchema,FunctionName,DataType,InputParams,RoutineDefinition,RoutineLanguage,Comment,Owner",
        lines.get(0));
    assertEquals(
        "my_catalog,my_schema,add_one,int,x int,RETURN x + 1,SQL,adds one,eve", lines.get(1));
  }

  @Test
  public void systemTask_whenQueryFails_throwsSQLExceptionAndHandlesException() {
    StatementResponse response = new StatementResponse();
    response.setStatementId("stmt-fail");
    StatementStatus status = new StatementStatus().setState(StatementState.FAILED);
    com.databricks.sdk.service.sql.ServiceError error =
        new com.databricks.sdk.service.sql.ServiceError()
            .setMessage(
                "[INSUFFICIENT_PERMISSIONS] User does not have USE CATALOG on Catalog 'system'");
    status.setError(error);
    response.setStatus(status);

    when(statementAPI.executeStatement(any())).thenReturn(response);

    DatabricksSystemSqlTablesTask task = new DatabricksSystemSqlTablesTask(c -> true, s -> true);
    MemoryByteSink sink = new MemoryByteSink();

    try {
      task.doRun(context, sink, handle);
      org.junit.Assert.fail("Expected SQLException");
    } catch (Exception e) {
      org.junit.Assert.assertTrue(e instanceof java.sql.SQLException);
      org.junit.Assert.assertTrue(task.handleException(e));
    }
  }

  @Test
  public void inaccessibleCatalog_isOmittedFromMatchingCatalogs() throws Exception {
    handle.markCatalogInaccessible("dmishyn");
    mockSqlQuery(
        "SHOW CATALOGS",
        Arrays.asList(
            Collections.singletonList("accessible_catalog"), Collections.singletonList("dmishyn")));

    DatabricksSqlTablesTask task = new DatabricksSqlTablesTask(c -> true, s -> true);
    List<String> catalogs = task.fetchMatchingCatalogs(handle);

    assertEquals(1, catalogs.size());
    assertEquals("accessible_catalog", catalogs.get(0));
  }
}
