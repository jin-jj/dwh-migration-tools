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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ServiceError;
import com.databricks.sdk.service.sql.StatementExecutionAPI;
import com.databricks.sdk.service.sql.StatementResponse;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
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
import org.mockito.ArgumentCaptor;

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

  private static final ImmutableList<String> NO_NAMES = ImmutableList.of();

  /** Returns a filter that accepts every catalog except {@code excluded}. */
  private static DatabricksFilter excluding(String excluded) {
    return new DatabricksFilter(ImmutableList.of(), ImmutableList.of(), ImmutableList.of(excluded));
  }

  /**
   * The filter exists to stop the warehouse producing rows the dump would discard, so it has to
   * reach the statement and not only the row handler. A metastore-wide scan that is sorted cannot
   * stream, so an unrestricted statement makes the warehouse materialise every catalog before the
   * first byte of a one-catalog dump is shipped.
   */
  @Test
  public void systemTablesTask_withAFilter_restrictsTheStatement() throws Exception {
    mockSqlQuery("system.information_schema.tables", Collections.emptyList());
    DatabricksFilter filter =
        new DatabricksFilter(
            ImmutableList.of("Main"), ImmutableList.of("Sales"), ImmutableList.of("system"));

    new DatabricksSystemSqlTablesTask(filter).doRun(context, new MemoryByteSink(), handle);

    ArgumentCaptor<ExecuteStatementRequest> request =
        ArgumentCaptor.forClass(ExecuteStatementRequest.class);
    verify(statementAPI).executeStatement(request.capture());
    String sql = request.getValue().getStatement();
    assertTrue(sql, sql.contains(filter.whereClause("table_catalog", "table_schema")));
    assertTrue(
        "The restriction has to precede the sort, or the sort still reads everything: " + sql,
        sql.indexOf(" WHERE ") < sql.indexOf(" ORDER BY "));
  }

  /**
   * The per-catalog tier issues one statement per catalog, so the catalog is already fixed and
   * naming it again in the statement would be redundant.
   */
  @Test
  public void perCatalogTablesTask_withAFilter_restrictsOnlyTheSchema() throws Exception {
    mockSqlQuery("SHOW CATALOGS", Collections.singletonList(Collections.singletonList("main")));
    mockSqlQuery(".information_schema.tables", Collections.emptyList());
    DatabricksFilter filter =
        new DatabricksFilter(ImmutableList.of("main"), ImmutableList.of("sales"), NO_NAMES);

    new DatabricksSqlTablesTask(filter).doRun(context, new MemoryByteSink(), handle);

    ArgumentCaptor<ExecuteStatementRequest> request =
        ArgumentCaptor.forClass(ExecuteStatementRequest.class);
    verify(statementAPI, org.mockito.Mockito.atLeastOnce()).executeStatement(request.capture());
    String sql = null;
    for (ExecuteStatementRequest candidate : request.getAllValues()) {
      if (candidate.getStatement().contains(".information_schema.tables")) {
        sql = candidate.getStatement();
      }
    }
    assertTrue("No per-catalog statement was issued", sql != null);
    assertTrue(sql, sql.contains("lower(table_schema) IN ('sales')"));
    assertTrue(
        "The catalog is already scoped by the loop: " + sql, !sql.contains("table_catalog)"));
  }

  /**
   * Every connector writes the dialect declared by {@link AbstractTask#FORMAT}. These tasks used to
   * redeclare their own {@code FORMAT}, which shadowed it and silently gave the Databricks dump
   * CRLF line endings and no escape character while every other dump used LF and a backslash.
   *
   * <p>The other tests here split on either line ending, so nothing caught it. This one does not.
   */
  @Test
  public void systemTablesTask_writesTheConnectorWideCsvDialect() throws Exception {
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
    MemoryByteSink sink = new MemoryByteSink();

    new DatabricksSystemSqlTablesTask(DatabricksFilter.all()).doRun(context, sink, handle);

    String content = sink.openStream().toString();
    String separator = AbstractTask.FORMAT.getRecordSeparator();
    assertEquals(
        "Record count is wrong for separator " + separator.replace("\n", "\\n"),
        2,
        content.split(java.util.regex.Pattern.quote(separator), -1).length - 1);
    assertTrue(
        "The dump must not carry a separator the rest of the connectors do not use",
        !separator.contains("\r") == !content.contains("\r"));
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

    DatabricksSqlCatalogsTask task = new DatabricksSqlCatalogsTask(excluding("samples"));
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

    DatabricksSqlSchemataTask task = new DatabricksSqlSchemataTask(DatabricksFilter.all());
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

    DatabricksSqlTablesTask task = new DatabricksSqlTablesTask(DatabricksFilter.all());
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

    DatabricksSqlColumnsTask task = new DatabricksSqlColumnsTask(DatabricksFilter.all());
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

    DatabricksSqlViewsTask task = new DatabricksSqlViewsTask(DatabricksFilter.all());
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(2, lines.size());
    assertEquals("TableCatalog,TableSchema,TableName,ViewDefinition", lines.get(0));
    assertEquals("my_catalog,my_schema,v_orders,SELECT * FROM orders", lines.get(1));
  }

  @Test
  public void tableConstraintsTask_writesOneRecordPerConstraint() throws Exception {
    mockSqlQuery(
        "SHOW CATALOGS", Collections.singletonList(Collections.singletonList("my_catalog")));
    mockSqlQuery(
        "information_schema.table_constraints",
        Arrays.asList(
            Arrays.asList(
                "my_catalog",
                "my_schema",
                "orders",
                "fk_customers",
                "FOREIGN KEY",
                "customer_id",
                "customers",
                "id"),
            Arrays.asList(
                "my_catalog",
                "my_schema",
                "orders",
                "pk_orders",
                "PRIMARY KEY",
                "order_id",
                null,
                null)));

    DatabricksSqlTableConstraintsTask task =
        new DatabricksSqlTableConstraintsTask(DatabricksFilter.all());
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(3, lines.size());
    assertEquals(
        "TableCatalog,TableSchema,TableName,ConstraintName,ConstraintType,ConstraintDetails",
        lines.get(0));
    assertEquals(
        "my_catalog,my_schema,orders,fk_customers,FOREIGN KEY,customer_id -> customers(id)",
        lines.get(1));
    assertEquals("my_catalog,my_schema,orders,pk_orders,PRIMARY KEY,order_id", lines.get(2));
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

    DatabricksSqlFunctionsTask task = new DatabricksSqlFunctionsTask(DatabricksFilter.all());
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

    DatabricksHiveMetastoreSchemataTask task =
        new DatabricksHiveMetastoreSchemataTask(DatabricksFilter.all());
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

    DatabricksHiveMetastoreTablesTask task =
        new DatabricksHiveMetastoreTablesTask(DatabricksFilter.all());
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
  public void hiveMetastoreTablesTask_scopesTheCatalogOnTheRequestNotTheStatement()
      throws Exception {
    mockHiveMetastoreSchema();

    new DatabricksHiveMetastoreTablesTask(DatabricksFilter.all())
        .doRun(context, new MemoryByteSink(), handle);

    ArgumentCaptor<ExecuteStatementRequest> captor =
        ArgumentCaptor.forClass(ExecuteStatementRequest.class);
    verify(statementAPI, atLeastOnce()).executeStatement(captor.capture());

    ExecuteStatementRequest walk = null;
    for (ExecuteStatementRequest request : captor.getAllValues()) {
      if (request.getStatement().contains("SHOW TABLE EXTENDED")) {
        walk = request;
      }
    }
    assertNotNull("The task never issued SHOW TABLE EXTENDED.", walk);

    // Databricks rejects `SHOW TABLE EXTENDED IN hive_metastore.<schema>` outright with
    // CROSS_CATALOG_SCHEMA_REFERENCE_NOT_SUPPORTED, so the catalog has to reach the server as
    // request context instead. A separate `USE CATALOG` statement would not do: statements are
    // independent, and the setting would be gone by the time this one ran.
    assertEquals("hive_metastore", walk.getCatalog());
    assertFalse(
        "The schema must not be catalog-qualified: " + walk.getStatement(),
        walk.getStatement().contains("hive_metastore."));
  }

  @Test
  public void hiveMetastoreColumnsTask_writesExpectedCsv() throws Exception {
    mockHiveMetastoreSchema();

    DatabricksHiveMetastoreColumnsTask task =
        new DatabricksHiveMetastoreColumnsTask(DatabricksFilter.all());
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

    DatabricksHiveMetastoreViewsTask task =
        new DatabricksHiveMetastoreViewsTask(DatabricksFilter.all());
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
        "SHOW TABLE EXTENDED IN `hms_schema`",
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
        new DatabricksSystemSqlCatalogsTask(excluding("samples"));
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
        new DatabricksSystemSqlSchemataTask(DatabricksFilter.all());
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

    DatabricksSystemSqlTablesTask task = new DatabricksSystemSqlTablesTask(DatabricksFilter.all());
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

    DatabricksSystemSqlColumnsTask task =
        new DatabricksSystemSqlColumnsTask(DatabricksFilter.all());
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

    DatabricksSystemSqlViewsTask task = new DatabricksSystemSqlViewsTask(DatabricksFilter.all());
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(2, lines.size());
    assertEquals("TableCatalog,TableSchema,TableName,ViewDefinition", lines.get(0));
    assertEquals("my_catalog,my_schema,v_orders,SELECT * FROM orders", lines.get(1));
  }

  @Test
  public void systemTableConstraintsTask_writesOneRecordPerConstraint() throws Exception {
    mockSqlQuery(
        "system.information_schema.table_constraints",
        Arrays.asList(
            Arrays.asList(
                "my_catalog", "my_schema", "orders", "chk_total", "CHECK", null, null, null),
            Arrays.asList(
                "my_catalog",
                "my_schema",
                "orders",
                "fk_customers",
                "FOREIGN KEY",
                "customer_id",
                "customers",
                "id"),
            Arrays.asList(
                "my_catalog",
                "my_schema",
                "orders",
                "pk_orders",
                "PRIMARY KEY",
                "order_id",
                null,
                null),
            Arrays.asList(
                "my_catalog",
                "my_schema",
                "orders",
                "pk_orders",
                "PRIMARY KEY",
                "line",
                null,
                null)));

    DatabricksSystemSqlTableConstraintsTask task =
        new DatabricksSystemSqlTableConstraintsTask(DatabricksFilter.all());
    MemoryByteSink sink = new MemoryByteSink();
    task.doRun(context, sink, handle);

    List<String> lines = readLines(sink);
    assertEquals(4, lines.size());
    assertEquals(
        "TableCatalog,TableSchema,TableName,ConstraintName,ConstraintType,ConstraintDetails",
        lines.get(0));
    assertEquals("my_catalog,my_schema,orders,chk_total,CHECK,", lines.get(1));
    assertEquals(
        "my_catalog,my_schema,orders,fk_customers,FOREIGN KEY,customer_id -> customers(id)",
        lines.get(2));
    assertEquals(
        "my_catalog,my_schema,orders,pk_orders,PRIMARY KEY,\"order_id, line\"", lines.get(3));
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
        new DatabricksSystemSqlFunctionsTask(DatabricksFilter.all());
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

    DatabricksSystemSqlTablesTask task = new DatabricksSystemSqlTablesTask(DatabricksFilter.all());
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

    DatabricksSqlTablesTask task = new DatabricksSqlTablesTask(DatabricksFilter.all());
    List<String> catalogs = task.fetchMatchingCatalogs(handle);

    assertEquals(1, catalogs.size());
    assertEquals("accessible_catalog", catalogs.get(0));
  }

  private void mockSqlFailure(String expectedSqlSubstring, String errorMessage) {
    StatementResponse response = new StatementResponse();
    response.setStatementId("stmt-fail-" + Math.abs(expectedSqlSubstring.hashCode()));
    response.setStatus(
        new StatementStatus()
            .setState(StatementState.FAILED)
            .setError(new ServiceError().setMessage(errorMessage)));

    when(statementAPI.executeStatement(
            org.mockito.ArgumentMatchers.argThat(
                req ->
                    req != null
                        && req.getStatement() != null
                        && req.getStatement().contains(expectedSqlSubstring))))
        .thenReturn(response);
  }

  /**
   * One refused catalog must not cost the others their output, and must be remembered.
   *
   * <p>The refusal here is deliberately <em>reworded</em>: it carries the SQLSTATE but not the "USE
   * CATALOG on Catalog '<i>x</i>'" phrasing that the message parser reads. The catalog can still be
   * marked, because the loop already knows which catalog it was reading — which is the point of
   * marking it there rather than parsing the name back out of English prose.
   */
  @Test
  public void perCatalogTier_whenOneCatalogIsRefused_marksItAndKeepsTheRest() throws Exception {
    mockSqlQuery(
        "SHOW CATALOGS",
        Arrays.asList(Collections.singletonList("good"), Collections.singletonList("mingjial")));
    mockSqlQuery(
        "`good`", Collections.singletonList(Arrays.asList("good", "sales", "orders", "MANAGED")));
    mockSqlFailure("`mingjial`", "Permission denied for this principal. SQLSTATE: 42501");

    DatabricksSqlTablesTask task = new DatabricksSqlTablesTask(DatabricksFilter.all());
    task.doRun(context, new MemoryByteSink(), handle);

    assertTrue(
        "The refused catalog should be remembered", handle.isCatalogInaccessible("mingjial"));
    assertFalse("A readable catalog should not be marked", handle.isCatalogInaccessible("good"));
  }
}
