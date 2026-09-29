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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ServiceError;
import com.databricks.sdk.service.sql.StatementExecutionAPI;
import com.databricks.sdk.service.sql.StatementResponse;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSink;
import com.google.common.io.Files;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDumpFormat.ColumnsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDumpFormat.DdlsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDumpFormat.FunctionDdlsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDumpFormat.FunctionsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDumpFormat.SchemataHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDumpFormat.TableConstraintsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDumpFormat.TablesHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDumpFormat.ViewsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreFunctionParser.ParsedFunctionExtended;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreColumnsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreDdlsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreFunctionDdlsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreFunctionsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreSchemataTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreTableConstraintsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreTablesTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreViewsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.HiveMetastoreSessionCache;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksHiveMetastoreTasksTest {

  private static final String SCHEMA = "legacy";
  private static final String SHOW_SCHEMAS = "SHOW SCHEMAS";
  private static final String SHOW_TABLES = "SHOW TABLE EXTENDED IN `legacy` LIKE '*'";
  private static final String SHOW_FUNCTIONS = "SHOW USER FUNCTIONS IN `legacy`";
  private static final int MAX_CONCURRENT_STATEMENTS = 4;

  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void schemataTask_writesLocation() throws Exception {
    String location = "dbfs:/mnt/lake/legacy";
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                "DESCRIBE SCHEMA EXTENDED `legacy`",
                succeededRows(
                    ImmutableList.of(
                        ImmutableList.of("Catalog Name", DatabricksFilter.HIVE_METASTORE_CATALOG),
                        ImmutableList.of("Namespace Name", SCHEMA),
                        ImmutableList.of("Comment", "Old data"),
                        ImmutableList.of("Location", location),
                        ImmutableList.of("Owner", "alice"),
                        ImmutableList.of("Properties", "((team,migration))")))));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreSchemataTask(cache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    assertEquals(location, records.get(0).get(SchemataHeader.StorageLocation));
    assertEquals("Old data", records.get(0).get(SchemataHeader.Comment));
    assertEquals("alice", records.get(0).get(SchemataHeader.Owner));
  }

  @Test
  public void schemataTask_keepsSchemaWhoseDescribeFails() throws Exception {
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA, "locked"),
                "DESCRIBE SCHEMA EXTENDED `legacy`",
                succeededRows(
                    ImmutableList.of(
                        ImmutableList.of("Comment", "Old data"),
                        ImmutableList.of("Owner", "alice"))),
                "DESCRIBE SCHEMA EXTENDED `locked`",
                failed("[INSUFFICIENT_PERMISSIONS] Insufficient privileges.")));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreSchemataTask(cache).doRun(null, sink, handle));

    assertEquals(2, records.size());
    assertEquals("Old data", records.get(0).get(SchemataHeader.Comment));
    assertEquals("alice", records.get(0).get(SchemataHeader.Owner));
    assertEquals("locked", records.get(1).get(SchemataHeader.SchemaName));
    assertEquals("", records.get(1).get(SchemataHeader.Owner));
    assertEquals("", records.get(1).get(SchemataHeader.StorageLocation));
  }

  @Test
  public void schemataTask_writesOnlySchemasAcceptedByFilter() throws Exception {
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded("information_schema", SCHEMA, "other"),
                "DESCRIBE SCHEMA EXTENDED `legacy`",
                succeededRows(ImmutableList.of(ImmutableList.of("Owner", "alice")))));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();
    DatabricksFilter filter = new DatabricksFilter(ImmutableList.of(" LEGACY "));

    List<CSVRecord> records =
        runTask(
            statements,
            filter,
            (sink, handle) ->
                new DatabricksHiveMetastoreSchemataTask(cache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    assertEquals(SCHEMA, records.get(0).get(SchemataHeader.SchemaName));
    assertEquals("alice", records.get(0).get(SchemataHeader.Owner));
  }

  @Test
  public void tablesTask_writesHiveStorageColumns() throws Exception {
    String serdeLibrary = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
    String inputFormat = "org.apache.hadoop.mapred.TextInputFormat";
    String outputFormat = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
    String storageProperties = "[field.delim=,, serialization.format=,]";
    String tableProperties = "[transient_lastDdlTime=1790541380]";
    String information =
        "Catalog: hive_metastore\n"
            + "Database: legacy\n"
            + "Table: csv_events\n"
            + "Type: EXTERNAL\n"
            + "Provider: hive\n"
            + "Table Properties: "
            + tableProperties
            + "\n"
            + "Serde Library: "
            + serdeLibrary
            + "\n"
            + "InputFormat: "
            + inputFormat
            + "\n"
            + "OutputFormat: "
            + outputFormat
            + "\n"
            + "Storage Properties: "
            + storageProperties
            + "\n"
            + "Schema: root\n"
            + " |-- id: integer (nullable = true)\n";
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                showTableResponse("csv_events", information)));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreTablesTask(cache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    CSVRecord record = records.get(0);
    assertEquals("csv_events", record.get(TablesHeader.TableName));
    assertEquals(serdeLibrary, record.get(TablesHeader.SerdeLibrary));
    assertEquals(inputFormat, record.get(TablesHeader.InputFormat));
    assertEquals(outputFormat, record.get(TablesHeader.OutputFormat));
    assertEquals(storageProperties, record.get(TablesHeader.StorageProperties));
    assertEquals(tableProperties, record.get(TablesHeader.TableProperties));
  }

  @Test
  public void tablesTask_skipsTemporaryViews() throws Exception {
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                succeededRows(
                    ImmutableList.of(
                        ImmutableList.of(SCHEMA, "events", "false", tableInformation("events")),
                        ImmutableList.of("", "session_view", "true", viewInformation("x"))))));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreTablesTask(cache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    assertEquals("events", records.get(0).get(TablesHeader.TableName));
  }

  @Test
  public void tablesTask_skipsSchemaWhoseShowTableExtendedFails() throws Exception {
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded("locked", SCHEMA),
                "SHOW TABLE EXTENDED IN `locked` LIKE '*'",
                failed("[INSUFFICIENT_PERMISSIONS] Insufficient privileges."),
                SHOW_TABLES,
                showTablesResponse("events")));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreTablesTask(cache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    assertEquals(SCHEMA, records.get(0).get(TablesHeader.TableSchema));
    assertEquals("events", records.get(0).get(TablesHeader.TableName));
  }

  @Test
  public void tablesTask_failsWhenShowTableExtendedFailsForEverySchema() {
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                failed("[INSUFFICIENT_PERMISSIONS] Insufficient privileges.")));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    IOException thrown =
        assertThrows(
            IOException.class,
            () ->
                runTask(
                    statements,
                    (sink, handle) ->
                        new DatabricksHiveMetastoreTablesTask(cache).doRun(null, sink, handle)));

    assertTrue(thrown.getMessage().contains("SHOW TABLE EXTENDED"));
  }

  @Test
  public void tablesTask_throwsInterruptedIoExceptionWhenInterrupted() {
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS, succeeded(SCHEMA), SHOW_TABLES, showTablesResponse("events")));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    Thread.currentThread().interrupt();
    try {
      assertThrows(
          InterruptedIOException.class,
          () ->
              runTask(
                  statements,
                  (sink, handle) ->
                      new DatabricksHiveMetastoreTablesTask(cache).doRun(null, sink, handle)));
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  public void columnsTask_writesColumnsInSchemaOrder() throws Exception {
    String information =
        String.join(
            "\n",
            "Catalog: hive_metastore",
            "Database: legacy",
            "Table: events",
            "Type: MANAGED",
            "Provider: delta",
            "Partition Columns: [`dt`]",
            "Schema: root",
            " |-- id: long (nullable = false)",
            " |-- point: struct (nullable = true)",
            " |    |-- x: double (nullable = true)",
            " |-- dt: date (nullable = true)",
            "");
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                showTableResponse("events", information)));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreColumnsTask(cache).doRun(null, sink, handle));

    ImmutableList.Builder<String> columns = ImmutableList.builder();
    for (CSVRecord record : records) {
      columns.add(
          String.join(
              "|",
              record.get(ColumnsHeader.TableName),
              record.get(ColumnsHeader.OrdinalPosition),
              record.get(ColumnsHeader.ColumnName),
              record.get(ColumnsHeader.DataType),
              record.get(ColumnsHeader.IsNullable),
              record.get(ColumnsHeader.PartitionIndex)));
    }
    assertEquals(
        ImmutableList.of(
            "events|0|id|bigint|NO|",
            "events|1|point|struct<x:double>|YES|",
            "events|2|dt|date|YES|0"),
        columns.build());
  }

  @Test
  public void viewsTask_writesOnlyViews() throws Exception {
    String viewText = "SELECT id\nFROM legacy.events";
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                succeededRows(
                    ImmutableList.of(
                        ImmutableList.of(SCHEMA, "events", "false", tableInformation("events")),
                        ImmutableList.of(
                            SCHEMA, "event_ids", "false", viewInformation(viewText))))));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreViewsTask(cache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    assertEquals("event_ids", records.get(0).get(ViewsHeader.TableName));
    assertEquals(viewText, records.get(0).get(ViewsHeader.ViewDefinition));
  }

  @Test
  public void tableConstraintsTask_writesDeltaCheckConstraints() throws Exception {
    String information =
        String.join(
            "\n",
            "Catalog: hive_metastore",
            "Database: legacy",
            "Table: orders",
            "Type: MANAGED",
            "Provider: delta",
            "Table Properties: [delta.constraints.positive_amount=amount > 0,"
                + " delta.constraints.known_status=status IN ('new', 'done'),"
                + " delta.minReaderVersion=1]",
            "Schema: root",
            " |-- amount: integer (nullable = true)",
            " |-- status: integer (nullable = true)",
            "");
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                showTableResponse("orders", information)));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreTableConstraintsTask(cache).doRun(null, sink, handle));

    ImmutableList.Builder<String> constraints = ImmutableList.builder();
    for (CSVRecord record : records) {
      constraints.add(
          String.join(
              "|",
              record.get(TableConstraintsHeader.TableName),
              record.get(TableConstraintsHeader.ConstraintName),
              record.get(TableConstraintsHeader.ConstraintType),
              record.get(TableConstraintsHeader.ConstraintDetails)));
    }
    assertEquals(
        ImmutableList.of(
            "orders|positive_amount|CHECK|amount > 0",
            "orders|known_status|CHECK|status IN ('new', 'done')"),
        constraints.build());
  }

  @Test
  public void columnsTask_writesCharAndVarcharLengthsFromDescribeTable() throws Exception {
    String information =
        String.join(
            "\n",
            "Catalog: hive_metastore",
            "Database: legacy",
            "Table: customers",
            "Type: MANAGED",
            "Provider: delta",
            "Partition Columns: [`dt`]",
            "Schema: root",
            " |-- id: long (nullable = false)",
            " |-- name: string (nullable = true)",
            " |-- tags: array (nullable = true)",
            " |    |-- element: string (containsNull = true)",
            " |-- dt: date (nullable = true)",
            "");
    ImmutableList<List<String>> columnRows =
        ImmutableList.of(
            Arrays.asList("id", "bigint", "primary id"),
            Arrays.asList("name", "varchar(20)", null),
            Arrays.asList("tags", "array<char(2)>", null),
            Arrays.asList("dt", "date", null));
    List<List<String>> describeRows = new ArrayList<>(columnRows);
    describeRows.add(Arrays.asList("# Partition Information", "", ""));
    describeRows.add(Arrays.asList("# col_name", "data_type", "comment"));
    describeRows.add(Arrays.asList("dt", "date", null));
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                showTableResponse("customers", information),
                "DESCRIBE TABLE `legacy`.`customers`",
                succeededRows(describeRows)));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreColumnsTask(cache).doRun(null, sink, handle));

    ImmutableList.Builder<String> expectedTypes = ImmutableList.builder();
    for (List<String> row : columnRows) {
      expectedTypes.add(row.get(1));
    }
    ImmutableList.Builder<String> writtenTypes = ImmutableList.builder();
    for (CSVRecord record : records) {
      writtenTypes.add(record.get(ColumnsHeader.DataType));
    }
    assertEquals(expectedTypes.build(), writtenTypes.build());
  }

  @Test
  public void ddlsTask_skipsTableWhoseShowCreateTableFails() throws Exception {
    String deltaDdl = "CREATE TABLE hive_metastore.legacy.delta_tbl (id INT NOT NULL) USING delta";
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                showTablesResponse("delta_tbl", "hive_serde_tbl"),
                "SHOW CREATE TABLE `legacy`.`delta_tbl`",
                succeeded(deltaDdl),
                "SHOW CREATE TABLE `legacy`.`hive_serde_tbl`",
                failed(
                    "[UNSUPPORTED_SHOW_CREATE_TABLE.WITH_UNSUPPORTED_SERDE_CONFIGURATION]"
                        + " Unsupported SHOW CREATE TABLE command.")));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) -> new DatabricksHiveMetastoreDdlsTask(cache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    assertEquals("delta_tbl", records.get(0).get(DdlsHeader.TableName));
    assertEquals("MANAGED", records.get(0).get(DdlsHeader.TableType));
    assertEquals(deltaDdl, records.get(0).get(DdlsHeader.Ddl));
  }

  @Test
  public void functionDdlsTask_skipsFunctionWhoseDescribeFails() throws Exception {
    ImmutableList<String> describeLines =
        ImmutableList.of(
            "Function: hive_metastore.legacy.double_it",
            "Type: SCALAR",
            "Input: x INT",
            "Returns: INT",
            "Body: x * 2");
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_FUNCTIONS,
                succeeded("legacy.double_it", "legacy.broken_fn"),
                "DESCRIBE FUNCTION EXTENDED `legacy`.`double_it`",
                succeeded(describeLines.toArray(new String[0])),
                "DESCRIBE FUNCTION EXTENDED `legacy`.`broken_fn`",
                failed("[ROUTINE_NOT_FOUND] The routine `legacy`.`broken_fn` cannot be found.")));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreFunctionDdlsTask(cache).doRun(null, sink, handle));

    String expectedDdl =
        DatabricksHiveMetastoreFunctionParser.parseDescribeFunctionExtended(
                DatabricksFilter.HIVE_METASTORE_CATALOG, SCHEMA, "double_it", describeLines)
            .ddl;
    assertEquals(1, records.size());
    assertEquals("double_it", records.get(0).get(FunctionDdlsHeader.FunctionName));
    assertEquals(expectedDdl, records.get(0).get(FunctionDdlsHeader.Ddl));
  }

  @Test
  public void columnsTask_runsNoDescribeTableForTableWithoutStringColumns() throws Exception {
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS, succeeded(SCHEMA), SHOW_TABLES, showTablesResponse("numbers")));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreColumnsTask(cache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    assertEquals("int", records.get(0).get(ColumnsHeader.DataType));
    int showStatementCount = ImmutableList.of(SHOW_SCHEMAS, SHOW_TABLES).size();
    verify(statements, times(showStatementCount))
        .executeStatement(any(ExecuteStatementRequest.class));
  }

  @Test
  public void columnsTask_keepsStringTypeWhenDescribeTableFails() throws Exception {
    String information =
        String.join(
            "\n",
            "Catalog: hive_metastore",
            "Database: legacy",
            "Table: customers",
            "Type: MANAGED",
            "Provider: delta",
            "Schema: root",
            " |-- name: string (nullable = true)",
            "");
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                showTableResponse("customers", information),
                "DESCRIBE TABLE `legacy`.`customers`",
                failed("[INSUFFICIENT_PERMISSIONS] Insufficient privileges.")));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreColumnsTask(cache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    assertEquals("name", records.get(0).get(ColumnsHeader.ColumnName));
    assertEquals("string", records.get(0).get(ColumnsHeader.DataType));
  }

  @Test
  public void ddlsTask_skipsTableWhoseResultDownloadFails() throws Exception {
    String goodDdl = "CREATE TABLE hive_metastore.legacy.good_tbl (id INT) USING delta";
    String brokenStatementId = "stmt-broken";
    StatementResponse brokenResponse =
        new StatementResponse()
            .setStatementId(brokenStatementId)
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
            .setResult(
                new ResultData()
                    .setChunkIndex(0L)
                    .setNextChunkIndex(1L)
                    .setDataArray(rows(ImmutableList.of(ImmutableList.of("CREATE TABLE")))));
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                showTablesResponse("broken_tbl", "good_tbl"),
                "SHOW CREATE TABLE `legacy`.`broken_tbl`",
                brokenResponse,
                "SHOW CREATE TABLE `legacy`.`good_tbl`",
                succeeded(goodDdl)));
    when(statements.getStatementResultChunkN(brokenStatementId, 1L))
        .thenThrow(new DatabricksException("Connection reset"));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) -> new DatabricksHiveMetastoreDdlsTask(cache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    assertEquals("good_tbl", records.get(0).get(DdlsHeader.TableName));
    assertEquals(goodDdl, records.get(0).get(DdlsHeader.Ddl));
  }

  @Test
  public void ddlsTask_dropsTrailingLineBreak() throws Exception {
    String ddl = "CREATE TABLE hive_metastore.legacy.delta_tbl (\n  id INT)\nUSING delta";
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                showTablesResponse("delta_tbl"),
                "SHOW CREATE TABLE `legacy`.`delta_tbl`",
                succeeded(ddl + "\n")));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) -> new DatabricksHiveMetastoreDdlsTask(cache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    assertEquals(ddl, records.get(0).get(DdlsHeader.Ddl));
  }

  @Test
  public void functionsTask_writesSqlAndJavaFunctions() throws Exception {
    ImmutableList<String> sqlDescribeLines =
        ImmutableList.of(
            "Function: hive_metastore.legacy.double_it",
            "Type: SCALAR",
            "Input: x INT",
            "Returns: INT",
            "Comment: Doubles input",
            "Owner: alice",
            "Body: x * 2");
    ImmutableList<String> javaDescribeLines =
        ImmutableList.of(
            "Function: hive_metastore.legacy.custom_hash",
            "Class: com.acme.udf.CustomHashUdf",
            "Owner: bob");
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_FUNCTIONS,
                succeeded("legacy.double_it", "legacy.custom_hash"),
                "DESCRIBE FUNCTION EXTENDED `legacy`.`double_it`",
                succeeded(sqlDescribeLines.toArray(new String[0])),
                "DESCRIBE FUNCTION EXTENDED `legacy`.`custom_hash`",
                succeeded(javaDescribeLines.toArray(new String[0]))));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreFunctionsTask(cache).doRun(null, sink, handle));

    ImmutableList.Builder<List<String>> expected = ImmutableList.builder();
    expected.add(functionRow(parseFunction("double_it", sqlDescribeLines)));
    expected.add(functionRow(parseFunction("custom_hash", javaDescribeLines)));
    ImmutableList.Builder<List<String>> actual = ImmutableList.builder();
    for (CSVRecord record : records) {
      actual.add(record.toList());
    }
    assertEquals(expected.build(), actual.build());
  }

  @Test
  public void functionsTask_skipsSchemaWhoseShowUserFunctionsFails() throws Exception {
    String brokenSchema = "restricted";
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(brokenSchema, SCHEMA),
                "SHOW USER FUNCTIONS IN `" + brokenSchema + "`",
                failed("[INSUFFICIENT_PERMISSIONS] Insufficient privileges."),
                SHOW_FUNCTIONS,
                succeeded("legacy.double_it"),
                "DESCRIBE FUNCTION EXTENDED `legacy`.`double_it`",
                succeeded("Function: hive_metastore.legacy.double_it", "Body: 2")));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreFunctionsTask(cache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    assertEquals(SCHEMA, records.get(0).get(FunctionsHeader.FunctionSchema));
    assertEquals("double_it", records.get(0).get(FunctionsHeader.FunctionName));
  }

  @Test
  public void sessionCache_describesFunctionsOnceForFunctionsAndFunctionDdls() throws Exception {
    String describeFunction = "DESCRIBE FUNCTION EXTENDED `legacy`.`double_it`";
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_FUNCTIONS,
                succeeded("legacy.double_it"),
                describeFunction,
                succeeded("Function: hive_metastore.legacy.double_it", "Body: 2")));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    runTask(
        statements,
        (sink, handle) ->
            new DatabricksHiveMetastoreFunctionsTask(cache).doRun(null, sink, handle));
    runTask(
        statements,
        (sink, handle) ->
            new DatabricksHiveMetastoreFunctionDdlsTask(cache).doRun(null, sink, handle));

    int statementCount = ImmutableList.of(SHOW_SCHEMAS, SHOW_FUNCTIONS, describeFunction).size();
    verify(statements, times(statementCount)).executeStatement(any(ExecuteStatementRequest.class));
  }

  @Test
  public void sessionCache_runsShowTableExtendedOncePerSchemaForAllTasks() throws Exception {
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS, succeeded(SCHEMA), SHOW_TABLES, showTablesResponse("events")));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    runTask(
        statements,
        (sink, handle) -> new DatabricksHiveMetastoreTablesTask(cache).doRun(null, sink, handle));
    runTask(
        statements,
        (sink, handle) -> new DatabricksHiveMetastoreViewsTask(cache).doRun(null, sink, handle));

    int showStatementCount = ImmutableList.of(SHOW_SCHEMAS, SHOW_TABLES).size();
    verify(statements, times(showStatementCount))
        .executeStatement(any(ExecuteStatementRequest.class));
  }

  @Test
  public void sessionCache_doesNotRepeatFailedScan() throws Exception {
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS, failed("[INSUFFICIENT_PERMISSIONS] Insufficient privileges.")));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    try (DatabricksHandle handle =
        newHandle(statements, new DatabricksFilter(ImmutableList.of()))) {
      assertThrows(IOException.class, () -> cache.listParsedTables(handle));
      IOException repeated = assertThrows(IOException.class, () -> cache.listSchemas(handle));
      assertTrue(repeated.getMessage().contains("failed earlier"));
    }

    verify(statements, times(1)).executeStatement(any(ExecuteStatementRequest.class));
  }

  private static ParsedFunctionExtended parseFunction(String functionName, List<String> lines) {
    return DatabricksHiveMetastoreFunctionParser.parseDescribeFunctionExtended(
        DatabricksFilter.HIVE_METASTORE_CATALOG, SCHEMA, functionName, lines);
  }

  /** Returns the {@code functions.csv} row of a function in {@link FunctionsHeader} order. */
  private static List<String> functionRow(ParsedFunctionExtended function) {
    return ImmutableList.of(
        function.functionCatalog,
        function.functionSchema,
        function.functionName,
        function.dataType,
        function.inputParams,
        function.routineDefinition,
        function.routineLanguage,
        function.comment,
        function.owner);
  }

  private interface TaskBody {

    void run(ByteSink sink, DatabricksHandle handle) throws Exception;
  }

  private List<CSVRecord> runTask(StatementExecutionAPI statements, TaskBody body)
      throws Exception {
    return runTask(statements, new DatabricksFilter(ImmutableList.of()), body);
  }

  private List<CSVRecord> runTask(
      StatementExecutionAPI statements, DatabricksFilter filter, TaskBody body) throws Exception {
    File output = tempFolder.newFile();
    try (DatabricksHandle handle = newHandle(statements, filter)) {
      body.run(Files.asByteSink(output), handle);
    }
    String csv = Files.asCharSource(output, UTF_8).read();
    try (CSVParser parser =
        AbstractTask.FORMAT
            .builder()
            .setHeader()
            .setSkipHeaderRecord(true)
            .get()
            .parse(new StringReader(csv))) {
      return parser.getRecords();
    }
  }

  private static DatabricksHandle newHandle(
      StatementExecutionAPI statements, DatabricksFilter filter) {
    WorkspaceClient client = mock(WorkspaceClient.class);
    when(client.statementExecution()).thenReturn(statements);
    DatabricksSqlHelper sqlHelper =
        new DatabricksSqlHelper(client, "wh-1", new DatabricksChunkDownloader(), 0L);
    return new DatabricksHandle(sqlHelper, filter, MAX_CONCURRENT_STATEMENTS);
  }

  /** Mocks the Statement Execution API to answer each SQL statement from the map. */
  private static StatementExecutionAPI mockStatements(
      Map<String, StatementResponse> responsesBySql) {
    StatementExecutionAPI statements = mock(StatementExecutionAPI.class);
    when(statements.executeStatement(any(ExecuteStatementRequest.class)))
        .thenAnswer(
            invocation -> {
              ExecuteStatementRequest request = invocation.getArgument(0);
              StatementResponse response = responsesBySql.get(request.getStatement());
              if (response == null) {
                throw new IllegalArgumentException("Unexpected SQL: " + request.getStatement());
              }
              return response;
            });
    return statements;
  }

  private static StatementResponse showTableResponse(String tableName, String information) {
    return succeededRows(
        ImmutableList.of(ImmutableList.of(SCHEMA, tableName, "false", information)));
  }

  private static StatementResponse showTablesResponse(String... tableNames) {
    List<List<String>> tableRows = new ArrayList<>();
    for (String tableName : tableNames) {
      tableRows.add(ImmutableList.of(SCHEMA, tableName, "false", tableInformation(tableName)));
    }
    return succeededRows(tableRows);
  }

  /** Returns the {@code SHOW TABLE EXTENDED} information of a Delta table with an int column. */
  private static String tableInformation(String tableName) {
    return "Catalog: hive_metastore\n"
        + "Database: legacy\n"
        + "Table: "
        + tableName
        + "\n"
        + "Type: MANAGED\n"
        + "Provider: delta\n"
        + "Schema: root\n"
        + " |-- id: integer (nullable = false)\n";
  }

  /** Returns the {@code SHOW TABLE EXTENDED} information of a view with an int column. */
  private static String viewInformation(String viewText) {
    return "Catalog: hive_metastore\n"
        + "Database: legacy\n"
        + "Table: event_ids\n"
        + "Type: VIEW\n"
        + "View Text: "
        + viewText
        + "\n"
        + "View Original Text: "
        + viewText
        + "\n"
        + "Schema: root\n"
        + " |-- id: integer (nullable = true)\n";
  }

  private static StatementResponse succeeded(String... singleColumnRows) {
    List<List<String>> tableRows = new ArrayList<>();
    for (String value : singleColumnRows) {
      tableRows.add(ImmutableList.of(value));
    }
    return succeededRows(tableRows);
  }

  private static StatementResponse succeededRows(List<? extends List<String>> tableRows) {
    return new StatementResponse()
        .setStatementId("stmt-ok")
        .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
        .setResult(new ResultData().setDataArray(rows(tableRows)));
  }

  private static Collection<Collection<String>> rows(List<? extends List<String>> tableRows) {
    return new ArrayList<Collection<String>>(tableRows);
  }

  private static StatementResponse failed(String message) {
    return new StatementResponse()
        .setStatementId("stmt-failed")
        .setStatus(
            new StatementStatus()
                .setState(StatementState.FAILED)
                .setError(new ServiceError().setMessage(message)));
  }
}
