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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.StatementExecutionAPI;
import com.databricks.sdk.service.sql.StatementResponse;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSink;
import com.google.common.io.Files;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDumpFormat.SchemataHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDumpFormat.TablesHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreSchemataTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreTablesTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.HiveMetastoreSessionCache;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import java.io.File;
import java.io.StringReader;
import java.util.ArrayList;
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
                succeededRows(
                    ImmutableList.of(
                        ImmutableList.of(SCHEMA, "csv_events", "false", information)))));
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

  private interface TaskBody {

    void run(ByteSink sink, DatabricksHandle handle) throws Exception;
  }

  private List<CSVRecord> runTask(StatementExecutionAPI statements, TaskBody body)
      throws Exception {
    File output = tempFolder.newFile();
    try (DatabricksHandle handle = newHandle(statements)) {
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

  private static DatabricksHandle newHandle(StatementExecutionAPI statements) {
    WorkspaceClient client = mock(WorkspaceClient.class);
    when(client.statementExecution()).thenReturn(statements);
    DatabricksSqlHelper sqlHelper =
        new DatabricksSqlHelper(client, "wh-1", new DatabricksChunkDownloader(), 0L);
    return new DatabricksHandle(sqlHelper, new DatabricksFilter(ImmutableList.of()));
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
}
