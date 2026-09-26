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
import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ServiceError;
import com.databricks.sdk.service.sql.StatementExecutionAPI;
import com.databricks.sdk.service.sql.StatementResponse;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreParser.ParsedFunctionExtended;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreParser.ParsedTableExtended;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreDdlsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.HiveMetastoreSessionCache;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksHiveMetastoreTasksTest {

  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void hiveMetastoreParser_parsesShowTableExtendedAndDescribeFunctionExtended() {
    String blob =
        "Catalog: hive_metastore\n"
            + "Database: legacy_db\n"
            + "Table: events\n"
            + "Owner: data_eng\n"
            + "Created Time: Wed Sep 18 12:00:00 UTC 2024\n"
            + "Type: EXTERNAL\n"
            + "Provider: parquet\n"
            + "Comment: Legacy events\n"
            + "Table Properties: [delta.checkpoint.writeStatsAsJson=false, "
            + "delta.constraints.valid_event_type=event_type IN ('click', 'view', 'purchase'), "
            + "delta.enableDeletionVectors=true]\n"
            + "Location: s3://legacy-bucket/events\n"
            + "Partition Columns: [`dt`, `country`]\n"
            + "Schema: root\n"
            + " |-- event_id: string (nullable = false)\n"
            + " |-- payload: string (nullable = true)\n"
            + " |-- dt: date (nullable = true)\n"
            + " |-- country: string (nullable = true)\n";

    ParsedTableExtended parsed =
        DatabricksHiveMetastoreParser.parseShowTableExtended("legacy_db", "events", blob);
    assertEquals("hive_metastore", parsed.catalogName);
    assertEquals("legacy_db", parsed.schemaName);
    assertEquals("events", parsed.tableName);
    assertEquals("EXTERNAL", parsed.tableType);
    assertEquals("PARQUET", parsed.dataSourceFormat);
    assertEquals("s3://legacy-bucket/events", parsed.storageLocation);
    assertEquals(4, parsed.columns.size());
    assertEquals("NO", parsed.columns.get(0).isNullable);
    assertEquals("", parsed.columns.get(0).partitionIndex);
    assertEquals("0", parsed.columns.get(2).partitionIndex);
    assertEquals("1", parsed.columns.get(3).partitionIndex);
    assertEquals(1, parsed.constraints.size());
    assertEquals("valid_event_type", parsed.constraints.get(0).constraintName);
    assertEquals("CHECK", parsed.constraints.get(0).constraintType);
    assertEquals(
        "event_type IN ('click', 'view', 'purchase')", parsed.constraints.get(0).constraintDetails);

    ParsedFunctionExtended sqlFn =
        DatabricksHiveMetastoreParser.parseDescribeFunctionExtended(
            "hive_metastore",
            "legacy_db",
            "double_it",
            Arrays.asList(
                "Function: hive_metastore.legacy_db.double_it",
                "Input: x INT",
                "Returns: INT",
                "Comment: Doubles input",
                "Owner: alice",
                "Body: x * 2"));
    assertEquals("INT", sqlFn.dataType);
    assertEquals("x INT", sqlFn.inputParams);
    assertEquals("x * 2", sqlFn.routineDefinition);
    assertEquals("SQL", sqlFn.routineLanguage);
    assertEquals(
        "CREATE FUNCTION hive_metastore.legacy_db.double_it(x INT)\n"
            + "RETURNS INT\n"
            + "COMMENT 'Doubles input'\n"
            + "RETURN x * 2",
        sqlFn.ddl);

    ParsedFunctionExtended javaFn =
        DatabricksHiveMetastoreParser.parseDescribeFunctionExtended(
            "hive_metastore",
            "legacy_db",
            "custom_hash",
            Arrays.asList(
                "Function: default.custom_hash",
                "Class: com.acme.udf.CustomHashUdf",
                "Owner: bob"));
    assertEquals("com.acme.udf.CustomHashUdf", javaFn.routineDefinition);
    assertEquals("JAVA", javaFn.routineLanguage);
    assertEquals(
        "CREATE FUNCTION hive_metastore.legacy_db.custom_hash AS 'com.acme.udf.CustomHashUdf'",
        javaFn.ddl);
  }

  @Test
  public void hiveMetastoreDdlsTask_dumpsShowCreateTableWithSerdeFallback() throws Exception {
    WorkspaceClient client = mock(WorkspaceClient.class);
    StatementExecutionAPI stmtApi = mock(StatementExecutionAPI.class);
    when(client.statementExecution()).thenReturn(stmtApi);

    StatementResponse showDatabasesResp =
        new StatementResponse()
            .setStatementId("stmt-dbs")
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
            .setResult(
                new ResultData()
                    .setDataArray(Collections.singletonList(Collections.singletonList("legacy"))));

    String deltaBlob =
        "Catalog: hive_metastore\n"
            + "Database: legacy\n"
            + "Table: delta_tbl\n"
            + "Type: MANAGED\n"
            + "Provider: delta\n"
            + "Schema: root\n"
            + " |-- id: integer (nullable = false)\n";
    String serdeBlob =
        "Catalog: hive_metastore\n"
            + "Database: legacy\n"
            + "Table: hive_serde_tbl\n"
            + "Type: EXTERNAL\n"
            + "Provider: hive\n"
            + "Schema: root\n"
            + " |-- raw_line: string (nullable = true)\n";
    StatementResponse showTableExtendedResp =
        new StatementResponse()
            .setStatementId("stmt-ext")
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
            .setResult(
                new ResultData()
                    .setDataArray(
                        Arrays.asList(
                            Arrays.asList("legacy", "delta_tbl", "false", deltaBlob),
                            Arrays.asList("legacy", "hive_serde_tbl", "false", serdeBlob))));

    StatementResponse deltaDdlResp =
        new StatementResponse()
            .setStatementId("stmt-ddl-1")
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
            .setResult(
                new ResultData()
                    .setDataArray(
                        Collections.singletonList(
                            Collections.singletonList(
                                "CREATE TABLE hive_metastore.legacy.delta_tbl (id INT NOT NULL) USING delta"))));

    StatementResponse serdeFailResp =
        new StatementResponse()
            .setStatementId("stmt-ddl-fail")
            .setStatus(
                new StatementStatus()
                    .setState(StatementState.FAILED)
                    .setError(
                        new ServiceError()
                            .setMessage(
                                "[SHOW_CREATE_TABLE_ON_HIVE_SERDE] Please execute SHOW CREATE TABLE ... AS SERDE")));

    StatementResponse serdeDdlResp =
        new StatementResponse()
            .setStatementId("stmt-ddl-2")
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
            .setResult(
                new ResultData()
                    .setDataArray(
                        Collections.singletonList(
                            Collections.singletonList(
                                "CREATE EXTERNAL TABLE hive_metastore.legacy.hive_serde_tbl (raw_line STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'"))));

    StatementResponse showUserFunctionsResp =
        new StatementResponse()
            .setStatementId("stmt-fns")
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
            .setResult(
                new ResultData()
                    .setDataArray(
                        Collections.singletonList(Collections.singletonList("legacy.double_it"))));

    StatementResponse descFunctionResp =
        new StatementResponse()
            .setStatementId("stmt-desc-fn")
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
            .setResult(
                new ResultData()
                    .setDataArray(
                        Arrays.asList(
                            Collections.singletonList("Function: hive_metastore.legacy.double_it"),
                            Collections.singletonList("Type: SCALAR"),
                            Collections.singletonList("Input: x INT"),
                            Collections.singletonList("Returns: INT"),
                            Collections.singletonList("Body: x * 2"))));

    when(stmtApi.executeStatement(any(ExecuteStatementRequest.class)))
        .thenAnswer(
            inv -> {
              ExecuteStatementRequest req = inv.getArgument(0);
              String sql = req.getStatement();
              if ("SHOW SCHEMAS".equals(sql)) {
                return showDatabasesResp;
              }
              if ("SHOW TABLE EXTENDED IN `legacy` LIKE '*'".equals(sql)) {
                return showTableExtendedResp;
              }
              if ("SHOW CREATE TABLE `legacy`.`delta_tbl`".equals(sql)) {
                return deltaDdlResp;
              }
              if ("SHOW CREATE TABLE `legacy`.`hive_serde_tbl`".equals(sql)) {
                return serdeFailResp;
              }
              if ("SHOW CREATE TABLE `legacy`.`hive_serde_tbl` AS SERDE".equals(sql)) {
                return serdeDdlResp;
              }
              if ("SHOW USER FUNCTIONS IN `legacy`".equals(sql)) {
                return showUserFunctionsResp;
              }
              if ("DESCRIBE FUNCTION EXTENDED `legacy`.`double_it`".equals(sql)) {
                return descFunctionResp;
              }
              throw new IllegalArgumentException("Unexpected SQL: " + sql);
            });

    DatabricksSqlHelper sqlHelper =
        new DatabricksSqlHelper(client, "wh-1", new DatabricksChunkDownloader(), 0L);
    try (DatabricksHandle handle =
        new DatabricksHandle(sqlHelper, new DatabricksFilter(ImmutableList.of()))) {
      HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();
      File ddlsFile = tempFolder.newFile("ddls.csv");
      new DatabricksHiveMetastoreDdlsTask(cache).doRun(null, Files.asByteSink(ddlsFile), handle);

      String ddlsCsv = Files.asCharSource(ddlsFile, StandardCharsets.UTF_8).read();
      assertTrue(
          ddlsCsv.contains(
              "hive_metastore,legacy,delta_tbl,MANAGED,CREATE TABLE hive_metastore.legacy.delta_tbl (id INT NOT NULL) USING delta"));
      assertTrue(
          ddlsCsv.contains(
              "hive_metastore,legacy,hive_serde_tbl,EXTERNAL,CREATE EXTERNAL TABLE hive_metastore.legacy.hive_serde_tbl (raw_line STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'"));
      assertTrue(
          ddlsCsv.contains(
              "hive_metastore,legacy,double_it,FUNCTION,\"CREATE FUNCTION hive_metastore.legacy.double_it(x INT)\n"
                  + "RETURNS INT\n"
                  + "RETURN x * 2\""));
    }
  }
}
