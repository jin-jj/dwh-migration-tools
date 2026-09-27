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

import static org.junit.Assert.assertFalse;
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
  public void hiveMetastoreDdlsTask_skipsTableWhoseShowCreateTableFails() throws Exception {
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
                                "[UNSUPPORTED_SHOW_CREATE_TABLE.WITH_UNSUPPORTED_SERDE_CONFIGURATION]"
                                    + " Unsupported SHOW CREATE TABLE command.")));

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
      assertFalse(ddlsCsv.contains("hive_serde_tbl"));
      assertTrue(
          ddlsCsv.contains(
              "hive_metastore,legacy,double_it,FUNCTION,\"CREATE FUNCTION hive_metastore.legacy.double_it(x INT)\n"
                  + "RETURNS INT\n"
                  + "RETURN x * 2\""));
    }
  }
}
