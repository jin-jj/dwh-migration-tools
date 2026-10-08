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
import com.google.common.io.ByteSink;
import com.google.common.io.Files;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import java.io.File;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/**
 * Stubs of the Databricks Statement Execution API that answer SQL statements with canned responses,
 * and a runner of tasks that write CSV files.
 */
final class DatabricksStatementStubs {

  private static final int MAX_CONCURRENT_STATEMENTS = 4;

  private DatabricksStatementStubs() {}

  /** The body of a task, such as {@code doRun} with the sink and the handle. */
  interface TaskBody {

    void run(ByteSink sink, DatabricksHandle handle) throws Exception;
  }

  /**
   * Runs a task that writes a CSV file with a header to {@code output}, and returns its records.
   */
  static List<CSVRecord> runTask(
      File output, StatementExecutionAPI statements, DatabricksFilter filter, TaskBody body)
      throws Exception {
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

  static DatabricksFilter acceptAllFilter() {
    return new DatabricksFilter(ImmutableList.of(), ImmutableList.of());
  }

  static DatabricksHandle newHandle(StatementExecutionAPI statements, DatabricksFilter filter) {
    WorkspaceClient client = mock(WorkspaceClient.class);
    when(client.statementExecution()).thenReturn(statements);
    DatabricksSqlHelper sqlHelper =
        new DatabricksSqlHelper(client, "wh-1", new DatabricksChunkDownloader(), 0L);
    return new DatabricksHandle(sqlHelper, filter, MAX_CONCURRENT_STATEMENTS);
  }

  /** Mocks the Statement Execution API to answer each SQL statement from the map. */
  static StatementExecutionAPI mockStatements(Map<String, StatementResponse> responsesBySql) {
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

  static StatementResponse succeeded(String... singleColumnRows) {
    List<List<String>> tableRows = new ArrayList<>();
    for (String value : singleColumnRows) {
      tableRows.add(ImmutableList.of(value));
    }
    return succeededRows(tableRows);
  }

  static StatementResponse succeededRows(List<? extends List<String>> tableRows) {
    return new StatementResponse()
        .setStatementId("stmt-ok")
        .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
        .setResult(new ResultData().setDataArray(new ArrayList<Collection<String>>(tableRows)));
  }

  static StatementResponse failed(String message) {
    return new StatementResponse()
        .setStatementId("stmt-failed")
        .setStatus(
            new StatementStatus()
                .setState(StatementState.FAILED)
                .setError(new ServiceError().setMessage(message)));
  }
}
