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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.StatementExecutionAPI;
import com.databricks.sdk.service.sql.StatementResponse;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksSqlHelperTest {

  @Test
  public void escapeIdentifier_escapesCorrectly() {
    assertEquals("`simple`", DatabricksSqlHelper.escapeIdentifier("simple"));
    assertEquals("`has``quote`", DatabricksSqlHelper.escapeIdentifier("has`quote"));
    assertEquals("`catalog-name`", DatabricksSqlHelper.escapeIdentifier("catalog-name"));
  }

  @Test
  public void executeQuery_succeedsAndReturnsRows() {
    WorkspaceClient client = mock(WorkspaceClient.class);
    StatementExecutionAPI statementAPI = mock(StatementExecutionAPI.class);
    when(client.statementExecution()).thenReturn(statementAPI);

    StatementResponse response = new StatementResponse();
    response.setStatementId("stmt-1");
    StatementStatus status = new StatementStatus().setState(StatementState.SUCCEEDED);
    response.setStatus(status);
    ResultData resultData = new ResultData();
    List<Collection<String>> rowsList = new ArrayList<>();
    rowsList.add(Arrays.asList("cat1", "schema1"));
    rowsList.add(Arrays.asList("cat1", "schema2"));
    resultData.setDataArray(rowsList);
    response.setResult(resultData);

    when(statementAPI.executeStatement(any(ExecuteStatementRequest.class))).thenReturn(response);

    DatabricksHandle handle = new DatabricksHandle(client, "wh-1");
    List<List<String>> rows = DatabricksSqlHelper.executeQuery(handle, "SELECT 1");

    assertEquals(2, rows.size());
    assertEquals(Arrays.asList("cat1", "schema1"), rows.get(0));
    assertEquals(Arrays.asList("cat1", "schema2"), rows.get(1));
  }

  @Test
  public void executeQuery_handlesPagination() {
    WorkspaceClient client = mock(WorkspaceClient.class);
    StatementExecutionAPI statementAPI = mock(StatementExecutionAPI.class);
    when(client.statementExecution()).thenReturn(statementAPI);

    StatementResponse response = new StatementResponse();
    response.setStatementId("stmt-1");
    response.setStatus(new StatementStatus().setState(StatementState.SUCCEEDED));
    ResultData chunk0 = new ResultData();
    List<Collection<String>> chunk0Rows = new ArrayList<>();
    chunk0Rows.add(Collections.singletonList("row1"));
    chunk0.setDataArray(chunk0Rows);
    chunk0.setNextChunkIndex(1L);
    response.setResult(chunk0);

    ResultData chunk1 = new ResultData();
    List<Collection<String>> chunk1Rows = new ArrayList<>();
    chunk1Rows.add(Collections.singletonList("row2"));
    chunk1.setDataArray(chunk1Rows);
    chunk1.setNextChunkIndex(null);

    when(statementAPI.executeStatement(any(ExecuteStatementRequest.class))).thenReturn(response);
    when(statementAPI.getStatementResultChunkN("stmt-1", 1L)).thenReturn(chunk1);

    DatabricksHandle handle = new DatabricksHandle(client, "wh-1");
    List<List<String>> rows = DatabricksSqlHelper.executeQuery(handle, "SELECT 1");

    assertEquals(2, rows.size());
    assertEquals(Collections.singletonList("row1"), rows.get(0));
    assertEquals(Collections.singletonList("row2"), rows.get(1));
  }
}
