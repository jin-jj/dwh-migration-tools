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
import static org.junit.Assert.assertThrows;
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
import java.sql.SQLException;
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
  public void executeQueryOrThrow_succeedsAndReturnsRows() throws Exception {
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
    List<List<String>> rows = DatabricksSqlHelper.executeQueryOrThrow(handle, "SELECT 1");

    assertEquals(2, rows.size());
    assertEquals(Arrays.asList("cat1", "schema1"), rows.get(0));
    assertEquals(Arrays.asList("cat1", "schema2"), rows.get(1));
  }

  @Test
  public void executeQueryOrThrow_handlesPagination() throws Exception {
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
    List<List<String>> rows = DatabricksSqlHelper.executeQueryOrThrow(handle, "SELECT 1");

    assertEquals(2, rows.size());
    assertEquals(Collections.singletonList("row1"), rows.get(0));
    assertEquals(Collections.singletonList("row2"), rows.get(1));
  }

  /**
   * The exact refusal a live workspace returned for {@code SELECT … FROM
   * mingjial.information_schema.tables}.
   *
   * <p>Note the trailing {@code . SQLSTATE: 42501}, which the earlier fixture for this test did not
   * have: the name parser reads to the next quote, so a suffix after the closing quote must not
   * disturb it.
   */
  private static final String REAL_REFUSAL =
      "[INSUFFICIENT_PERMISSIONS] Insufficient privileges:\n"
          + "User does not have USE CATALOG on Catalog 'mingjial'. SQLSTATE: 42501";

  private static DatabricksHandle handleFailingWith(String message) {
    WorkspaceClient client = mock(WorkspaceClient.class);
    StatementExecutionAPI statementAPI = mock(StatementExecutionAPI.class);
    when(client.statementExecution()).thenReturn(statementAPI);

    StatementResponse response = new StatementResponse();
    response.setStatementId("stmt-fail");
    response.setStatus(
        new StatementStatus()
            .setState(StatementState.FAILED)
            .setError(new ServiceError().setMessage(message)));
    when(statementAPI.executeStatement(any(ExecuteStatementRequest.class))).thenReturn(response);

    return new DatabricksHandle(client, "wh-1");
  }

  @Test
  public void executeQueryOrThrow_whenFailed_throwsSQLExceptionAndMarksCatalogInaccessible() {
    DatabricksHandle handle = handleFailingWith(REAL_REFUSAL);

    assertThrows(
        SQLException.class,
        () ->
            DatabricksSqlHelper.executeQueryOrThrow(
                handle, "SELECT 1 FROM mingjial.information_schema.tables"));
    assertTrue(handle.isCatalogInaccessible("mingjial"));
  }

  @Test
  public void isInsufficientPrivilege_recognizesTheRealRefusal() {
    assertTrue(DatabricksSqlHelper.isInsufficientPrivilege(new SQLException(REAL_REFUSAL)));
  }

  /**
   * The name parser reads English prose, which Databricks may reword at any time. The SQLSTATE is
   * standardised, so a refusal stays recognisable even when the sentence around it changes — which
   * is what lets the per-catalog loop mark the catalog it already knows it was reading.
   */
  @Test
  public void isInsufficientPrivilege_recognizesArewordedRefusalBySqlstate() {
    assertTrue(
        DatabricksSqlHelper.isInsufficientPrivilege(
            new SQLException("Permission denied on catalog mingjial. SQLSTATE: 42501")));
  }

  @Test
  public void isInsufficientPrivilege_ignoresUnrelatedFailures() {
    assertFalse(
        DatabricksSqlHelper.isInsufficientPrivilege(
            new SQLException("[TABLE_OR_VIEW_NOT_FOUND] … SQLSTATE: 42P01")));
    assertFalse(DatabricksSqlHelper.isInsufficientPrivilege(null));
  }
}
