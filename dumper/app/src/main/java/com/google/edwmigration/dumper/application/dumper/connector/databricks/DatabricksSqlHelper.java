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

import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.StatementResponse;
import com.databricks.sdk.service.sql.StatementState;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper for executing SQL statements on Databricks SQL Warehouses. */
final class DatabricksSqlHelper {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksSqlHelper.class);

  private static final long MAX_WAIT_MILLIS = 120_000L;
  private static final long POLL_INTERVAL_MILLIS = 1_000L;

  private DatabricksSqlHelper() {}

  /** Escapes a Databricks SQL identifier using backticks, escaping existing backticks. */
  @Nonnull
  public static String escapeIdentifier(@Nonnull String identifier) {
    return "`" + identifier.replace("`", "``") + "`";
  }

  @Nonnull
  public static List<List<String>> executeQuery(
      @Nonnull DatabricksHandle handle, @Nonnull String sql) {
    List<List<String>> rows = new ArrayList<>();
    executeQuery(handle, sql, rows::add);
    return rows;
  }

  public static void executeQuery(
      @Nonnull DatabricksHandle handle,
      @Nonnull String sql,
      @Nonnull Consumer<List<String>> rowConsumer) {
    if (!handle.hasWarehouseId()) {
      return;
    }
    String warehouseId = handle.getWarehouseId();
    ExecuteStatementRequest request =
        new ExecuteStatementRequest()
            .setWarehouseId(warehouseId)
            .setStatement(sql)
            .setWaitTimeout("30s");
    StatementResponse response = handle.getClient().statementExecution().executeStatement(request);
    String statementId = response.getStatementId();

    long startTime = System.currentTimeMillis();
    StatementState state = response.getStatus() != null ? response.getStatus().getState() : null;
    while ((state == StatementState.PENDING || state == StatementState.RUNNING)
        && (System.currentTimeMillis() - startTime < MAX_WAIT_MILLIS)) {
      try {
        Thread.sleep(POLL_INTERVAL_MILLIS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        cancelStatementQuietly(handle, statementId);
        throw new RuntimeException("Interrupted while waiting for SQL statement execution", e);
      }
      response = handle.getClient().statementExecution().getStatement(statementId);
      state = response.getStatus() != null ? response.getStatus().getState() : null;
    }

    if (state == StatementState.PENDING || state == StatementState.RUNNING) {
      cancelStatementQuietly(handle, statementId);
      logger.warn("Databricks SQL query timed out after {} ms. Query: {}", MAX_WAIT_MILLIS, sql);
      return;
    }

    if (state != StatementState.SUCCEEDED && state != StatementState.CLOSED) {
      String errMsg =
          response.getStatus() != null && response.getStatus().getError() != null
              ? response.getStatus().getError().getMessage()
              : "Unknown error";
      logger.warn("Databricks SQL query failed with state {}: {}. Query: {}", state, errMsg, sql);
      return;
    }

    if (response.getResult() != null && response.getResult().getDataArray() != null) {
      for (Collection<String> row : response.getResult().getDataArray()) {
        rowConsumer.accept(new ArrayList<>(row));
      }
      Long nextChunk = response.getResult().getNextChunkIndex();
      while (nextChunk != null) {
        ResultData chunk =
            handle
                .getClient()
                .statementExecution()
                .getStatementResultChunkN(statementId, nextChunk);
        if (chunk != null && chunk.getDataArray() != null) {
          for (Collection<String> row : chunk.getDataArray()) {
            rowConsumer.accept(new ArrayList<>(row));
          }
          nextChunk = chunk.getNextChunkIndex();
        } else {
          break;
        }
      }
    }
  }

  private static void cancelStatementQuietly(DatabricksHandle handle, String statementId) {
    try {
      handle.getClient().statementExecution().cancelExecution(statementId);
    } catch (Exception e) {
      logger.debug("Failed to cancel statement '{}': {}", statementId, e.getMessage());
    }
  }
}
