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
import java.sql.SQLException;
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

  private static final long MAX_WAIT_MILLIS = 600_000L;
  private static final long POLL_INTERVAL_MILLIS = 1_000L;
  private static final int MAX_RETRIES = 5;

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
    try {
      executeQueryOrThrow(handle, sql, rowConsumer);
    } catch (Exception e) {
      logger.warn("Databricks SQL query execution failed: {}. Query: {}", e.getMessage(), sql);
    }
  }

  @Nonnull
  public static List<List<String>> executeQueryOrThrow(
      @Nonnull DatabricksHandle handle, @Nonnull String sql) throws SQLException {
    List<List<String>> rows = new ArrayList<>();
    executeQueryOrThrow(handle, sql, rows::add);
    return rows;
  }

  public static void executeQueryOrThrow(
      @Nonnull DatabricksHandle handle,
      @Nonnull String sql,
      @Nonnull Consumer<List<String>> rowConsumer)
      throws SQLException {
    if (!handle.hasWarehouseId()) {
      return;
    }
    String warehouseId = handle.getWarehouseId();
    ExecuteStatementRequest request =
        new ExecuteStatementRequest()
            .setWarehouseId(warehouseId)
            .setStatement(sql)
            .setWaitTimeout("30s");

    StatementResponse response = null;
    long backoffMs = 1000L;
    for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        response = handle.getClient().statementExecution().executeStatement(request);
        break;
      } catch (Exception e) {
        if (isRateLimited(e) && attempt < MAX_RETRIES) {
          logger.warn(
              "Rate limited on SQL statement execution. Retrying in {}ms (attempt {}/{})",
              backoffMs,
              attempt,
              MAX_RETRIES);
          try {
            Thread.sleep(backoffMs);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during rate limit backoff", ie);
          }
          backoffMs *= 2;
          continue;
        }
        throw new SQLException("Failed to execute SQL statement: " + sql, e);
      }
    }

    if (response == null) {
      throw new SQLException("Received null response for SQL statement: " + sql);
    }

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
      try {
        response = handle.getClient().statementExecution().getStatement(statementId);
        state = response.getStatus() != null ? response.getStatus().getState() : null;
      } catch (Exception e) {
        throw new SQLException("Failed to get statement status for '" + statementId + "'", e);
      }
    }

    if (state == StatementState.PENDING || state == StatementState.RUNNING) {
      cancelStatementQuietly(handle, statementId);
      throw new SQLException(
          "Databricks SQL query timed out after " + MAX_WAIT_MILLIS + " ms. Query: " + sql);
    }

    if (state != StatementState.SUCCEEDED && state != StatementState.CLOSED) {
      String errMsg =
          response.getStatus() != null && response.getStatus().getError() != null
              ? response.getStatus().getError().getMessage()
              : "Unknown error";
      if (errMsg.contains("USE CATALOG on Catalog '")) {
        int start =
            errMsg.indexOf("USE CATALOG on Catalog '") + "USE CATALOG on Catalog '".length();
        int end = errMsg.indexOf("'", start);
        if (end > start) {
          String cat = errMsg.substring(start, end);
          handle.markCatalogInaccessible(cat);
          logger.info("Marked catalog '{}' as inaccessible due to insufficient privileges.", cat);
        }
      }
      throw new SQLException("Databricks SQL query failed with state " + state + ": " + errMsg);
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

  private static boolean isRateLimited(Throwable t) {
    while (t != null) {
      String msg = t.getMessage();
      if (msg != null
          && (msg.contains("429")
              || msg.toLowerCase().contains("rate limit")
              || msg.toLowerCase().contains("too many requests")
              || msg.contains("Current request has to be retried"))) {
        return true;
      }
      t = t.getCause();
    }
    return false;
  }

  private static void cancelStatementQuietly(DatabricksHandle handle, String statementId) {
    try {
      handle.getClient().statementExecution().cancelExecution(statementId);
    } catch (Exception e) {
      logger.debug("Failed to cancel statement '{}': {}", statementId, e.getMessage());
    }
  }
}
