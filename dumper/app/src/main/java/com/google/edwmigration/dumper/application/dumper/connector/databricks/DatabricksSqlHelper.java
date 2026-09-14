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

import com.databricks.sdk.service.sql.Disposition;
import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.ExternalLink;
import com.databricks.sdk.service.sql.Format;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.StatementResponse;
import com.databricks.sdk.service.sql.StatementState;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper for executing SQL statements on Databricks SQL Warehouses.
 *
 * <p>Two result transports are supported. {@code INLINE} returns rows embedded in the JSON response
 * and is limited by Databricks to 25 MiB, beyond which the statement is aborted; it is appropriate
 * for small results such as {@code SHOW} commands. {@code EXTERNAL_LINKS} streams arbitrarily large
 * results as CSV through pre-signed URLs and must be used for bulk queries such as metastore-wide
 * {@code information_schema} scans.
 *
 * <p>All methods propagate failures. Callers are tasks, which are the failure boundary, and a
 * swallowed failure would silently produce an empty output file and defeat connector fallback.
 */
final class DatabricksSqlHelper {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksSqlHelper.class);

  private static final long MAX_WAIT_MILLIS = 600_000L;
  private static final long POLL_INTERVAL_MILLIS = 1_000L;
  private static final int MAX_RETRIES = 5;
  private static final int HTTP_CONNECT_TIMEOUT_MILLIS = 30_000;
  private static final int HTTP_READ_TIMEOUT_MILLIS = 300_000;
  private static final String WAIT_TIMEOUT = "30s";
  private static final String PERMISSION_DENIED_MARKER = "USE CATALOG on Catalog '";

  private DatabricksSqlHelper() {}

  /** Escapes a Databricks SQL identifier using backticks, escaping existing backticks. */
  @Nonnull
  static String escapeIdentifier(@Nonnull String identifier) {
    return "`" + identifier.replace("`", "``") + "`";
  }

  /**
   * Executes a statement expected to return a small result, buffering all rows in memory.
   *
   * <p>Only use for bounded results such as {@code SHOW CATALOGS}. Use {@link
   * #executeBulkQueryOrThrow} for anything that scales with the size of the metastore.
   */
  @Nonnull
  static List<List<String>> executeQueryOrThrow(
      @Nonnull DatabricksHandle handle, @Nonnull String sql) throws SQLException {
    List<List<String>> rows = new ArrayList<>();
    executeQueryOrThrow(handle, sql, rows::add);
    return ImmutableList.copyOf(rows);
  }

  /** Executes a statement with the inline transport, streaming rows to {@code rowConsumer}. */
  static void executeQueryOrThrow(
      @Nonnull DatabricksHandle handle,
      @Nonnull String sql,
      @Nonnull Consumer<List<String>> rowConsumer)
      throws SQLException {
    execute(handle, sql, Disposition.INLINE, Format.JSON_ARRAY, rowConsumer);
  }

  /**
   * Executes a statement with the external-links transport, streaming rows to {@code rowConsumer}.
   *
   * <p>Results are transferred as CSV through pre-signed URLs, which lifts the 25 MiB inline result
   * cap to 100 GiB and keeps memory bounded.
   */
  static void executeBulkQueryOrThrow(
      @Nonnull DatabricksHandle handle,
      @Nonnull String sql,
      @Nonnull Consumer<List<String>> rowConsumer)
      throws SQLException {
    execute(handle, sql, Disposition.EXTERNAL_LINKS, Format.CSV, rowConsumer);
  }

  private static void execute(
      @Nonnull DatabricksHandle handle,
      @Nonnull String sql,
      @Nonnull Disposition disposition,
      @Nonnull Format format,
      @Nonnull Consumer<List<String>> rowConsumer)
      throws SQLException {
    if (!handle.hasWarehouseId()) {
      throw new SQLException(
          "A SQL warehouse is required to execute Databricks statements. Pass --warehouse.");
    }
    ExecuteStatementRequest request =
        new ExecuteStatementRequest()
            .setWarehouseId(handle.getWarehouseId())
            .setStatement(sql)
            .setDisposition(disposition)
            .setFormat(format)
            .setWaitTimeout(WAIT_TIMEOUT);

    StatementResponse response = submit(handle, request, sql);
    String statementId = response.getStatementId();
    response = awaitTerminalState(handle, response, statementId, sql);
    checkSucceeded(handle, response, sql);

    ResultData result = response.getResult();
    while (result != null) {
      readResultData(result, rowConsumer);
      Long nextChunk = nextChunkIndex(result);
      if (nextChunk == null) {
        return;
      }
      result = fetchChunk(handle, statementId, nextChunk);
    }
  }

  private static StatementResponse submit(
      DatabricksHandle handle, ExecuteStatementRequest request, String sql) throws SQLException {
    long backoffMs = 1000L;
    for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        return handle.getClient().statementExecution().executeStatement(request);
      } catch (Exception e) {
        if (isRateLimited(e) && attempt < MAX_RETRIES) {
          logger.warn(
              "Rate limited on SQL statement execution. Retrying in {}ms (attempt {}/{})",
              backoffMs,
              attempt,
              MAX_RETRIES);
          sleep(backoffMs, "rate limit backoff");
          backoffMs *= 2;
          continue;
        }
        throw new SQLException("Failed to execute SQL statement: " + sql, e);
      }
    }
    throw new SQLException("Exhausted retries executing SQL statement: " + sql);
  }

  private static StatementResponse awaitTerminalState(
      DatabricksHandle handle, StatementResponse response, String statementId, String sql)
      throws SQLException {
    long startTime = System.currentTimeMillis();
    StatementState state = stateOf(response);
    while ((state == StatementState.PENDING || state == StatementState.RUNNING)
        && (System.currentTimeMillis() - startTime < MAX_WAIT_MILLIS)) {
      sleepOrCancel(handle, statementId);
      try {
        response = handle.getClient().statementExecution().getStatement(statementId);
      } catch (Exception e) {
        throw new SQLException("Failed to get statement status for '" + statementId + "'", e);
      }
      state = stateOf(response);
    }

    if (state == StatementState.PENDING || state == StatementState.RUNNING) {
      cancelQuietly(handle, statementId);
      throw new SQLException(
          "Databricks SQL query timed out after " + MAX_WAIT_MILLIS + " ms. Query: " + sql);
    }
    return response;
  }

  private static void checkSucceeded(
      DatabricksHandle handle, StatementResponse response, String sql) throws SQLException {
    StatementState state = stateOf(response);
    if (state == StatementState.SUCCEEDED || state == StatementState.CLOSED) {
      return;
    }
    String errMsg =
        response.getStatus() != null && response.getStatus().getError() != null
            ? response.getStatus().getError().getMessage()
            : "Unknown error";
    markInaccessibleCatalog(handle, errMsg);
    throw new SQLException("Databricks SQL query failed with state " + state + ": " + errMsg);
  }

  /**
   * Records a catalog the current principal may not read, so later tasks can skip it instead of
   * repeatedly failing on it.
   */
  private static void markInaccessibleCatalog(DatabricksHandle handle, String errMsg) {
    if (errMsg == null || !errMsg.contains(PERMISSION_DENIED_MARKER)) {
      return;
    }
    int start = errMsg.indexOf(PERMISSION_DENIED_MARKER) + PERMISSION_DENIED_MARKER.length();
    int end = errMsg.indexOf('\'', start);
    if (end > start) {
      String catalog = errMsg.substring(start, end);
      handle.markCatalogInaccessible(catalog);
      logger.info("Marked catalog '{}' as inaccessible due to insufficient privileges.", catalog);
    }
  }

  private static void readResultData(ResultData result, Consumer<List<String>> rowConsumer)
      throws SQLException {
    Collection<Collection<String>> dataArray = result.getDataArray();
    if (dataArray != null) {
      for (Collection<String> row : dataArray) {
        rowConsumer.accept(new ArrayList<>(row));
      }
    }
    Collection<ExternalLink> links = result.getExternalLinks();
    if (links != null) {
      for (ExternalLink link : links) {
        readExternalLink(link, rowConsumer);
      }
    }
  }

  private static void readExternalLink(ExternalLink link, Consumer<List<String>> rowConsumer)
      throws SQLException {
    String url = link.getExternalLink();
    if (url == null) {
      return;
    }
    try (InputStream in = openLink(url, link.getHttpHeaders());
        Reader reader = new InputStreamReader(in, StandardCharsets.UTF_8);
        CSVParser parser = CSVFormat.DEFAULT.parse(reader)) {
      for (CSVRecord record : parser) {
        List<String> row = new ArrayList<>(record.size());
        for (int i = 0; i < record.size(); i++) {
          row.add(record.get(i));
        }
        rowConsumer.accept(row);
      }
    } catch (IOException e) {
      throw new SQLException("Failed to read Databricks result chunk from external link", e);
    }
  }

  private static InputStream openLink(String url, Map<String, String> headers) throws IOException {
    HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    connection.setRequestMethod("GET");
    connection.setConnectTimeout(HTTP_CONNECT_TIMEOUT_MILLIS);
    connection.setReadTimeout(HTTP_READ_TIMEOUT_MILLIS);
    if (headers != null) {
      for (Map.Entry<String, String> header : headers.entrySet()) {
        connection.setRequestProperty(header.getKey(), header.getValue());
      }
    }
    int status = connection.getResponseCode();
    if (status / 100 != 2) {
      connection.disconnect();
      throw new IOException("Result chunk download failed with HTTP status " + status);
    }
    InputStream in = connection.getInputStream();
    if ("gzip".equalsIgnoreCase(connection.getContentEncoding())) {
      return new GZIPInputStream(in);
    }
    return in;
  }

  private static ResultData fetchChunk(DatabricksHandle handle, String statementId, long chunkIndex)
      throws SQLException {
    try {
      return handle
          .getClient()
          .statementExecution()
          .getStatementResultChunkN(statementId, chunkIndex);
    } catch (Exception e) {
      throw new SQLException(
          "Failed to fetch result chunk " + chunkIndex + " for statement '" + statementId + "'", e);
    }
  }

  /**
   * Returns the next chunk index, preferring the value carried by the last external link because
   * the enclosing {@link ResultData} does not always repeat it.
   */
  private static Long nextChunkIndex(ResultData result) {
    Collection<ExternalLink> links = result.getExternalLinks();
    if (links != null) {
      Long fromLink = null;
      for (ExternalLink link : links) {
        if (link.getNextChunkIndex() != null) {
          fromLink = link.getNextChunkIndex();
        }
      }
      if (fromLink != null) {
        return fromLink;
      }
    }
    return result.getNextChunkIndex();
  }

  private static StatementState stateOf(StatementResponse response) {
    return response.getStatus() != null ? response.getStatus().getState() : null;
  }

  private static void sleepOrCancel(DatabricksHandle handle, String statementId) {
    try {
      Thread.sleep(POLL_INTERVAL_MILLIS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      cancelQuietly(handle, statementId);
      throw new RuntimeException("Interrupted while waiting for SQL statement execution", e);
    }
  }

  private static void sleep(long millis, String reason) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during " + reason, e);
    }
  }

  private static boolean isRateLimited(Throwable t) {
    Throwable current = t;
    while (current != null) {
      String msg = current.getMessage();
      if (msg != null
          && (msg.contains("429")
              || msg.toLowerCase().contains("rate limit")
              || msg.toLowerCase().contains("too many requests")
              || msg.contains("Current request has to be retried"))) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private static void cancelQuietly(DatabricksHandle handle, String statementId) {
    try {
      handle.getClient().statementExecution().cancelExecution(statementId);
    } catch (Exception e) {
      logger.debug("Failed to cancel statement '{}': {}", statementId, e.getMessage());
    }
  }
}
