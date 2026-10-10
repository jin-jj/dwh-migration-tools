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

import static com.google.common.base.CharMatcher.whitespace;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.commons.CommonsHttpClient;
import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.service.sql.CancelExecutionRequest;
import com.databricks.sdk.service.sql.ColumnInfo;
import com.databricks.sdk.service.sql.Disposition;
import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.ExecuteStatementRequestOnWaitTimeout;
import com.databricks.sdk.service.sql.ExternalLink;
import com.databricks.sdk.service.sql.Format;
import com.databricks.sdk.service.sql.QueryTag;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;
import com.databricks.sdk.service.sql.ResultSchema;
import com.databricks.sdk.service.sql.ServiceError;
import com.databricks.sdk.service.sql.StatementResponse;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksChunkDownloader.Sleeper;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes SQL statements on a Databricks SQL Warehouse via the Statement Execution API using
 * {@link Disposition#EXTERNAL_LINKS} and {@link Format#JSON_ARRAY}, streaming chunks incrementally.
 *
 * <p>Submitting a statement, polling it and fetching its result chunks are retried by {@link
 * DatabricksRetrier} when they fail with a transient error. A statement whose submission failed may
 * have started, but the statements only read metadata, so running one twice is harmless.
 */
final class DatabricksSqlHelper {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksSqlHelper.class);
  private static final String DEFAULT_WAIT_TIMEOUT = "50s";
  private static final long DEFAULT_POLL_INTERVAL_MS = 1000L;
  private static final Duration DEFAULT_STATEMENT_TIMEOUT = Duration.ofMinutes(30);
  private static final long LINK_EXPIRATION_SKEW_SECONDS = 30L;
  private static final int DEFAULT_HTTP_TIMEOUT_SECONDS = 60;

  /** The length of the statement text in log messages, after which it is cut off. */
  private static final int LOGGED_STATEMENT_LENGTH = 100;

  /**
   * The key and value of the query tag of the statements of the dumper. The query history records
   * the tags of each statement, so the logs connector leaves the statements of the dumper out.
   */
  static final String QUERY_TAG_KEY = "source";

  static final String QUERY_TAG_VALUE = "dwh-migration-dumper";

  private static final ImmutableList<QueryTag> QUERY_TAGS =
      ImmutableList.of(new QueryTag().setKey(QUERY_TAG_KEY).setValue(QUERY_TAG_VALUE));

  private final WorkspaceClient workspaceClient;
  private final String warehouseId;
  private final DatabricksChunkDownloader chunkDownloader;
  private final long pollIntervalMs;
  private final Duration statementTimeout;
  private final DatabricksRetrier retrier;

  DatabricksSqlHelper(@Nonnull WorkspaceClient workspaceClient, @Nonnull String warehouseId) {
    this(
        workspaceClient,
        warehouseId,
        new DatabricksChunkDownloader(resolveHttpClient(workspaceClient)),
        DEFAULT_POLL_INTERVAL_MS,
        DEFAULT_STATEMENT_TIMEOUT);
  }

  private static HttpClient resolveHttpClient(@Nonnull WorkspaceClient workspaceClient) {
    Preconditions.checkNotNull(workspaceClient, "WorkspaceClient was null.");
    if (workspaceClient.apiClient() != null
        && workspaceClient.apiClient().getHttpClient() != null) {
      return workspaceClient.apiClient().getHttpClient();
    }
    return new CommonsHttpClient.Builder().withTimeoutSeconds(DEFAULT_HTTP_TIMEOUT_SECONDS).build();
  }

  @VisibleForTesting
  DatabricksSqlHelper(
      @Nonnull WorkspaceClient workspaceClient,
      @Nonnull String warehouseId,
      @Nonnull DatabricksChunkDownloader chunkDownloader,
      long pollIntervalMs) {
    this(workspaceClient, warehouseId, chunkDownloader, pollIntervalMs, DEFAULT_STATEMENT_TIMEOUT);
  }

  @VisibleForTesting
  DatabricksSqlHelper(
      @Nonnull WorkspaceClient workspaceClient,
      @Nonnull String warehouseId,
      @Nonnull DatabricksChunkDownloader chunkDownloader,
      long pollIntervalMs,
      @Nonnull Duration statementTimeout) {
    this(
        workspaceClient,
        warehouseId,
        chunkDownloader,
        pollIntervalMs,
        statementTimeout,
        Thread::sleep);
  }

  /** Creates a helper whose retries wait with {@code retrySleeper}. */
  @VisibleForTesting
  DatabricksSqlHelper(
      @Nonnull WorkspaceClient workspaceClient,
      @Nonnull String warehouseId,
      @Nonnull DatabricksChunkDownloader chunkDownloader,
      long pollIntervalMs,
      @Nonnull Duration statementTimeout,
      @Nonnull Sleeper retrySleeper) {
    this.workspaceClient = Preconditions.checkNotNull(workspaceClient, "WorkspaceClient was null.");
    this.warehouseId = Preconditions.checkNotNull(warehouseId, "Warehouse ID was null.");
    this.chunkDownloader = Preconditions.checkNotNull(chunkDownloader, "ChunkDownloader was null.");
    this.pollIntervalMs = pollIntervalMs;
    this.statementTimeout =
        Preconditions.checkNotNull(statementTimeout, "StatementTimeout was null.");
    this.retrier = new DatabricksRetrier(retrySleeper, Ticker.systemTicker());
  }

  @Nonnull
  CloseableIterable<List<String>> executeSqlInCatalog(@Nonnull String catalog, @Nonnull String sql)
      throws IOException {
    return rowsOf(executeToCompletion(catalog, sql));
  }

  /**
   * Executes a SQL statement in a catalog and returns the cells of its first row by column name,
   * such as the output of {@code DESCRIBE DETAIL}. A null cell has no entry, and the map is empty
   * if the statement returns no row.
   */
  @Nonnull
  ImmutableMap<String, String> executeSqlInCatalogForFirstRow(
      @Nonnull String catalog, @Nonnull String sql) throws IOException {
    SucceededStatement statement = executeToCompletion(catalog, sql);
    ImmutableList<String> columnNames = columnNames(statement.response.getManifest());
    try (CloseableIterable<List<String>> rows = rowsOf(statement)) {
      Iterator<List<String>> iterator = rows.iterator();
      if (!iterator.hasNext()) {
        return ImmutableMap.of();
      }

      List<String> row = iterator.next();
      Map<String, String> cells = new LinkedHashMap<>();
      for (int index = 0; index < columnNames.size() && index < row.size(); index++) {
        String cell = row.get(index);
        if (cell != null) {
          cells.putIfAbsent(columnNames.get(index), cell);
        }
      }
      return ImmutableMap.copyOf(cells);
    }
  }

  /** Executes a SQL statement in a catalog and waits until it succeeds. */
  @Nonnull
  private SucceededStatement executeToCompletion(@Nonnull String catalog, @Nonnull String sql)
      throws IOException {
    Preconditions.checkNotNull(catalog, "Catalog was null.");
    Preconditions.checkNotNull(sql, "SQL statement was null.");
    ExecuteStatementRequest request =
        new ExecuteStatementRequest()
            .setWarehouseId(warehouseId)
            .setCatalog(catalog)
            .setStatement(sql)
            .setDisposition(Disposition.EXTERNAL_LINKS)
            .setFormat(Format.JSON_ARRAY)
            .setWaitTimeout(DEFAULT_WAIT_TIMEOUT)
            .setOnWaitTimeout(ExecuteStatementRequestOnWaitTimeout.CONTINUE)
            .setQueryTags(QUERY_TAGS);

    StatementResponse response;
    try {
      response =
          retrier.call(
              "Submitting " + abbreviateStatement(sql),
              () -> workspaceClient.statementExecution().executeStatement(request));
    } catch (DatabricksException e) {
      throw new DatabricksSqlException(
          "Databricks Statement Execution API call failed: " + e.getMessage(), e);
    }

    String statementId = response != null ? response.getStatementId() : null;
    Instant deadline = Instant.now().plus(statementTimeout);
    while (response != null && isRunningState(extractState(response))) {
      if (statementId == null) {
        throw new DatabricksSqlException(
            "Statement is in RUNNING/PENDING state without a statementId.");
      }
      if (!Instant.now().isBefore(deadline)) {
        cancelQuietly(statementId);
        throw new DatabricksSqlException(
            String.format(
                "Timed out waiting for SQL statement %s after %s.", statementId, statementTimeout));
      }
      sleepForPoll(statementId);
      try {
        response =
            retrier.call(
                "Polling statement " + statementId,
                () -> workspaceClient.statementExecution().getStatement(statementId));
      } catch (InterruptedIOException e) {
        cancelQuietly(statementId);
        throw e;
      } catch (DatabricksException e) {
        throw new DatabricksSqlException(
            "Failed polling statement " + statementId + ": " + e.getMessage(), e);
      }
    }

    if (response == null) {
      throw new DatabricksSqlException("Received null StatementResponse.");
    }

    StatementStatus status = response.getStatus();
    StatementState state = extractState(response);
    if (state != StatementState.SUCCEEDED) {
      String sqlState = status != null ? status.getSqlState() : null;
      ServiceError err = status != null ? status.getError() : null;
      String errCode = err != null && err.getErrorCode() != null ? err.getErrorCode().name() : null;
      String errMsg = err != null && err.getMessage() != null ? err.getMessage() : "Unknown error";
      throw new DatabricksSqlException(
          String.format(
              "SQL statement %s failed in state %s (SQLSTATE: %s, errorCode: %s): %s",
              statementId, state, sqlState, errCode, errMsg));
    }
    return new SucceededStatement(statementId, response);
  }

  /** Returns the rows of a statement, fetching the chunks after the first as they are read. */
  @Nonnull
  private CloseableIterable<List<String>> rowsOf(@Nonnull SucceededStatement statement)
      throws IOException {
    ResultData firstResult = statement.response.getResult();
    ResultManifest manifest = statement.response.getManifest();
    Long totalChunks = manifest != null ? manifest.getTotalChunkCount() : null;

    ChunkRowIterator rowIterator =
        new ChunkRowIterator(statement.statementId, firstResult, totalChunks);
    return new CloseableIterable<List<String>>() {
      @Override
      @Nonnull
      public Iterator<List<String>> iterator() {
        return rowIterator;
      }

      @Override
      public void close() {
        rowIterator.close();
      }
    };
  }

  /** Returns the column names of a result in their order, or an empty list without a schema. */
  @Nonnull
  private static ImmutableList<String> columnNames(@CheckForNull ResultManifest manifest) {
    ResultSchema schema = manifest != null ? manifest.getSchema() : null;
    if (schema == null || schema.getColumns() == null) {
      return ImmutableList.of();
    }

    ImmutableList.Builder<String> names = ImmutableList.builder();
    for (ColumnInfo column : schema.getColumns()) {
      names.add(Strings.nullToEmpty(column.getName()));
    }
    return names.build();
  }

  /** A statement that succeeded, with the ID that it was submitted with. */
  private static final class SucceededStatement {

    @CheckForNull final String statementId;
    final StatementResponse response;

    SucceededStatement(@CheckForNull String statementId, @Nonnull StatementResponse response) {
      this.statementId = statementId;
      this.response = response;
    }
  }

  @CheckForNull
  private static StatementState extractState(@CheckForNull StatementResponse response) {
    if (response == null || response.getStatus() == null) {
      return null;
    }
    return response.getStatus().getState();
  }

  private static boolean isRunningState(@CheckForNull StatementState state) {
    return state == StatementState.PENDING || state == StatementState.RUNNING;
  }

  private void sleepForPoll(@Nonnull String statementId) throws IOException {
    if (pollIntervalMs <= 0L) {
      return;
    }
    try {
      Thread.sleep(pollIntervalMs);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      cancelQuietly(statementId);
      throw new DatabricksSqlException(
          "Interrupted while waiting for SQL statement " + statementId, e);
    }
  }

  private void cancelQuietly(@CheckForNull String statementId) {
    if (statementId == null) {
      return;
    }
    try {
      workspaceClient
          .statementExecution()
          .cancelExecution(new CancelExecutionRequest().setStatementId(statementId));
    } catch (RuntimeException e) {
      logger.debug("Ignored error cancelling statement {}: {}", statementId, e.getMessage());
    }
  }

  @CheckForNull
  static String cell(@CheckForNull List<String> row, int index) {
    if (row == null || index < 0 || index >= row.size()) {
      return null;
    }
    return row.get(index);
  }

  @Nonnull
  static String cellOrEmpty(@CheckForNull List<String> row, int index) {
    String val = cell(row, index);
    return val != null ? val : "";
  }

  /** Quotes a SQL identifier with backticks, doubling any backtick inside it. */
  @Nonnull
  static String quoteIdentifier(@Nonnull String identifier) {
    return "`" + identifier.replace("`", "``") + "`";
  }

  /**
   * Quotes a SQL string literal. Backslashes and single quotes are escaped with a backslash, as in
   * the output of {@code SHOW CREATE TABLE}.
   */
  @Nonnull
  static String quoteStringLiteral(@Nonnull String value) {
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
  }

  /**
   * Returns a statement for log messages: on one line, and cut off after {@link
   * #LOGGED_STATEMENT_LENGTH} characters.
   */
  @Nonnull
  static String abbreviateStatement(@Nonnull String sql) {
    String line = whitespace().trimAndCollapseFrom(sql, ' ');
    return line.length() <= LOGGED_STATEMENT_LENGTH
        ? line
        : line.substring(0, LOGGED_STATEMENT_LENGTH) + "...";
  }

  private final class ChunkRowIterator implements Iterator<List<String>> {

    @CheckForNull private final String statementId;
    @CheckForNull private final Long totalChunkCount;
    @CheckForNull private ResultData currentChunkData;
    private long currentChunkIndex;
    private Iterator<List<String>> currentRows = Collections.emptyIterator();
    private boolean closed;

    ChunkRowIterator(
        @CheckForNull String statementId,
        @CheckForNull ResultData initialChunkData,
        @CheckForNull Long totalChunkCount)
        throws IOException {
      this.statementId = statementId;
      this.totalChunkCount = totalChunkCount;
      this.currentChunkData = initialChunkData;
      this.currentChunkIndex =
          initialChunkData != null && initialChunkData.getChunkIndex() != null
              ? initialChunkData.getChunkIndex()
              : 0L;
      if (initialChunkData != null) {
        this.currentRows = loadRowsFromChunkData(initialChunkData).iterator();
      }
    }

    @Override
    public boolean hasNext() {
      if (closed) {
        return false;
      }
      while (!currentRows.hasNext()) {
        Long nextIdx = resolveNextChunkIndex();
        if (nextIdx == null || statementId == null) {
          closed = true;
          return false;
        }
        long chunkIndex = nextIdx.longValue();
        try {
          currentChunkData =
              retrier.call(
                  "Fetching chunk " + chunkIndex + " of statement " + statementId,
                  () ->
                      workspaceClient
                          .statementExecution()
                          .getStatementResultChunkN(statementId, chunkIndex));
          currentChunkIndex = chunkIndex;
          if (currentChunkData == null) {
            closed = true;
            return false;
          }
          currentRows = loadRowsFromChunkData(currentChunkData).iterator();
        } catch (IOException e) {
          closed = true;
          throw new UncheckedIOException(e);
        } catch (DatabricksException e) {
          closed = true;
          throw new UncheckedIOException(
              new DatabricksSqlException(
                  "Failed fetching chunk " + chunkIndex + " for statement " + statementId, e));
        }
      }
      return true;
    }

    @Override
    public List<String> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return currentRows.next();
    }

    @CheckForNull
    private Long resolveNextChunkIndex() {
      if (currentChunkData == null) {
        return null;
      }
      if (currentChunkData.getNextChunkIndex() != null) {
        return currentChunkData.getNextChunkIndex();
      }
      Collection<ExternalLink> links = currentChunkData.getExternalLinks();
      if (links != null) {
        for (ExternalLink link : links) {
          if (link != null && link.getNextChunkIndex() != null) {
            return link.getNextChunkIndex();
          }
        }
      }
      if (totalChunkCount != null && currentChunkIndex + 1L < totalChunkCount.longValue()) {
        return Long.valueOf(currentChunkIndex + 1L);
      }
      return null;
    }

    @Nonnull
    private List<List<String>> loadRowsFromChunkData(@Nonnull ResultData chunkData)
        throws IOException {
      ResultData activeChunkData = chunkData;
      Collection<ExternalLink> externalLinks = activeChunkData.getExternalLinks();
      if (externalLinks != null
          && !externalLinks.isEmpty()
          && statementId != null
          && isAnyExternalLinkExpired(externalLinks)) {
        long chunkIdx =
            activeChunkData.getChunkIndex() != null
                ? activeChunkData.getChunkIndex().longValue()
                : currentChunkIndex;
        logger.info(
            "Refreshing expired pre-signed external link for statement {} chunk {}.",
            statementId,
            chunkIdx);
        try {
          ResultData refreshed =
              retrier.call(
                  "Refreshing the links of chunk " + chunkIdx + " of statement " + statementId,
                  () ->
                      workspaceClient
                          .statementExecution()
                          .getStatementResultChunkN(statementId, chunkIdx));
          if (refreshed != null && refreshed.getExternalLinks() != null) {
            activeChunkData = refreshed;
            currentChunkData = refreshed;
            externalLinks = refreshed.getExternalLinks();
          }
        } catch (DatabricksException e) {
          throw new DatabricksSqlException(
              "Failed refreshing expired external link for statement "
                  + statementId
                  + " chunk "
                  + chunkIdx,
              e);
        }
      }
      if (externalLinks != null && !externalLinks.isEmpty()) {
        List<List<String>> combined = new ArrayList<>();
        for (ExternalLink link : externalLinks) {
          if (link != null && link.getExternalLink() != null) {
            combined.addAll(chunkDownloader.downloadChunkRows(link));
          }
        }
        return combined;
      }
      Collection<Collection<String>> inlineData = activeChunkData.getDataArray();
      if (inlineData != null && !inlineData.isEmpty()) {
        List<List<String>> rows = new ArrayList<>(inlineData.size());
        for (Collection<String> row : inlineData) {
          rows.add(row != null ? new ArrayList<>(row) : Collections.emptyList());
        }
        return rows;
      }
      return Collections.emptyList();
    }

    private boolean isAnyExternalLinkExpired(@Nonnull Collection<ExternalLink> links) {
      Instant threshold = Instant.now().plusSeconds(LINK_EXPIRATION_SKEW_SECONDS);
      for (ExternalLink link : links) {
        if (link == null || link.getExpiration() == null || link.getExpiration().trim().isEmpty()) {
          continue;
        }
        try {
          Instant exp = Instant.parse(link.getExpiration().trim());
          if (!exp.isAfter(threshold)) {
            return true;
          }
        } catch (DateTimeParseException ignored) {
          // Ignore unparseable expiration timestamp and proceed with download
        }
      }
      return false;
    }

    void close() {
      closed = true;
      currentRows = Collections.emptyIterator();
    }
  }
}
