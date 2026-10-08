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

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.commons.CommonsHttpClient;
import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.service.sql.CancelExecutionRequest;
import com.databricks.sdk.service.sql.Disposition;
import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.ExecuteStatementRequestOnWaitTimeout;
import com.databricks.sdk.service.sql.ExternalLink;
import com.databricks.sdk.service.sql.Format;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;
import com.databricks.sdk.service.sql.ServiceError;
import com.databricks.sdk.service.sql.StatementResponse;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes SQL statements on a Databricks SQL Warehouse via the Statement Execution API using
 * {@link Disposition#EXTERNAL_LINKS} and {@link Format#JSON_ARRAY}, streaming chunks incrementally.
 */
final class DatabricksSqlHelper {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksSqlHelper.class);
  private static final String DEFAULT_WAIT_TIMEOUT = "50s";
  private static final long DEFAULT_POLL_INTERVAL_MS = 1000L;
  private static final Duration DEFAULT_STATEMENT_TIMEOUT = Duration.ofMinutes(30);
  private static final long LINK_EXPIRATION_SKEW_SECONDS = 30L;
  private static final int DEFAULT_HTTP_TIMEOUT_SECONDS = 60;

  private final WorkspaceClient workspaceClient;
  private final String warehouseId;
  private final DatabricksChunkDownloader chunkDownloader;
  private final long pollIntervalMs;
  private final Duration statementTimeout;

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
    this.workspaceClient = Preconditions.checkNotNull(workspaceClient, "WorkspaceClient was null.");
    this.warehouseId = Preconditions.checkNotNull(warehouseId, "Warehouse ID was null.");
    this.chunkDownloader = Preconditions.checkNotNull(chunkDownloader, "ChunkDownloader was null.");
    this.pollIntervalMs = pollIntervalMs;
    this.statementTimeout =
        Preconditions.checkNotNull(statementTimeout, "StatementTimeout was null.");
  }

  @Nonnull
  CloseableIterable<List<String>> executeSqlInCatalog(@Nonnull String catalog, @Nonnull String sql)
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
            .setOnWaitTimeout(ExecuteStatementRequestOnWaitTimeout.CONTINUE);

    StatementResponse response;
    try {
      response = workspaceClient.statementExecution().executeStatement(request);
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
        response = workspaceClient.statementExecution().getStatement(statementId);
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

    ResultData firstResult = response.getResult();
    ResultManifest manifest = response.getManifest();
    Long totalChunks = manifest != null ? manifest.getTotalChunkCount() : null;

    ChunkRowIterator rowIterator = new ChunkRowIterator(statementId, firstResult, totalChunks);
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
        try {
          currentChunkData =
              workspaceClient
                  .statementExecution()
                  .getStatementResultChunkN(statementId, nextIdx.longValue());
          currentChunkIndex = nextIdx.longValue();
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
                  "Failed fetching chunk " + nextIdx + " for statement " + statementId, e));
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
              workspaceClient.statementExecution().getStatementResultChunkN(statementId, chunkIdx);
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
