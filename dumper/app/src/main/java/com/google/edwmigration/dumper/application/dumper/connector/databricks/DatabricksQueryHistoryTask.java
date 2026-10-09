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

import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.service.sql.ListQueriesResponse;
import com.databricks.sdk.service.sql.ListQueryHistoryRequest;
import com.databricks.sdk.service.sql.QueryFilter;
import com.databricks.sdk.service.sql.QueryInfo;
import com.databricks.sdk.service.sql.QueryMetrics;
import com.databricks.sdk.service.sql.QueryTag;
import com.databricks.sdk.service.sql.TimeRange;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.RecordProgressMonitor;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksLogsDumpFormat.Header;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task for exporting query history from Databricks {@code system.query.history} to CSV, with
 * automatic fallback to the Workspace Query History REST API ({@code GET
 * /api/2.0/sql/history/queries}) when the caller lacks permission on the {@code system} catalog or
 * when {@code statement_text} is {@code <REDACTED>}.
 */
class DatabricksQueryHistoryTask extends AbstractTask<Void> {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksQueryHistoryTask.class);

  private static final AtomicBoolean hasWarnedRedaction = new AtomicBoolean(false);

  private static final int MAX_PREVIEW_ROWS = 64;

  private final String query;
  @CheckForNull private final ZonedInterval interval;
  @CheckForNull private final String workspaceId;
  private final boolean allowRestFallback;

  DatabricksQueryHistoryTask(@Nonnull String targetPath, @Nonnull String query) {
    this(targetPath, query, null, null, false);
  }

  DatabricksQueryHistoryTask(
      @Nonnull String targetPath,
      @Nonnull String query,
      @Nullable ZonedInterval interval,
      @Nullable String workspaceId,
      boolean allowRestFallback) {
    super(targetPath);
    this.query = Preconditions.checkNotNull(query, "Query cannot be null.");
    this.interval = interval;
    this.workspaceId = workspaceId;
    this.allowRestFallback = allowRestFallback && interval != null;
  }

  @Nonnull
  @Override
  public TaskCategory getCategory() {
    return TaskCategory.REQUIRED;
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    DatabricksHandle databricksHandle = (DatabricksHandle) handle;
    logger.info("Writing Databricks query history to '{}'", getTargetPath());

    if (allowRestFallback
        && (!databricksHandle.hasWarehouseId()
            || databricksHandle.isSystemQueryHistoryUnavailable())) {
      databricksHandle.markSystemQueryHistoryUnavailable();
      dumpViaRest(databricksHandle, sink);
      return null;
    }

    int columnCount = Header.values().length;
    try (LazyCsvOutput output = new LazyCsvOutput(sink, getTargetPath(), columnCount)) {
      DatabricksSqlHelper.executeBulkQueryOrThrow(
          databricksHandle,
          query,
          row -> {
            String statementText = cell(row, 5);
            if ("<REDACTED>".equals(statementText)) {
              if (allowRestFallback && !output.isOpened()) {
                throw new RedactedQueryHistoryException();
              }
              if (hasWarnedRedaction.compareAndSet(false, true)) {
                logger.warn(
                    "Query text in Databricks system.query.history is <REDACTED>. To export actual"
                        + " query text from system.query.history, ensure the querying principal is"
                        + " an Account Admin or belongs to the 'databricks_pii_access' group.");
              }
            }
            try {
              output.writeRow(row, statementText != null && !statementText.isEmpty());
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          });
      output.flushAndEnsureHeader();
    } catch (RedactedQueryHistoryException e) {
      if (databricksHandle.markSystemQueryHistoryUnavailable()) {
        logger.warn(
            "Query text (statement_text) in Databricks system.query.history is <REDACTED> because"
                + " the caller is not an Account Admin or member of 'databricks_pii_access'."
                + " Automatically falling back to the Workspace Query History REST API"
                + " (/api/2.0/sql/history/queries) to extract unredacted query text.");
      }
      dumpViaRest(databricksHandle, sink);
    } catch (SQLException | DatabricksException e) {
      if (allowRestFallback) {
        if (databricksHandle.markSystemQueryHistoryUnavailable()) {
          logger.warn(
              "Unable to query Databricks system.query.history ({}). Automatically falling back to"
                  + " the Workspace Query History REST API (/api/2.0/sql/history/queries).",
              e.getMessage());
        }
        dumpViaRest(databricksHandle, sink);
      } else {
        throw e;
      }
    }
    return null;
  }

  private void dumpViaRest(@Nonnull DatabricksHandle handle, @Nonnull ByteSink sink)
      throws IOException {
    Preconditions.checkState(
        interval != null, "Cannot fall back to Query History REST API without a ZonedInterval.");
    long startMs = interval.getStart().toInstant().toEpochMilli();
    long endMs = interval.getEndExclusive().toInstant().toEpochMilli();

    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
        CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer);
        RecordProgressMonitor monitor =
            new RecordProgressMonitor("Writing REST query history to " + getTargetPath())) {
      DatabricksRestHelper.forEachItem(
          handle,
          "listing query history for " + interval,
          pageToken -> {
            TimeRange timeRange = new TimeRange().setStartTimeMs(startMs).setEndTimeMs(endMs);
            QueryFilter filter = new QueryFilter().setQueryStartTimeRange(timeRange);
            ListQueryHistoryRequest request =
                new ListQueryHistoryRequest()
                    .setFilterBy(filter)
                    .setIncludeMetrics(true)
                    .setMaxResults(DatabricksRestHelper.PAGE_SIZE);
            if (pageToken != null) {
              request.setPageToken(pageToken);
            }
            ListQueriesResponse response = handle.getClient().queryHistory().impl().list(request);
            String nextToken =
                Boolean.FALSE.equals(response.getHasNextPage())
                    ? null
                    : response.getNextPageToken();
            return new DatabricksRestHelper.Page<>(response.getRes(), nextToken);
          },
          queryInfo -> {
            if (isDumperQuery(queryInfo)) {
              return;
            }
            monitor.count();
            printer.printRecord(toCsvRecord(queryInfo, workspaceId));
          });
    }
  }

  @VisibleForTesting
  static boolean isDumperQuery(@Nonnull QueryInfo info) {
    if (DatabricksHandle.CLIENT_APPLICATION.equals(info.getClientApplication())) {
      return true;
    }
    Collection<QueryTag> tags = info.getQueryTags();
    if (tags != null) {
      for (QueryTag tag : tags) {
        if (tag != null
            && DatabricksHandle.QUERY_TAG_KEY.equals(tag.getKey())
            && DatabricksHandle.CLIENT_APPLICATION.equals(tag.getValue())) {
          return true;
        }
      }
    }
    return false;
  }

  @Nonnull
  @VisibleForTesting
  static List<Object> toCsvRecord(@Nonnull QueryInfo info, @Nullable String workspaceId) {
    QueryMetrics metrics = info.getMetrics();
    String executedBy =
        info.getExecutedAsUserName() != null ? info.getExecutedAsUserName() : info.getUserName();
    String statementType = info.getStatementType() != null ? info.getStatementType().name() : null;
    String status = info.getStatus() != null ? info.getStatus().name() : null;
    String warehouseId =
        info.getWarehouseId() != null ? info.getWarehouseId() : info.getEndpointId();
    String computeType = warehouseId != null ? "WAREHOUSE" : null;
    String startTime =
        info.getQueryStartTimeMs() != null
            ? Instant.ofEpochMilli(info.getQueryStartTimeMs()).toString()
            : null;
    Long endMs =
        info.getQueryEndTimeMs() != null ? info.getQueryEndTimeMs() : info.getExecutionEndTimeMs();
    String endTime = endMs != null ? Instant.ofEpochMilli(endMs).toString() : null;
    Long totalDurationMs =
        metrics != null && metrics.getTotalTimeMs() != null
            ? metrics.getTotalTimeMs()
            : info.getDuration();
    Long compilationDurationMs = metrics != null ? metrics.getCompilationTimeMs() : null;
    Long executionDurationMs = metrics != null ? metrics.getExecutionTimeMs() : null;
    Long readBytes = metrics != null ? metrics.getReadBytes() : null;
    Long readRows = metrics != null ? metrics.getRowsReadCount() : null;
    Long writtenBytes = metrics != null ? metrics.getWriteRemoteBytes() : null;
    Long writtenRows =
        metrics != null && metrics.getRowsProducedCount() != null
            ? metrics.getRowsProducedCount()
            : info.getRowsProduced();
    Long spilledLocalBytes = metrics != null ? metrics.getSpillToDiskBytes() : null;

    return Arrays.asList(
        null, // AccountId
        workspaceId, // WorkspaceId
        info.getQueryId(), // StatementId
        info.getSessionId(), // SessionId
        executedBy, // ExecutedBy
        info.getQueryText(), // StatementText
        statementType, // StatementType
        status, // ExecutionStatus
        info.getErrorMessage(), // ErrorMessage
        warehouseId, // WarehouseId
        computeType, // ComputeType
        startTime, // StartTime
        endTime, // EndTime
        totalDurationMs, // TotalDurationMs
        compilationDurationMs, // CompilationDurationMs
        executionDurationMs, // ExecutionDurationMs
        null, // WaitingAtCapacityDurationMs
        readBytes, // ReadBytes
        readRows, // ReadRows
        writtenBytes, // WrittenBytes
        writtenRows, // WrittenRows
        spilledLocalBytes, // SpilledLocalBytes
        info.getClientApplication() // ClientApplication
        );
  }

  @CheckForNull
  private static String cell(@Nonnull List<String> row, int index) {
    return index < row.size() ? row.get(index) : null;
  }

  private static final class RedactedQueryHistoryException extends RuntimeException {}

  /**
   * Buffers up to {@link #MAX_PREVIEW_ROWS} rows before opening the underlying {@link ByteSink} so
   * that if {@code system.query.history} fails or returns {@code <REDACTED>} query text, the task
   * can cleanly fall back to the REST API without writing a duplicate or partial ZIP entry.
   */
  private static final class LazyCsvOutput implements Closeable {

    private final ByteSink sink;
    private final String targetPath;
    private final int columnCount;
    private final List<List<String>> previewRows = new ArrayList<>();
    @CheckForNull private Writer writer;
    @CheckForNull private CSVPrinter printer;
    @CheckForNull private RecordProgressMonitor monitor;

    LazyCsvOutput(@Nonnull ByteSink sink, @Nonnull String targetPath, int columnCount) {
      this.sink = sink;
      this.targetPath = targetPath;
      this.columnCount = columnCount;
    }

    boolean isOpened() {
      return printer != null;
    }

    void writeRow(@Nonnull List<String> row, boolean hasStatementText) throws IOException {
      List<String> record = new ArrayList<>(columnCount);
      for (int i = 0; i < columnCount; i++) {
        record.add(cell(row, i));
      }
      if (printer != null) {
        monitor.count();
        printer.printRecord(record);
        return;
      }
      previewRows.add(record);
      if (hasStatementText || previewRows.size() >= MAX_PREVIEW_ROWS) {
        openAndFlushPreview();
      }
    }

    void flushAndEnsureHeader() throws IOException {
      if (printer == null) {
        openAndFlushPreview();
      }
    }

    private void openAndFlushPreview() throws IOException {
      this.writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
      this.printer = FORMAT.withHeader(Header.class).print(writer);
      this.monitor = new RecordProgressMonitor("Writing query history to " + targetPath);
      for (List<String> record : previewRows) {
        monitor.count();
        printer.printRecord(record);
      }
      previewRows.clear();
    }

    @Override
    public void close() throws IOException {
      IOException firstException = null;
      if (monitor != null) {
        try {
          monitor.close();
        } catch (Exception e) {
          // RecordProgressMonitor.close() does not throw IOException in practice
        }
      }
      if (printer != null) {
        try {
          printer.close();
        } catch (IOException e) {
          firstException = e;
        }
      }
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException e) {
          if (firstException == null) {
            firstException = e;
          }
        }
      }
      if (firstException != null) {
        throw firstException;
      }
    }
  }
}
