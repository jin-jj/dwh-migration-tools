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
package com.google.edwmigration.dumper.application.dumper.connector.databricks.logs;

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper.SqlQuoting.quoteStringLiteral;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper.StatementResult.cell;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.databricks.sdk.service.sql.ListQueriesResponse;
import com.databricks.sdk.service.sql.QueryInfo;
import com.databricks.sdk.service.sql.QueryMetrics;
import com.databricks.sdk.service.sql.QueryTag;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksRestHelper;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper.SqlQuoting;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper.StatementResult;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksLogsDumpFormat.Header;
import com.google.edwmigration.dumper.application.dumper.handle.AbstractHandle;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.io.Writer;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.csv.CSVPrinter;

/**
 * Dumps the Databricks queries that started in one interval to one CSV zip entry, from
 * system.query.history if the {@link DatabricksLogsHandle} has a SQL warehouse, or else from the
 * query history API.
 */
public final class QueryHistoryTask extends AbstractTask<Void> {

  public static final String REDACTED_STATEMENT_TEXT = "<REDACTED>";
  private static final String SYSTEM_CATALOG = "system";
  private static final String WAREHOUSE_COMPUTE_TYPE = "WAREHOUSE";
  // The columns of system.query.history, in the order of Header.
  private static final String SYSTEM_TABLE_COLUMNS =
      "account_id, workspace_id, statement_id, session_id, executed_by, statement_text,"
          + " statement_type, execution_status, error_message, compute.warehouse_id, compute.type,"
          + " unix_millis(start_time), unix_millis(end_time), total_duration_ms,"
          + " compilation_duration_ms, execution_duration_ms, waiting_at_capacity_duration_ms,"
          + " read_bytes, read_rows, produced_rows, written_bytes, written_rows,"
          + " spilled_local_bytes, client_application";
  private static final Header[] HEADERS = Header.values();

  /**
   * Thread-safe shared {@code Handle} for the Databricks logs connector. Without a {@link
   * DatabricksSqlHelper}, the query history API is read instead of system.query.history.
   */
  @ThreadSafe
  public static final class DatabricksLogsHandle extends AbstractHandle {

    private final DatabricksSqlHelper sqlHelper;
    private final DatabricksRestHelper restHelper;
    private final String urlWorkspaceId;
    private final ImmutableList<String> workspaceIds;

    public DatabricksLogsHandle(
        DatabricksSqlHelper sqlHelper,
        DatabricksRestHelper restHelper,
        String urlWorkspaceId,
        ImmutableList<String> workspaceIds) {
      this.sqlHelper = sqlHelper;
      this.restHelper = Preconditions.checkNotNull(restHelper, "DatabricksRestHelper was null.");
      this.urlWorkspaceId = urlWorkspaceId;
      this.workspaceIds = Preconditions.checkNotNull(workspaceIds, "Workspace IDs were null.");
    }

    /** Returns the helper that reads system.query.history, or null to read the API instead. */
    public DatabricksSqlHelper sqlHelper() {
      return this.sqlHelper;
    }

    public String urlWorkspaceId() {
      return this.urlWorkspaceId;
    }

    public ImmutableList<String> workspaceIds() {
      return this.workspaceIds;
    }
  }

  private final ZonedInterval interval;

  public QueryHistoryTask(String targetPath, ZonedInterval interval) {
    super(targetPath);
    this.interval = Preconditions.checkNotNull(interval, "Interval was null.");
  }

  @Override
  public TaskCategory getCategory() {
    return TaskCategory.REQUIRED;
  }

  @Override
  protected Void doRun(TaskRunContext context, ByteSink sink, Handle handle) throws Exception {
    DatabricksLogsHandle logsHandle = (DatabricksLogsHandle) handle;
    if (logsHandle.sqlHelper() == null) {
      writeApiQueries(sink, logsHandle);
    } else {
      writeSystemTableQueries(sink, logsHandle);
    }
    return null;
  }

  /**
   * Returns the statement text of one query of the range that has a statement text, or null if no
   * query has one. Fails if system.query.history can't be read.
   */
  public static String probeStatementText(
      DatabricksSqlHelper sqlHelper, ZonedInterval range, ImmutableList<String> workspaceIds)
      throws IOException {
    String sql = systemTableQuery(range, workspaceIds) + " AND statement_text <> '' LIMIT 1";
    StatementResult rows = sqlHelper.executeSqlInCatalog(SYSTEM_CATALOG, sql);
    return cell(rows.nextRow(), Header.StatementText.ordinal());
  }

  private void writeSystemTableQueries(ByteSink sink, DatabricksLogsHandle handle)
      throws IOException {
    String sql = systemTableQuery(this.interval, handle.workspaceIds());
    StatementResult rows = handle.sqlHelper().executeSqlInCatalog(SYSTEM_CATALOG, sql);
    try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
        CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer)) {
      for (List<String> row = rows.nextRow(); row != null; row = rows.nextRow()) {
        printer.printRecord(systemTableRecord(row));
      }
    }
  }

  // Each page is written before the next one is read. The API may send a page token with the last
  // page, so has_next_page decides whether there is a next page. The time range of the API includes
  // its end, so the range ends at the last millisecond of the interval.
  private void writeApiQueries(ByteSink sink, DatabricksLogsHandle handle) throws IOException {
    long startTimeMillis = toEpochMilli(this.interval.getStart());
    long endTimeMillis = toEpochMilli(this.interval.getEndInclusive());
    try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
        CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer)) {
      String pageToken = null;
      do {
        String previousPageToken = pageToken;
        ListQueriesResponse page =
            handle.restHelper.listQueryHistoryPage(startTimeMillis, endTimeMillis, pageToken);
        if (page.getRes() != null) {
          for (QueryInfo query : page.getRes()) {
            if (!isDumperQuery(query)) {
              printer.printRecord(apiRecord(query, handle.urlWorkspaceId()));
            }
          }
        }
        pageToken = Boolean.FALSE.equals(page.getHasNextPage()) ? null : page.getNextPageToken();
        if (pageToken != null && pageToken.equals(previousPageToken)) {
          throw new IOException("Databricks returned the page token '" + pageToken + "' twice.");
        }
      } while (!Strings.isNullOrEmpty(pageToken));
    }
  }

  static String systemTableQuery(ZonedInterval interval, ImmutableList<String> workspaceIds) {
    String sql =
        "SELECT "
            + SYSTEM_TABLE_COLUMNS
            + " FROM system.query.history WHERE start_time >= timestamp_millis("
            + toEpochMilli(interval.getStart())
            + ") AND start_time < timestamp_millis("
            + toEpochMilli(interval.getEndExclusive())
            + ") AND COALESCE(try_element_at(query_tags, "
            + quoteStringLiteral(DatabricksSqlHelper.QUERY_TAG_KEY)
            + "), '') <> "
            + quoteStringLiteral(DatabricksSqlHelper.QUERY_TAG_VALUE);
    if (workspaceIds.isEmpty()) {
      return sql;
    }

    List<String> quotedIds = Lists.transform(workspaceIds, SqlQuoting::quoteStringLiteral);
    return sql + " AND workspace_id IN (" + String.join(", ", quotedIds) + ")";
  }

  private static long toEpochMilli(ZonedDateTime dateTime) {
    return dateTime.toInstant().toEpochMilli();
  }

  // A row has a cell for each header. The times are epoch milliseconds.
  private static Object[] systemTableRecord(List<String> row) {
    Object[] record = row.toArray();
    for (Header header : ImmutableList.of(Header.StartTime, Header.EndTime)) {
      String epochMillis = row.get(header.ordinal());
      if (epochMillis != null) {
        record[header.ordinal()] = formatTime(Long.parseLong(epochMillis));
      }
    }
    return record;
  }

  private static Object[] apiRecord(QueryInfo query, String workspaceId) {
    QueryMetrics metrics = query.getMetrics() != null ? query.getMetrics() : new QueryMetrics();
    String warehouseId = query.getWarehouseId();
    String executedBy =
        query.getUserName() != null ? query.getUserName() : query.getExecutedAsUserName();
    Long endTime =
        query.getQueryEndTimeMs() != null
            ? query.getQueryEndTimeMs()
            : query.getExecutionEndTimeMs();
    Long totalDuration =
        metrics.getTotalTimeMs() != null ? metrics.getTotalTimeMs() : query.getDuration();
    Long producedRows =
        metrics.getRowsProducedCount() != null
            ? metrics.getRowsProducedCount()
            : query.getRowsProduced();
    // CSVPrinter prints null values as empty and an empty first value as "".
    Object[] record = new Object[HEADERS.length];
    record[Header.AccountId.ordinal()] = "";
    record[Header.WorkspaceId.ordinal()] = workspaceId;
    record[Header.StatementId.ordinal()] = query.getQueryId();
    record[Header.SessionId.ordinal()] = query.getSessionId();
    record[Header.ExecutedBy.ordinal()] = executedBy;
    record[Header.StatementText.ordinal()] = query.getQueryText();
    record[Header.StatementType.ordinal()] = query.getStatementType();
    record[Header.ExecutionStatus.ordinal()] = query.getStatus();
    record[Header.ErrorMessage.ordinal()] = query.getErrorMessage();
    record[Header.WarehouseId.ordinal()] = warehouseId;
    record[Header.ComputeType.ordinal()] = warehouseId == null ? "" : WAREHOUSE_COMPUTE_TYPE;
    record[Header.StartTime.ordinal()] = formatTime(query.getQueryStartTimeMs());
    record[Header.EndTime.ordinal()] = formatTime(endTime);
    record[Header.TotalDurationMs.ordinal()] = totalDuration;
    record[Header.CompilationDurationMs.ordinal()] = metrics.getCompilationTimeMs();
    record[Header.ExecutionDurationMs.ordinal()] = metrics.getExecutionTimeMs();
    record[Header.ReadBytes.ordinal()] = metrics.getReadBytes();
    record[Header.ReadRows.ordinal()] = metrics.getRowsReadCount();
    record[Header.ProducedRows.ordinal()] = producedRows;
    record[Header.WrittenBytes.ordinal()] = metrics.getWriteRemoteBytes();
    record[Header.SpilledLocalBytes.ordinal()] = metrics.getSpillToDiskBytes();
    record[Header.ClientApplication.ordinal()] = query.getClientApplication();
    return record;
  }

  private static String formatTime(Long epochMillis) {
    return epochMillis == null ? "" : Instant.ofEpochMilli(epochMillis).toString();
  }

  private static boolean isDumperQuery(QueryInfo query) {
    if (query.getQueryTags() == null) {
      return false;
    }
    for (QueryTag tag : query.getQueryTags()) {
      if (tag != null
          && DatabricksSqlHelper.QUERY_TAG_KEY.equals(tag.getKey())
          && DatabricksSqlHelper.QUERY_TAG_VALUE.equals(tag.getValue())) {
        return true;
      }
    }
    return false;
  }
}
