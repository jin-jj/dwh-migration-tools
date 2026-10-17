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

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksClientSqlRunner.QUERY_TAG_KEY;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksClientSqlRunner.QUERY_TAG_VALUE;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksClientSqlRunner.SqlQuoting.quoteStringLiteral;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksRestApi.executeJson;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlRunner.StatementResult.cell;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.service.sql.ListQueryHistoryRequest;
import com.databricks.sdk.service.sql.QueryFilter;
import com.databricks.sdk.service.sql.QueryStatus;
import com.databricks.sdk.service.sql.TimeRange;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.ByteSink;
import com.google.common.primitives.Longs;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksClientSqlRunner.SqlQuoting;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlRunner;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlRunner.StatementResult;
import com.google.edwmigration.dumper.application.dumper.handle.AbstractHandle;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksLogsDumpFormat.Header;
import java.io.IOException;
import java.io.Writer;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.csv.CSVPrinter;

/**
 * Dumps the Databricks queries that started in one interval to one CSV zip entry, from
 * system.query.history if the {@link DatabricksLogsHandle} has a {@link DatabricksSqlRunner}, or
 * else from the query history API.
 */
public final class QueryHistoryTask extends AbstractTask<Void> {

  public static final String REDACTED_STATEMENT_TEXT = "<REDACTED>";
  private static final String SYSTEM_CATALOG = "system";
  private static final String WAREHOUSE_COMPUTE_TYPE = "WAREHOUSE";
  // The expression of each Header in system.query.history, in the order of Header. A new column
  // goes at the end of both Header and this map, so that the columns of earlier dumps keep their
  // positions.
  private static final ImmutableMap<Header, String> SYSTEM_TABLE_COLUMNS =
      ImmutableMap.<Header, String>builder()
          .put(Header.AccountId, "account_id")
          .put(Header.WorkspaceId, "workspace_id")
          .put(Header.StatementId, "statement_id")
          .put(Header.SessionId, "session_id")
          .put(Header.ExecutedBy, "executed_by")
          .put(Header.StatementText, "statement_text")
          .put(Header.StatementType, "statement_type")
          .put(Header.ExecutionStatus, "execution_status")
          .put(Header.ErrorMessage, "error_message")
          .put(Header.WarehouseId, "compute.warehouse_id")
          .put(Header.ComputeType, "compute.type")
          .put(Header.StartTime, "unix_millis(start_time)")
          .put(Header.EndTime, "unix_millis(end_time)")
          .put(Header.TotalDurationMs, "total_duration_ms")
          .put(Header.CompilationDurationMs, "compilation_duration_ms")
          .put(Header.ExecutionDurationMs, "execution_duration_ms")
          .put(Header.WaitingAtCapacityDurationMs, "waiting_at_capacity_duration_ms")
          .put(Header.ReadBytes, "read_bytes")
          .put(Header.ReadRows, "read_rows")
          .put(Header.ProducedRows, "produced_rows")
          .put(Header.WrittenBytes, "written_bytes")
          .put(Header.WrittenRows, "written_rows")
          .put(Header.SpilledLocalBytes, "spilled_local_bytes")
          .put(Header.ClientApplication, "client_application")
          .build();
  private static final String SYSTEM_TABLE_SELECT_LIST = createSystemTableSelectList();
  // Selects the queries that started in [start, end), as the Teradata and Redshift logs connectors
  // do, so that each query is in exactly one file, and skips the statements of the dumper.
  private static final String SYSTEM_TABLE_SQL =
      "SELECT %s FROM system.query.history"
          + " WHERE start_time >= timestamp_millis(%s) AND start_time < timestamp_millis(%s)"
          + " AND COALESCE(try_element_at(query_tags, %s), '') <> %s";
  // A record is an Object[] indexed by Header.ordinal(), which is the column order that
  // FORMAT.withHeader(Header.class) writes, so each value is printed under its column.
  private static final int RECORD_SIZE = Header.values().length;
  private static final long QUERY_HISTORY_PAGE_SIZE = 100L;
  private static final String QUERY_HISTORY_PATH = "/api/2.0/sql/history/queries";
  private static final ImmutableList<QueryStatus> ENDED_QUERY_STATUSES =
      ImmutableList.of(QueryStatus.FINISHED, QueryStatus.FAILED, QueryStatus.CANCELED);

  /**
   * Thread-safe shared {@code Handle} for the Databricks logs connector. Without a {@link
   * DatabricksSqlRunner}, the query history API is read instead of system.query.history.
   */
  @ThreadSafe
  public static final class DatabricksLogsHandle extends AbstractHandle {

    @Nullable private final DatabricksSqlRunner sqlRunner;
    private final WorkspaceClient client;
    @Nullable private final String urlWorkspaceId;
    private final ImmutableList<String> workspaceIds;

    public DatabricksLogsHandle(
        @Nullable DatabricksSqlRunner sqlRunner,
        WorkspaceClient client,
        @Nullable String urlWorkspaceId,
        ImmutableList<String> workspaceIds) {
      this.sqlRunner = sqlRunner;
      this.client = Preconditions.checkNotNull(client, "WorkspaceClient was null.");
      this.urlWorkspaceId = urlWorkspaceId;
      this.workspaceIds = Preconditions.checkNotNull(workspaceIds, "Workspace IDs were null.");
    }

    /** Returns the runner that reads system.query.history, or null to read the API instead. */
    @Nullable
    public DatabricksSqlRunner sqlRunner() {
      return this.sqlRunner;
    }

    public ImmutableList<String> workspaceIds() {
      return this.workspaceIds;
    }

    @Override
    public void close() throws IOException {
      if (this.sqlRunner != null) {
        this.sqlRunner.close();
      }
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
    if (logsHandle.sqlRunner == null) {
      dumpFromApi(sink, logsHandle);
    } else {
      dumpFromSystemTable(sink, logsHandle);
    }

    return null;
  }

  /**
   * Returns the statement text of one query of the range that has a statement text, or null if no
   * query has one. Fails if system.query.history can't be read.
   */
  @Nullable
  public static String probeStatementText(
      DatabricksSqlRunner sqlRunner, ZonedInterval range, ImmutableList<String> workspaceIds)
      throws IOException {
    String sql = systemTableSql(range, workspaceIds) + " AND statement_text <> '' LIMIT 1";
    try (StatementResult rows = sqlRunner.executeSqlInCatalog(SYSTEM_CATALOG, sql)) {
      return cell(rows.nextRow(), Header.StatementText.ordinal());
    }
  }

  private void dumpFromSystemTable(ByteSink sink, DatabricksLogsHandle handle) throws IOException {
    String sql = systemTableSql(this.interval, handle.workspaceIds);
    try (StatementResult rows = handle.sqlRunner.executeSqlInCatalog(SYSTEM_CATALOG, sql);
        Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
        CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer)) {
      for (List<String> row = rows.nextRow(); row != null; row = rows.nextRow()) {
        printer.printRecord(systemTableRecord(row));
      }
    }
  }

  // Each page is written before the next one is read. The API may send a page token with the last
  // page, so has_next_page decides whether there is a next page. The time range of the API includes
  // its end, so the range ends at the last millisecond of the interval. The queries of the dumper
  // are tagged, and skipped. The request is the one of QueryHistoryAPI.list, but the response is
  // read as JSON, because the SDK models drop fields, such as metrics.write_remote_rows, and read
  // the enum values that they don't know, such as a new statement_type, as null.
  private void dumpFromApi(ByteSink sink, DatabricksLogsHandle handle) throws IOException {
    ListQueryHistoryRequest listRequest =
        new ListQueryHistoryRequest()
            .setFilterBy(
                new QueryFilter()
                    .setQueryStartTimeRange(
                        new TimeRange()
                            .setStartTimeMs(toEpochMilli(this.interval.getStart()))
                            .setEndTimeMs(toEpochMilli(this.interval.getEndInclusive())))
                    .setStatuses(ENDED_QUERY_STATUSES))
            .setIncludeMetrics(true)
            .setMaxResults(QUERY_HISTORY_PAGE_SIZE);
    try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
        CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer)) {
      String pageToken = null;
      do {
        String previousPageToken = pageToken;
        Request request = new Request(Request.GET, QUERY_HISTORY_PATH);
        ApiClient.setQuery(request, listRequest.setPageToken(pageToken));
        JsonNode page = executeJson(handle.client, request);
        for (JsonNode query : page.path("res")) {
          if (!hasDumperTag(query)) {
            printer.printRecord(apiRecord(query, handle.urlWorkspaceId));
          }
        }

        JsonNode hasNextPage = page.path("has_next_page");
        pageToken =
            hasNextPage.isBoolean() && !hasNextPage.booleanValue()
                ? null
                : text(page, "next_page_token");
        if (pageToken != null && pageToken.equals(previousPageToken)) {
          throw new IOException("Databricks returned the page token '" + pageToken + "' twice.");
        }
      } while (!Strings.isNullOrEmpty(pageToken));
    }
  }

  static String systemTableSql(ZonedInterval interval, ImmutableList<String> workspaceIds) {
    String sql =
        String.format(
            SYSTEM_TABLE_SQL,
            SYSTEM_TABLE_SELECT_LIST,
            toEpochMilli(interval.getStart()),
            toEpochMilli(interval.getEndExclusive()),
            quoteStringLiteral(QUERY_TAG_KEY),
            quoteStringLiteral(QUERY_TAG_VALUE));
    if (workspaceIds.isEmpty()) {
      return sql;
    }

    return String.format(
        "%s AND workspace_id IN (%s)",
        sql, Joiner.on(", ").join(Lists.transform(workspaceIds, SqlQuoting::quoteStringLiteral)));
  }

  /**
   * Returns the expressions of {@link #SYSTEM_TABLE_COLUMNS} in the order of Header, which is the
   * order of the CSV columns, so that each value of a row is written under its header.
   */
  private static String createSystemTableSelectList() {
    List<String> expressions = new ArrayList<>();
    for (Header header : Header.values()) {
      String expression = SYSTEM_TABLE_COLUMNS.get(header);
      Preconditions.checkState(
          expression != null, "No system.query.history column for the header %s.", header);
      expressions.add(expression);
    }

    return String.join(", ", expressions);
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

  // A query of the query history API. The workspace_id of a query, which the SDK QueryInfo doesn't
  // have, is the --url workspace when the API doesn't send it.
  private static Object[] apiRecord(JsonNode query, @Nullable String urlWorkspaceId) {
    JsonNode metrics = query.path("metrics");
    String warehouseId = text(query, "warehouse_id");
    // CSVPrinter prints null values as empty and an empty first value as "".
    Object[] record = new Object[RECORD_SIZE];
    record[Header.AccountId.ordinal()] = "";
    record[Header.WorkspaceId.ordinal()] = coalesce(text(query, "workspace_id"), urlWorkspaceId);
    record[Header.StatementId.ordinal()] = text(query, "query_id");
    record[Header.SessionId.ordinal()] = text(query, "session_id");
    record[Header.ExecutedBy.ordinal()] =
        coalesce(text(query, "user_name"), text(query, "executed_as_user_name"));
    record[Header.StatementText.ordinal()] = text(query, "query_text");
    record[Header.StatementType.ordinal()] = text(query, "statement_type");
    record[Header.ExecutionStatus.ordinal()] = text(query, "status");
    record[Header.ErrorMessage.ordinal()] = text(query, "error_message");
    record[Header.WarehouseId.ordinal()] = warehouseId;
    record[Header.ComputeType.ordinal()] = warehouseId == null ? "" : WAREHOUSE_COMPUTE_TYPE;
    record[Header.StartTime.ordinal()] = formatTime(epochMillis(query, "query_start_time_ms"));
    record[Header.EndTime.ordinal()] =
        formatTime(
            coalesce(
                epochMillis(query, "query_end_time_ms"),
                epochMillis(query, "execution_end_time_ms")));
    record[Header.TotalDurationMs.ordinal()] =
        coalesce(text(metrics, "total_time_ms"), text(query, "duration"));
    record[Header.CompilationDurationMs.ordinal()] = text(metrics, "compilation_time_ms");
    record[Header.ExecutionDurationMs.ordinal()] = text(metrics, "execution_time_ms");
    record[Header.ReadBytes.ordinal()] = text(metrics, "read_bytes");
    record[Header.ReadRows.ordinal()] = text(metrics, "rows_read_count");
    record[Header.ProducedRows.ordinal()] =
        coalesce(text(metrics, "rows_produced_count"), text(query, "rows_produced"));
    record[Header.WrittenBytes.ordinal()] = text(metrics, "write_remote_bytes");
    record[Header.WrittenRows.ordinal()] = text(metrics, "write_remote_rows");
    record[Header.SpilledLocalBytes.ordinal()] = text(metrics, "spill_to_disk_bytes");
    record[Header.ClientApplication.ordinal()] = text(query, "client_application");
    return record;
  }

  /** Returns whether a query of the query history API has the query tag of the dumper. */
  private static boolean hasDumperTag(JsonNode query) {
    for (JsonNode tag : query.path("query_tags")) {
      if (QUERY_TAG_KEY.equals(text(tag, "key")) && QUERY_TAG_VALUE.equals(text(tag, "value"))) {
        return true;
      }
    }

    return false;
  }

  /** Returns the value of a field of a JSON object as text, or null if it's missing or null. */
  @Nullable
  private static String text(JsonNode node, String field) {
    JsonNode value = node.path(field);
    return value.isValueNode() && !value.isNull() ? value.asText() : null;
  }

  /** Returns the epoch milliseconds of a field of a JSON object, or null if it has none. */
  @Nullable
  private static Long epochMillis(JsonNode node, String field) {
    JsonNode value = node.path(field);
    return value.isIntegralNumber() ? Long.valueOf(value.asLong()) : Longs.tryParse(value.asText());
  }

  @Nullable
  private static <T> T coalesce(@Nullable T first, @Nullable T second) {
    return first != null ? first : second;
  }

  private static String formatTime(@Nullable Long epochMillis) {
    return epochMillis == null ? "" : Instant.ofEpochMilli(epochMillis).toString();
  }
}
