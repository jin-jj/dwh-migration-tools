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

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksLogsConnector.DatabricksLogsProperty.STRATEGY;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksLogsConnector.DatabricksLogsProperty.WHERE;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksLogsConnector.DatabricksLogsProperty.WORKSPACE_ID;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.QUERY_TAG_KEY;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.QUERY_TAG_VALUE;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.cell;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.quoteStringLiteral;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.formatEnum;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.formatNumber;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.formatTime;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.databricks.sdk.service.sql.QueryInfo;
import com.databricks.sdk.service.sql.QueryMetrics;
import com.databricks.sdk.service.sql.QueryTag;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksLogsConnector.Strategy;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksLogsDumpFormat.Header;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tasks that dump the Databricks query history, one task for each interval.
 *
 * <p>The query history is read from {@code system.query.history} on the SQL warehouse, or from the
 * query history API, which has the queries of the last 30 days of the {@code --url} workspace and
 * needs no SQL warehouse. With the default strategy, the first task checks once, for all the tasks,
 * whether {@code system.query.history} can be read and has the statement text, and the API is read
 * instead if it can't be read or has no statement text. So all the files come from the same source.
 *
 * <p>Both sources select the queries by their start time, because the API can filter the queries
 * only by their start time, so an interval has the same queries in both.
 */
final class DatabricksQueryHistoryTasks {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksQueryHistoryTasks.class);

  /** The catalog of the statements, which has the {@code system.query.history} table. */
  private static final String SYSTEM_CATALOG = "system";

  /** The statement text in system.query.history for a user who may not read the text. */
  static final String REDACTED_STATEMENT_TEXT = "<REDACTED>";

  /** The placeholder of a custom query for the start, and then the end, of an interval. */
  static final String PLACEHOLDER = "%s";

  /** The compute type of the queries of a SQL warehouse, as in system.query.history. */
  private static final String WAREHOUSE_COMPUTE_TYPE = "WAREHOUSE";

  /**
   * The columns of system.query.history, in the order of {@link Header}. The times are selected as
   * epoch milliseconds and written as instants, as the times of the query history API.
   */
  static final ImmutableList<String> SYSTEM_TABLE_COLUMNS =
      ImmutableList.of(
          "account_id",
          "workspace_id",
          "statement_id",
          "session_id",
          "executed_by",
          "statement_text",
          "statement_type",
          "execution_status",
          "error_message",
          "compute.warehouse_id",
          "compute.type",
          "unix_millis(start_time)",
          "unix_millis(end_time)",
          "total_duration_ms",
          "compilation_duration_ms",
          "execution_duration_ms",
          "waiting_at_capacity_duration_ms",
          "read_bytes",
          "read_rows",
          "produced_rows",
          "written_bytes",
          "written_rows",
          "spilled_local_bytes",
          "client_application");

  private static final int HEADER_SIZE = Header.values().length;
  private static final int STATEMENT_TEXT_INDEX = Header.StatementText.ordinal();
  private static final int START_TIME_INDEX = Header.StartTime.ordinal();
  private static final int END_TIME_INDEX = Header.EndTime.ordinal();

  private DatabricksQueryHistoryTasks() {}

  /** Where the query history is read from. */
  enum Source {
    SYSTEM_TABLE,
    QUERY_HISTORY_API
  }

  /**
   * The state of the query history tasks of one run: the options, the range of all the intervals,
   * and the source of the query history, which the first task chooses for all the tasks.
   */
  @ThreadSafe
  static final class QueryHistorySession {

    private final Strategy strategy;
    @CheckForNull private final String customQuery;
    @CheckForNull private final String condition;
    private final ZonedInterval range;
    private final Memoized<Source> source =
        new Memoized<>("Choosing the source of the query history");

    /**
     * @param customQuery the query of {@code -Ddatabricks.logs.query}, or null
     * @param condition the condition of {@code -Ddatabricks.logs.where}, or null
     * @param range the range of all the intervals
     */
    QueryHistorySession(
        @Nonnull Strategy strategy,
        @CheckForNull String customQuery,
        @CheckForNull String condition,
        @Nonnull ZonedInterval range) {
      this.strategy = Preconditions.checkNotNull(strategy, "Strategy was null.");
      this.customQuery = customQuery;
      this.condition = condition;
      this.range = Preconditions.checkNotNull(range, "Range was null.");
    }

    /** Returns the source of the query history, which is chosen once, by the first task. */
    @Nonnull
    Source source(@Nonnull DatabricksLogsHandle handle) throws IOException {
      return source.load(() -> chooseSource(handle));
    }

    /** Returns the query of system.query.history for the queries that started in the interval. */
    @Nonnull
    String query(@Nonnull ZonedInterval interval, @Nonnull DatabricksLogsHandle handle) {
      if (customQuery != null) {
        return customQuery(customQuery, interval);
      }

      return systemTableQuery(interval, handle.workspaceIds(), condition);
    }

    @Nonnull
    private Source chooseSource(@Nonnull DatabricksLogsHandle handle) throws IOException {
      if (strategy == Strategy.REST_ONLY) {
        logger.info(
            "Reading the query history API, which has the queries of the last 30 days of the"
                + " --url workspace.");
        return Source.QUERY_HISTORY_API;
      }

      if (customQuery != null) {
        return Source.SYSTEM_TABLE;
      }

      String reason = findReasonApiCannotBeRead(handle);
      String statementText;
      try (CloseableIterable<List<String>> rows =
          handle
              .sqlHelper()
              .executeSqlInCatalog(
                  SYSTEM_CATALOG, probeQuery(range, handle.workspaceIds(), condition))) {
        Iterator<List<String>> iterator = rows.iterator();
        statementText = iterator.hasNext() ? cell(iterator.next(), STATEMENT_TEXT_INDEX) : null;
      } catch (IOException | UncheckedIOException e) {
        if (Thread.currentThread().isInterrupted()) {
          throw e;
        }

        if (reason != null) {
          throw new IOException(
              String.format(
                  "Failed to read system.query.history, and the query history API can't be read"
                      + " instead, because %s. Cause: %s",
                  reason, e.getMessage()),
              e);
        }

        logger.warn(
            "Failed to read system.query.history, so the query history API is read instead. It"
                + " has the queries of the last 30 days of the --url workspace. Cause: {}",
            e.getMessage());
        return Source.QUERY_HISTORY_API;
      }

      if (!REDACTED_STATEMENT_TEXT.equals(statementText)) {
        logger.info("Reading system.query.history.");
        return Source.SYSTEM_TABLE;
      }

      if (reason != null) {
        logger.warn(
            "The statement text of system.query.history is {} unless the user is an account admin"
                + " or a member of the databricks_pii_access group. The queries are dumped"
                + " without their text, because the query history API can't be read instead: {}.",
            REDACTED_STATEMENT_TEXT,
            reason);
        return Source.SYSTEM_TABLE;
      }

      logger.warn(
          "The statement text of system.query.history is {} unless the user is an account admin or"
              + " a member of the databricks_pii_access group, so the query history API is read"
              + " instead. It has the queries of the last 30 days of the --url workspace.",
          REDACTED_STATEMENT_TEXT);
      return Source.QUERY_HISTORY_API;
    }

    /**
     * Returns why the query history API can't be read instead of system.query.history, or null if
     * it can. The API has only the queries of the --url workspace and can't apply a condition.
     */
    @CheckForNull
    private String findReasonApiCannotBeRead(@Nonnull DatabricksLogsHandle handle) {
      if (strategy == Strategy.SYSTEM_ONLY) {
        return String.format("-D%s=%s is set", STRATEGY.getName(), Strategy.SYSTEM_ONLY);
      }

      if (condition != null) {
        return String.format("it can't apply -D%s", WHERE.getName());
      }

      String urlWorkspaceId = handle.urlWorkspaceId();
      if (urlWorkspaceId == null) {
        return "the ID of the --url workspace isn't known";
      }

      if (!handle.workspaceIds().equals(ImmutableList.of(urlWorkspaceId))) {
        return String.format(
            "it has only the queries of the --url workspace %s, but -D%s asks for %s",
            urlWorkspaceId,
            WORKSPACE_ID.getName(),
            handle.workspaceIds().isEmpty()
                ? "every workspace in the region"
                : String.join(", ", handle.workspaceIds()));
      }

      return null;
    }
  }

  /** Dumps the queries that started in one interval to one file. */
  static final class DatabricksQueryHistoryTask extends AbstractTask<Void> {

    private final ZonedInterval interval;
    private final QueryHistorySession session;

    DatabricksQueryHistoryTask(
        @Nonnull String targetPath,
        @Nonnull ZonedInterval interval,
        @Nonnull QueryHistorySession session) {
      super(targetPath);
      this.interval = Preconditions.checkNotNull(interval, "Interval was null.");
      this.session = Preconditions.checkNotNull(session, "Query history session was null.");
    }

    @Override
    @Nonnull
    public TaskCategory getCategory() {
      return TaskCategory.REQUIRED;
    }

    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      DatabricksLogsHandle logsHandle = (DatabricksLogsHandle) handle;
      if (session.source(logsHandle) == Source.QUERY_HISTORY_API) {
        writeApiQueries(sink, logsHandle);
      } else {
        writeSystemTableQueries(sink, logsHandle);
      }
      return null;
    }

    private void writeSystemTableQueries(
        @Nonnull ByteSink sink, @Nonnull DatabricksLogsHandle handle) throws IOException {
      String sql = session.query(interval, handle);
      try (CloseableIterable<List<String>> rows =
              handle.sqlHelper().executeSqlInCatalog(SYSTEM_CATALOG, sql);
          Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(Header.class).print(writer)) {
        for (List<String> row : rows) {
          csv.printRecord(systemTableRecord(row));
        }
      }
    }

    private void writeApiQueries(@Nonnull ByteSink sink, @Nonnull DatabricksLogsHandle handle)
        throws IOException {
      String workspaceId = handle.urlWorkspaceId();
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(Header.class).print(writer)) {
        handle
            .restHelper()
            .listQueryHistory(
                toEpochMilli(interval.getStart()),
                toEpochMilli(interval.getEndExclusive()),
                queries -> {
                  for (QueryInfo query : queries) {
                    if (!isDumperQuery(query)) {
                      csv.printRecord(apiRecord(query, workspaceId));
                    }
                  }
                });
      }
    }
  }

  /**
   * Returns the query of system.query.history for the queries that started in the interval and
   * aren't statements of the dumper.
   *
   * @param workspaceIds the workspaces of the queries, or an empty list for every workspace
   * @param condition a condition that the queries must also meet, or null
   */
  @Nonnull
  static String systemTableQuery(
      @Nonnull ZonedInterval interval,
      @Nonnull ImmutableList<String> workspaceIds,
      @CheckForNull String condition) {
    StringBuilder sql =
        new StringBuilder("SELECT ")
            .append(String.join(", ", SYSTEM_TABLE_COLUMNS))
            .append(" FROM system.query.history WHERE start_time >= timestamp_millis(")
            .append(toEpochMilli(interval.getStart()))
            .append(") AND start_time < timestamp_millis(")
            .append(toEpochMilli(interval.getEndExclusive()))
            .append(") AND COALESCE(try_element_at(query_tags, ")
            .append(quoteStringLiteral(QUERY_TAG_KEY))
            .append("), '') <> ")
            .append(quoteStringLiteral(QUERY_TAG_VALUE));
    if (!workspaceIds.isEmpty()) {
      sql.append(" AND workspace_id IN (");
      for (int i = 0; i < workspaceIds.size(); i++) {
        if (i > 0) {
          sql.append(", ");
        }
        sql.append(quoteStringLiteral(workspaceIds.get(i)));
      }
      sql.append(")");
    }

    if (condition != null) {
      sql.append(" AND (").append(condition).append(")");
    }

    return sql.toString();
  }

  /**
   * Returns the query of one query in the range that has a statement text, which shows whether
   * system.query.history can be read and has the statement text.
   */
  @Nonnull
  static String probeQuery(
      @Nonnull ZonedInterval range,
      @Nonnull ImmutableList<String> workspaceIds,
      @CheckForNull String condition) {
    return systemTableQuery(range, workspaceIds, condition) + " AND statement_text <> '' LIMIT 1";
  }

  /**
   * Returns a custom query with its two placeholders replaced with the start and the end of the
   * interval, such as {@code 2026-10-09T13:00:00Z}.
   */
  @Nonnull
  static String customQuery(@Nonnull String query, @Nonnull ZonedInterval interval) {
    String withStart = replaceFirstPlaceholder(query, interval.getStart().toInstant().toString());
    return replaceFirstPlaceholder(withStart, interval.getEndExclusive().toInstant().toString());
  }

  @Nonnull
  private static String replaceFirstPlaceholder(@Nonnull String query, @Nonnull String value) {
    int index = query.indexOf(PLACEHOLDER);
    return query.substring(0, index) + value + query.substring(index + PLACEHOLDER.length());
  }

  /** Returns the record of a row of system.query.history, with its times as instants. */
  @Nonnull
  static ImmutableList<String> systemTableRecord(@Nonnull List<String> row) {
    ImmutableList.Builder<String> record = ImmutableList.builder();
    for (int index = 0; index < HEADER_SIZE; index++) {
      String value = Strings.nullToEmpty(cell(row, index));
      if (index == START_TIME_INDEX || index == END_TIME_INDEX) {
        value = formatEpochMillis(value);
      }
      record.add(value);
    }
    return record.build();
  }

  /**
   * Returns epoch milliseconds as an instant. Other values, such as the times of a custom query,
   * are returned as they are.
   */
  @Nonnull
  private static String formatEpochMillis(@Nonnull String value) {
    if (value.isEmpty()) {
      return value;
    }

    try {
      return Instant.ofEpochMilli(Long.parseLong(value)).toString();
    } catch (NumberFormatException e) {
      return value;
    }
  }

  /**
   * Returns the record of a query of the query history API.
   *
   * @param workspaceId the ID of the --url workspace, or null if it isn't known
   */
  @Nonnull
  static ImmutableList<String> apiRecord(
      @Nonnull QueryInfo query, @CheckForNull String workspaceId) {
    QueryMetrics metrics = query.getMetrics() != null ? query.getMetrics() : new QueryMetrics();
    String warehouseId =
        query.getWarehouseId() != null ? query.getWarehouseId() : query.getEndpointId();
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
    return ImmutableList.of(
        // AccountId isn't in the API.
        "",
        Strings.nullToEmpty(workspaceId),
        Strings.nullToEmpty(query.getQueryId()),
        Strings.nullToEmpty(query.getSessionId()),
        Strings.nullToEmpty(executedBy),
        Strings.nullToEmpty(query.getQueryText()),
        formatEnum(query.getStatementType()),
        formatEnum(query.getStatus()),
        Strings.nullToEmpty(query.getErrorMessage()),
        Strings.nullToEmpty(warehouseId),
        warehouseId == null ? "" : WAREHOUSE_COMPUTE_TYPE,
        formatTime(query.getQueryStartTimeMs()),
        formatTime(endTime),
        formatNumber(totalDuration),
        formatNumber(metrics.getCompilationTimeMs()),
        formatNumber(metrics.getExecutionTimeMs()),
        // WaitingAtCapacityDurationMs isn't in the API.
        "",
        formatNumber(metrics.getReadBytes()),
        formatNumber(metrics.getRowsReadCount()),
        formatNumber(producedRows),
        formatNumber(metrics.getWriteRemoteBytes()),
        // WrittenRows isn't in the API.
        "",
        formatNumber(metrics.getSpillToDiskBytes()),
        Strings.nullToEmpty(query.getClientApplication()));
  }

  /** Returns whether a query of the query history API is a statement of the dumper. */
  static boolean isDumperQuery(@Nonnull QueryInfo query) {
    if (query.getQueryTags() == null) {
      return false;
    }

    for (QueryTag tag : query.getQueryTags()) {
      if (tag != null
          && QUERY_TAG_KEY.equals(tag.getKey())
          && QUERY_TAG_VALUE.equals(tag.getValue())) {
        return true;
      }
    }
    return false;
  }

  private static long toEpochMilli(@Nonnull ZonedDateTime dateTime) {
    return dateTime.toInstant().toEpochMilli();
  }
}
