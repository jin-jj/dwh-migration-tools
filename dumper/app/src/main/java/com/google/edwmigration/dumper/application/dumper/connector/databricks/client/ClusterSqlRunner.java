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
package com.google.edwmigration.dumper.application.dumper.connector.databricks.client;

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper.SqlQuoting.quoteIdentifier;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.compute.CancelCommand;
import com.databricks.sdk.service.compute.Command;
import com.databricks.sdk.service.compute.CommandExecutionAPI;
import com.databricks.sdk.service.compute.CommandStatus;
import com.databricks.sdk.service.compute.CommandStatusResponse;
import com.databricks.sdk.service.compute.CreateContext;
import com.databricks.sdk.service.compute.Created;
import com.databricks.sdk.service.compute.DestroyContext;
import com.databricks.sdk.service.compute.Language;
import com.databricks.sdk.service.compute.ResultType;
import com.databricks.sdk.service.compute.Results;
import com.databricks.sdk.support.Wait;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes SQL statements on a running all-purpose cluster with the Command Execution API 1.2, so
 * that no SQL warehouse is needed. The statements run one at a time in one SQL execution context,
 * which {@link #close()} destroys. The API has no catalog parameter, so the catalog of a statement
 * is set with {@code USE CATALOG} when it changes.
 *
 * <p>The API returns at most 1,000 rows of a result. When a result is truncated, a Python command
 * in the same context stores the result of the statement in a temporary view with a row index, and
 * SQL commands read the view in pages of up to 1,000 rows, in the order of the index.
 */
@ThreadSafe
public final class ClusterSqlRunner implements DatabricksSqlRunner {

  private static final Logger logger = LoggerFactory.getLogger(ClusterSqlRunner.class);
  private static final Duration DEFAULT_COMMAND_TIMEOUT = Duration.ofMinutes(30);
  // The API returns at most 1,000 rows of a table result.
  private static final int DEFAULT_PAGE_ROWS = 1_000;
  private static final String RESULT_VIEW = "dwh_dumper_result";
  private static final String ROW_INDEX_COLUMN = "_dwh_dumper_row";
  // The cause of a failed command is a stack trace, colored with ANSI escape sequences.
  private static final Pattern ANSI_ESCAPE_PATTERN = Pattern.compile("\u001B\\[[;\\d]*m");
  private static final Pattern SEPARATOR_LINE_PATTERN = Pattern.compile("-*");
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final CommandExecutionAPI commands;
  private final String clusterId;
  private final Duration commandTimeout;
  private final int pageRows;
  private final Object lock = new Object();

  @GuardedBy("lock")
  private String contextId;

  @GuardedBy("lock")
  private String currentCatalog;

  public ClusterSqlRunner(WorkspaceClient client, String clusterId) {
    this(client, clusterId, DEFAULT_COMMAND_TIMEOUT, DEFAULT_PAGE_ROWS);
  }

  ClusterSqlRunner(
      WorkspaceClient client, String clusterId, Duration commandTimeout, int pageRows) {
    Preconditions.checkNotNull(client, "WorkspaceClient was null.");
    Preconditions.checkNotNull(clusterId, "Cluster ID was null.");
    Preconditions.checkArgument(pageRows > 0, "Page rows must be positive but was %s.", pageRows);
    this.commands = client.commandExecution();
    this.clusterId = clusterId;
    this.commandTimeout = commandTimeout;
    this.pageRows = pageRows;
  }

  @Override
  public StatementResult executeSqlInCatalog(String catalog, String sql) throws IOException {
    // An execution context runs one command at a time.
    synchronized (this.lock) {
      if (!catalog.equals(this.currentCatalog)) {
        execute(Language.SQL, "USE CATALOG " + quoteIdentifier(catalog));
        this.currentCatalog = catalog;
      }

      Results results = execute(Language.SQL, sql);
      if (Boolean.TRUE.equals(results.getTruncated())) {
        return readInPages(sql);
      }

      return toStatementResult(results);
    }
  }

  /** Destroys the execution context, if any, ignoring failures. */
  @Override
  public void close() {
    synchronized (this.lock) {
      if (this.contextId == null) {
        return;
      }

      try {
        this.commands.destroy(
            new DestroyContext().setClusterId(this.clusterId).setContextId(this.contextId));
      } catch (RuntimeException e) {
        logger.debug(
            "Ignored error destroying execution context {} of cluster {}: {}",
            this.contextId,
            this.clusterId,
            e.getMessage());
      }
      this.contextId = null;
    }
  }

  /**
   * Reads the whole result of {@code sql}, whose first result was truncated. The rows are copied to
   * the driver before the index is added, so that the pages read a fixed result.
   */
  @GuardedBy("lock")
  private StatementResult readInPages(String sql) throws IOException {
    execute(
        Language.PYTHON,
        String.format(
            "from pyspark.sql.functions import monotonically_increasing_id\n"
                + "_dwh_dumper_df = spark.sql(%s)\n"
                + "spark.createDataFrame(_dwh_dumper_df.collect(), _dwh_dumper_df.schema)"
                + ".withColumn('%s', monotonically_increasing_id())"
                + ".createOrReplaceTempView('%s')",
            MAPPER.writeValueAsString(sql), ROW_INDEX_COLUMN, RESULT_VIEW));
    try {
      ImmutableList<String> columnNames = ImmutableList.of();
      ImmutableList.Builder<List<String>> rows = ImmutableList.builder();
      long offset = 0;
      while (true) {
        Results page =
            execute(
                Language.SQL,
                String.format(
                    "SELECT * EXCEPT (%s) FROM %s ORDER BY %s LIMIT %d OFFSET %d",
                    ROW_INDEX_COLUMN, RESULT_VIEW, ROW_INDEX_COLUMN, this.pageRows, offset));
        if (Boolean.TRUE.equals(page.getTruncated())) {
          throw new IOException(
              String.format(
                  "Cluster %s truncated a page of %d rows of the result of the SQL command: %s",
                  this.clusterId, this.pageRows, sql));
        }

        columnNames = findColumnNames(page);
        List<List<String>> rowsOfPage = findRows(page);
        rows.addAll(rowsOfPage);
        if (rowsOfPage.size() < this.pageRows) {
          logger.debug("Read {} rows in pages of: {}", offset + rowsOfPage.size(), sql);
          return new ListStatementResult(columnNames, rows.build());
        }

        offset += rowsOfPage.size();
      }
    } finally {
      executeQuietly("DROP VIEW IF EXISTS " + RESULT_VIEW);
    }
  }

  @GuardedBy("lock")
  private void executeQuietly(String sql) {
    try {
      execute(Language.SQL, sql);
    } catch (IOException | RuntimeException e) {
      logger.debug(
          "Ignored error of SQL command on cluster {}: {}", this.clusterId, e.getMessage());
    }
  }

  /** Runs a command and returns its results, which can be truncated. */
  @GuardedBy("lock")
  private Results execute(Language language, String code) throws IOException {
    String context = findContextId();
    Wait<CommandStatusResponse, Created> command =
        this.commands.execute(
            new Command()
                .setClusterId(this.clusterId)
                .setContextId(context)
                .setLanguage(language)
                .setCommand(code));
    CommandStatusResponse response;
    try {
      response = command.get(this.commandTimeout);
    } catch (TimeoutException e) {
      cancelQuietly(context, command.getResponse());
      throw new IOException(
          String.format(
              "Timed out after %s waiting for %s command on cluster %s: %s",
              this.commandTimeout, language, this.clusterId, code),
          e);
    } catch (IllegalStateException e) {
      // The SDK throws it when the command is cancelled.
      throw new IOException(
          String.format(
              "%s command on cluster %s failed: %s: %s",
              language, this.clusterId, e.getMessage(), code),
          e);
    }

    Results results = response.getResults();
    if (response.getStatus() == CommandStatus.ERROR
        || results == null
        || results.getResultType() == ResultType.ERROR) {
      throw new IOException(
          String.format(
              "%s command on cluster %s failed in status %s: %s",
              language, this.clusterId, response.getStatus(), describeError(results)));
    }

    return results;
  }

  @GuardedBy("lock")
  private String findContextId() throws IOException {
    if (this.contextId == null) {
      try {
        this.contextId =
            this.commands
                .create(new CreateContext().setClusterId(this.clusterId).setLanguage(Language.SQL))
                .get(this.commandTimeout)
                .getId();
      } catch (TimeoutException | IllegalStateException e) {
        throw new IOException(
            String.format(
                "Failed to create a SQL execution context on cluster %s: %s",
                this.clusterId, e.getMessage()),
            e);
      }
    }

    return this.contextId;
  }

  private void cancelQuietly(String context, Created command) {
    if (command == null) {
      return;
    }

    try {
      this.commands.cancel(
          new CancelCommand()
              .setClusterId(this.clusterId)
              .setContextId(context)
              .setCommandId(command.getId()));
    } catch (RuntimeException e) {
      logger.debug("Ignored error cancelling command {}: {}", command.getId(), e.getMessage());
    }
  }

  /**
   * Returns the summary of a failed command, which has the error class of a SQL error, and the
   * first line of its cause, such as {@code java.lang.Exception: Results too large}.
   */
  private static String describeError(Results results) {
    if (results == null) {
      return "no result";
    }

    String summary = Strings.nullToEmpty(results.getSummary()).trim();
    String cause =
        ANSI_ESCAPE_PATTERN.matcher(Strings.nullToEmpty(results.getCause())).replaceAll("");
    for (String line : cause.split("\n")) {
      String trimmed = line.trim();
      if (!SEPARATOR_LINE_PATTERN.matcher(trimmed).matches()) {
        return summary + " (" + trimmed.replaceAll("\\s+", " ") + ")";
      }
    }

    return summary;
  }

  /**
   * Returns the rows of a table result, with each cell as text: a string as it is, a number or a
   * boolean with {@code toString()}, and an array or a map as JSON, as a SQL warehouse prints them.
   * A result that isn't a table, such as the result of {@code USE CATALOG}, has no rows.
   */
  private static StatementResult toStatementResult(Results results) throws IOException {
    return new ListStatementResult(findColumnNames(results), findRows(results));
  }

  private static ImmutableList<String> findColumnNames(Results results) {
    ImmutableList.Builder<String> columnNames = ImmutableList.builder();
    if (results.getResultType() == ResultType.TABLE && results.getSchema() != null) {
      for (Map<String, Object> column : results.getSchema()) {
        columnNames.add(String.valueOf(column.get("name")));
      }
    }

    return columnNames.build();
  }

  private static List<List<String>> findRows(Results results) throws IOException {
    List<List<String>> rows = new ArrayList<>();
    if (results.getResultType() != ResultType.TABLE || !(results.getData() instanceof List)) {
      return rows;
    }

    for (Object row : (List<?>) results.getData()) {
      if (!(row instanceof List)) {
        throw new IOException("Expected a row as a JSON array but got: " + row);
      }

      List<String> cells = new ArrayList<>();
      for (Object cell : (List<?>) row) {
        cells.add(toCellText(cell));
      }
      // The cells can be null, which ImmutableList doesn't allow.
      rows.add(Collections.unmodifiableList(cells));
    }
    return rows;
  }

  private static String toCellText(Object cell) throws JsonProcessingException {
    if (cell == null || cell instanceof String) {
      return (String) cell;
    }

    if (cell instanceof Map || cell instanceof List) {
      return MAPPER.writeValueAsString(cell);
    }

    return cell.toString();
  }

  /** The rows of a command result, which the API returns in one response. */
  private static final class ListStatementResult implements StatementResult {

    private final ImmutableList<String> columnNames;
    private final Iterator<List<String>> rows;

    private ListStatementResult(ImmutableList<String> columnNames, List<List<String>> rows) {
      this.columnNames = columnNames;
      this.rows = rows.iterator();
    }

    @Override
    public ImmutableList<String> columnNames() {
      return this.columnNames;
    }

    @Override
    public List<String> nextRow() {
      return this.rows.hasNext() ? this.rows.next() : null;
    }
  }
}
