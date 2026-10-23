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
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes SQL statements on a running all-purpose cluster with the Command Execution API 1.2, so
 * that no SQL warehouse is needed. The statements run one at a time in one SQL execution context,
 * which {@link #close()} destroys. The API has no catalog parameter, so the catalog of a statement
 * is set with {@code USE CATALOG} when it changes. A statement fails with an {@link IOException}
 * when the cluster returns an error or a truncated result.
 */
@ThreadSafe
public final class ClusterSqlRunner implements DatabricksSqlRunner {

  private static final Logger logger = LoggerFactory.getLogger(ClusterSqlRunner.class);
  private static final Duration DEFAULT_COMMAND_TIMEOUT = Duration.ofMinutes(30);
  // The cause of a failed command can be a long stack trace.
  private static final int MAX_CAUSE_LENGTH = 2_000;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final CommandExecutionAPI commands;
  private final String clusterId;
  private final Duration commandTimeout;
  private final Object lock = new Object();

  @GuardedBy("lock")
  private String contextId;

  @GuardedBy("lock")
  private String currentCatalog;

  public ClusterSqlRunner(WorkspaceClient client, String clusterId) {
    this(client, clusterId, DEFAULT_COMMAND_TIMEOUT);
  }

  ClusterSqlRunner(WorkspaceClient client, String clusterId, Duration commandTimeout) {
    Preconditions.checkNotNull(client, "WorkspaceClient was null.");
    Preconditions.checkNotNull(clusterId, "Cluster ID was null.");
    this.commands = client.commandExecution();
    this.clusterId = clusterId;
    this.commandTimeout = commandTimeout;
  }

  @Override
  public StatementResult executeSqlInCatalog(String catalog, String sql) throws IOException {
    // An execution context runs one command at a time.
    synchronized (this.lock) {
      if (!catalog.equals(this.currentCatalog)) {
        execute("USE CATALOG " + quoteIdentifier(catalog));
        this.currentCatalog = catalog;
      }

      return toStatementResult(execute(sql));
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

  @GuardedBy("lock")
  private Results execute(String sql) throws IOException {
    String context = findContextId();
    Wait<CommandStatusResponse, Created> command =
        this.commands.execute(
            new Command()
                .setClusterId(this.clusterId)
                .setContextId(context)
                .setLanguage(Language.SQL)
                .setCommand(sql));
    CommandStatusResponse response;
    try {
      response = command.get(this.commandTimeout);
    } catch (TimeoutException e) {
      cancelQuietly(context, command.getResponse());
      throw new IOException(
          String.format(
              "Timed out after %s waiting for SQL command on cluster %s: %s",
              this.commandTimeout, this.clusterId, sql),
          e);
    } catch (IllegalStateException e) {
      // The SDK throws it when the command is cancelled.
      throw new IOException(
          String.format(
              "SQL command on cluster %s failed: %s: %s", this.clusterId, e.getMessage(), sql),
          e);
    }

    Results results = response.getResults();
    if (response.getStatus() == CommandStatus.ERROR
        || results == null
        || results.getResultType() == ResultType.ERROR) {
      throw new IOException(
          String.format(
              "SQL command on cluster %s failed in status %s: %s",
              this.clusterId, response.getStatus(), describeError(results)));
    }

    if (Boolean.TRUE.equals(results.getTruncated())) {
      throw new IOException(
          String.format(
              "Cluster %s truncated the result of the SQL command, which exceeds the result size"
                  + " limit of the cluster: %s",
              this.clusterId, sql));
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

  /** Returns the summary and the beginning of the cause of a failed command. */
  private static String describeError(Results results) {
    if (results == null) {
      return "no result";
    }

    String cause = Strings.nullToEmpty(results.getCause());
    if (cause.length() > MAX_CAUSE_LENGTH) {
      cause = cause.substring(0, MAX_CAUSE_LENGTH) + "...";
    }
    return Strings.nullToEmpty(results.getSummary()) + " " + cause;
  }

  /**
   * Returns the rows of a table result, with each cell as text: a string as it is, a number or a
   * boolean with {@code toString()}, and an array or a map as JSON, as a SQL warehouse prints them.
   * A result that isn't a table, such as the result of {@code USE CATALOG}, has no rows.
   */
  private static StatementResult toStatementResult(Results results) throws IOException {
    if (results.getResultType() != ResultType.TABLE) {
      return new ListStatementResult(ImmutableList.of(), ImmutableList.of());
    }

    ImmutableList.Builder<String> columnNames = ImmutableList.builder();
    if (results.getSchema() != null) {
      for (Map<String, Object> column : results.getSchema()) {
        columnNames.add(String.valueOf(column.get("name")));
      }
    }

    ImmutableList.Builder<List<String>> rows = ImmutableList.builder();
    if (results.getData() instanceof List) {
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
    }

    return new ListStatementResult(columnNames.build(), rows.build());
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
