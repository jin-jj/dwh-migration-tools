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

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataConnector.JAVA_VERSION_PROPERTY;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataConnector.WAREHOUSE_TRANSPORT_DESCRIPTION;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataConnector.createSqlRunner;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataConnector.createWorkspaceClient;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataConnector.getCluster;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataConnector.getWarehouse;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataConnector.parseProperty;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataConnector.requireUrl;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataConnector.validateComputeAndTransport;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataConnector.validateNotAssessment;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.logs.QueryHistoryTask.REDACTED_STATEMENT_TEXT;
import static com.google.edwmigration.dumper.application.dumper.utils.ArchiveNameUtil.getEntryFileNameWithTimestamp;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.support.Header;
import com.google.auto.service.AutoService;
import com.google.common.base.Ascii;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closeables;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogDays;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogEnd;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentQueryLogStart;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnector;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import com.google.edwmigration.dumper.application.dumper.connector.IntervalExpander;
import com.google.edwmigration.dumper.application.dumper.connector.LogsConnector;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedInterval;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedIntervalIterable;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedIntervalIterableGenerator;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlRunner;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.logs.QueryHistoryTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.logs.QueryHistoryTask.DatabricksLogsHandle;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksLogsDumpFormat;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Databricks logs connector. */
@AutoService(Connector.class)
@Description(
    "Dumps the query history of a Databricks workspace from system.query.history using a SQL"
        + " warehouse or an all-purpose cluster, or from the query history API, as chosen by"
        + " -Ddatabricks.logs.source.")
@RespectsArgumentQueryLogDays
@RespectsArgumentQueryLogStart
@RespectsArgumentQueryLogEnd
@RespectsInput(
    order = 100,
    arg = ConnectorArguments.OPT_URI,
    description = "The Databricks workspace URL (e.g. https://<workspace-host>).")
@RespectsInput(
    order = 200,
    arg = ConnectorArguments.OPT_PASSWORD,
    description =
        "The Databricks personal access token (PAT) or OAuth token. Without it, the Databricks"
            + " SDK default authentication is used, such as the DATABRICKS_TOKEN environment"
            + " variable.")
@RespectsInput(
    order = 300,
    arg = ConnectorArguments.OPT_WAREHOUSE,
    description =
        "The ID of the Databricks SQL warehouse that reads system.query.history, which keeps the"
            + " queries of the last 365 days. Without it or --cluster, the query history API is"
            + " read, which has the queries of the last 30 days of the --url workspace. The API is"
            + " also read if system.query.history can't be read or its statement text is"
            + " <REDACTED>, unless -Ddatabricks.logs.source=system-table or"
            + " -Ddatabricks.logs.workspace-id is set. Queries run with the Databricks JDBC driver"
            + " on Java 11 or above, and with the Statement Execution API on Java 8, unless"
            + " -Ddatabricks.logs.warehouse-transport is set.")
@RespectsInput(
    order = 350,
    arg = ConnectorArguments.OPT_CLUSTER,
    description =
        "The ID of a running Unity Catalog-enabled cluster that reads system.query.history"
            + " instead of a SQL warehouse, such as the cluster of the notebook that runs the"
            + " dumper. Requires running the dumper with Java 11 or above.")
public class DatabricksLogsConnector extends AbstractConnector implements LogsConnector {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksLogsConnector.class);

  public static final String CONNECTOR_NAME = "databricks-logs";

  private static final String CURRENT_USER_PATH = "/api/2.0/preview/scim/v2/Me";
  private static final String WORKSPACE_ID_HEADER = "X-Databricks-Org-Id";
  private static final String READING_API_MESSAGE =
      "Reading the query history API, which has the queries of the last 30 days of the --url"
          + " workspace.";
  // Appended to the usage errors about where the queries are read from.
  private static final String SOURCE_USAGE =
      " For example, '--connector databricks-logs --url https://<workspace-host> --warehouse"
          + " <warehouse-id> --query-log-days 7' reads the queries of the last 7 days from"
          + " system.query.history, which keeps the queries of the last 365 days, or from the query"
          + " history API if system.query.history can't be read. Without '--warehouse' and"
          + " '--cluster', only the query history API is read, which has the queries of the last 30"
          + " days of the --url workspace. '-Ddatabricks.logs.source=system-table' or"
          + " '-Ddatabricks.logs.source=api' reads only one of them.";
  private static final String SYSTEM_TABLE_ACCESS =
      "Check that the user can use the SQL warehouse or cluster, and has USE CATALOG on system,"
          + " USE SCHEMA on system.query and SELECT on system.query.history.";
  private static final int SQL_PARALLELISM = 1;

  /** Response of {@link #CURRENT_USER_PATH}, of which only the workspace ID header is read. */
  public static final class WorkspaceIdResponse {

    @Header(WORKSPACE_ID_HEADER)
    private String workspaceId;
  }

  /** Where the queries are read from: the values of {@link DatabricksLogsProperty#SOURCE}. */
  enum LogsSource {
    AUTO,
    SYSTEM_TABLE,
    API;

    /** Returns the value of the property, such as {@code system-table}. */
    @Override
    public String toString() {
      return Ascii.toLowerCase(name()).replace('_', '-');
    }
  }

  /** Properties of the Databricks logs connector, passed as {@code -D<name>=<value>}. */
  enum DatabricksLogsProperty implements ConnectorProperty {
    SOURCE(
        "source",
        "Where the queries are read from: 'system-table' reads only system.query.history, which"
            + " keeps the queries of the last 365 days and needs '--warehouse' or '--cluster';"
            + " 'api' reads only the query history API, which has the queries of the last 30 days"
            + " of the --url workspace; 'auto' reads system.query.history with '--warehouse' or"
            + " '--cluster', and the query history API without them, or if system.query.history"
            + " can't be read or its statement text is <REDACTED>, unless"
            + " -Ddatabricks.logs.workspace-id is set. Default: 'auto'."),
    WAREHOUSE_TRANSPORT("warehouse-transport", WAREHOUSE_TRANSPORT_DESCRIPTION),
    WORKSPACE_ID(
        "workspace-id",
        "The IDs of the workspaces whose queries are read from system.query.history, separated by"
            + " commas, or 'all' for every workspace in the region. Default: the --url workspace."
            + " Needs '--warehouse' or '--cluster'. Because the query history API has only the"
            + " queries of the --url workspace, this option can't be used with"
            + " -Ddatabricks.logs.source=api, and with it the API never replaces"
            + " system.query.history.");

    /** The value of {@link #WORKSPACE_ID} for every workspace in the region. */
    public static final String ALL_WORKSPACES = "all";

    private final String name;
    private final String description;

    DatabricksLogsProperty(String name, String description) {
      this.name = "databricks.logs." + name;
      this.description = description;
    }

    @Override
    public String getName() {
      return this.name;
    }

    @Override
    public String getDescription() {
      return this.description;
    }
  }

  public DatabricksLogsConnector() {
    super(CONNECTOR_NAME);
  }

  @Override
  protected Class<? extends Enum<? extends ConnectorProperty>> getConnectorProperties() {
    return DatabricksLogsProperty.class;
  }

  @Override
  public void validate(ConnectorArguments arguments) {
    Preconditions.checkNotNull(arguments, "ConnectorArguments was null.");
    validateNotAssessment(getName(), arguments);
    requireUrl(arguments);
    validateComputeAndTransport(arguments, DatabricksLogsProperty.WAREHOUSE_TRANSPORT);
    LogsSource source = getSource(arguments);
    boolean hasCompute = getWarehouse(arguments) != null || getCluster(arguments) != null;
    if (source == LogsSource.SYSTEM_TABLE && !hasCompute) {
      throw new MetadataDumperUsageException(
          "'-Ddatabricks.logs.source=system-table' needs '--warehouse <warehouse-id>' or"
              + " '--cluster <cluster-id>' to run the SQL that reads system.query.history."
              + SOURCE_USAGE);
    }

    if (parseWorkspaceIds(arguments) != null && (source == LogsSource.API || !hasCompute)) {
      throw new MetadataDumperUsageException(
          "'-Ddatabricks.logs.workspace-id' selects the workspaces of system.query.history, so it"
              + " needs '--warehouse <warehouse-id>' or '--cluster <cluster-id>', and can't be"
              + " used with '-Ddatabricks.logs.source=api': the query history API has only the"
              + " queries of the --url workspace."
              + SOURCE_USAGE);
    }
  }

  @Override
  public void addTasksTo(List<? super Task<?>> out, ConnectorArguments arguments) {
    Preconditions.checkNotNull(out, "Tasks list was null.");
    Preconditions.checkNotNull(arguments, "ConnectorArguments was null.");

    out.add(new DumpMetadataTask(arguments, DatabricksLogsDumpFormat.FORMAT_NAME));
    out.add(new FormatTask(DatabricksLogsDumpFormat.FORMAT_NAME));
    for (ZonedInterval interval : createIntervals(arguments)) {
      out.add(
          new QueryHistoryTask(
              getEntryFileNameWithTimestamp(DatabricksLogsDumpFormat.ZIP_ENTRY_PREFIX, interval),
              interval));
    }
  }

  /**
   * Opens the handle of the run. With {@code -Ddatabricks.logs.source=api}, or without a SQL
   * warehouse or cluster, the query history API is read. Otherwise, system.query.history is read,
   * unless one probe query fails or shows that the statement text is {@code <REDACTED>}: then the
   * query history API is read instead, if the source is {@code auto} and {@code
   * -Ddatabricks.logs.workspace-id} isn't set.
   */
  @Override
  public Handle open(ConnectorArguments arguments) throws IOException {
    Preconditions.checkNotNull(arguments, "ConnectorArguments was null.");
    WorkspaceClient client = createWorkspaceClient(arguments);
    LogsSource source = getSource(arguments);
    if (source == LogsSource.API
        || (getWarehouse(arguments) == null && getCluster(arguments) == null)) {
      logger.info(READING_API_MESSAGE);
      return new DatabricksLogsHandle(null, client, getWorkspaceId(client), ImmutableList.of());
    }

    // The query history API has only the queries of the --url workspace, so it replaces
    // system.query.history only with the 'auto' source and without -Ddatabricks.logs.workspace-id.
    ImmutableList<String> workspaceIds = parseWorkspaceIds(arguments);
    boolean apiAllowed = source == LogsSource.AUTO && workspaceIds == null;
    String urlWorkspaceId = null;
    if (workspaceIds == null) {
      urlWorkspaceId = getWorkspaceId(client);
      if (urlWorkspaceId == null) {
        throw new MetadataDumperUsageException(
            String.format(
                "Failed to find the ID of the workspace %s, whose queries are read from"
                    + " system.query.history by default. Set '-Ddatabricks.logs.workspace-id' to"
                    + " its ID, or to 'all' for every workspace in the region, or set"
                    + " '-Ddatabricks.logs.source=api' to read the query history API instead.",
                requireUrl(arguments)));
      }

      workspaceIds = ImmutableList.of(urlWorkspaceId);
    }

    DatabricksSqlRunner sqlRunner =
        openSystemTableRunner(client, arguments, workspaceIds, apiAllowed);
    logger.info(sqlRunner == null ? READING_API_MESSAGE : "Reading system.query.history.");
    return new DatabricksLogsHandle(sqlRunner, client, urlWorkspaceId, workspaceIds);
  }

  /**
   * Returns the runner that reads system.query.history, or null to read the query history API
   * instead when {@code apiAllowed} and one probe query fails or shows that the statement text is
   * {@code <REDACTED>}.
   */
  @Nullable
  private static DatabricksSqlRunner openSystemTableRunner(
      WorkspaceClient client,
      ConnectorArguments arguments,
      ImmutableList<String> workspaceIds,
      boolean apiAllowed)
      throws IOException {
    ZonedIntervalIterable intervals = createIntervals(arguments);
    ZonedInterval range = new ZonedInterval(intervals.getStart(), intervals.getEnd());
    DatabricksSqlRunner sqlRunner = null;
    boolean readSystemTable = false;
    try {
      sqlRunner =
          createSqlRunner(
              client,
              arguments,
              DatabricksLogsProperty.WAREHOUSE_TRANSPORT,
              SQL_PARALLELISM,
              System.getProperty(JAVA_VERSION_PROPERTY));
      String statementText = QueryHistoryTask.probeStatementText(sqlRunner, range, workspaceIds);
      if (statementText == null) {
        // The workspace IDs aren't checked, so a wrong ID only shows as a dump without queries.
        logger.warn(
            "system.query.history has no queries of {} that started from {} to {}.",
            workspaceIds.isEmpty() ? "any workspace" : "the workspaces " + workspaceIds,
            range.getStart(),
            range.getEndExclusive());
      }

      readSystemTable = !apiAllowed || !REDACTED_STATEMENT_TEXT.equals(statementText);
      if (REDACTED_STATEMENT_TEXT.equals(statementText)) {
        logger.warn(
            "The statement text of system.query.history is {} unless the user is an account admin"
                + " or a member of the databricks_pii_access group, so {}.",
            REDACTED_STATEMENT_TEXT,
            readSystemTable
                ? "the queries are dumped without their text"
                : "the query history API is read instead");
      }
    } catch (IOException | DatabricksException e) {
      if (!apiAllowed) {
        throw new IOException(
            String.format(
                "Failed to read system.query.history. %s Or set '-Ddatabricks.logs.source=api',"
                    + " without '-Ddatabricks.logs.workspace-id', to read the query history API"
                    + " instead. Cause: %s",
                SYSTEM_TABLE_ACCESS, e.getMessage()),
            e);
      }

      logger.warn(
          "Failed to read system.query.history. {} Cause: {}", SYSTEM_TABLE_ACCESS, e.getMessage());
    } finally {
      if (!readSystemTable) {
        Closeables.close(sqlRunner, true);
      }
    }

    return readSystemTable ? sqlRunner : null;
  }

  private static LogsSource getSource(ConnectorArguments arguments) {
    return firstNonNull(
        parseProperty(arguments, DatabricksLogsProperty.SOURCE, LogsSource.values()),
        LogsSource.AUTO);
  }

  /**
   * Returns the ID of the --url workspace from the client configuration or the current user API, or
   * null if neither has it.
   */
  @Nullable
  private static String getWorkspaceId(WorkspaceClient client) throws IOException {
    String workspaceId = Strings.emptyToNull(client.config().getWorkspaceId());
    if (workspaceId != null) {
      return workspaceId;
    }

    // The SDK's currentUser().me() sends the same request, but its User drops the response header
    // that has the workspace ID.
    Request request =
        new Request(Request.GET, CURRENT_USER_PATH).withHeader("Accept", "application/json");
    return Strings.emptyToNull(
        client.apiClient().execute(request, WorkspaceIdResponse.class).workspaceId);
  }

  private static ZonedIntervalIterable createIntervals(ConnectorArguments arguments) {
    Duration rotation = arguments.getQueryLogRotationFrequency();
    return ZonedIntervalIterableGenerator.forConnectorArguments(
        arguments, rotation, IntervalExpander.createBasedOnDuration(rotation));
  }

  @Nullable
  private static ImmutableList<String> parseWorkspaceIds(ConnectorArguments arguments) {
    String value =
        Strings.nullToEmpty(arguments.getDefinition(DatabricksLogsProperty.WORKSPACE_ID)).trim();
    if (value.isEmpty()) {
      return null;
    }

    if (DatabricksLogsProperty.ALL_WORKSPACES.equalsIgnoreCase(value)) {
      return ImmutableList.of();
    }

    ImmutableList<String> workspaceIds =
        ImmutableList.copyOf(Splitter.on(',').trimResults().split(value));
    if (workspaceIds.contains("")) {
      throw new MetadataDumperUsageException(
          String.format(
              "Option '%s' accepts '%s' or non-empty workspace IDs separated by commas, such as"
                  + " 1234567890123456. Actual: '%s'.",
              DatabricksLogsProperty.WORKSPACE_ID.getName(),
              DatabricksLogsProperty.ALL_WORKSPACES,
              value));
    }

    return workspaceIds;
  }
}
