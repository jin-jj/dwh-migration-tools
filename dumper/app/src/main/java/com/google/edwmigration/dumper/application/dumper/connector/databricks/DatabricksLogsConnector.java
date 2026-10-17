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

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataConnector.createWorkspaceClient;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataConnector.findWarehouse;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataConnector.requireUrl;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataConnector.validateNotAssessment;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.logs.QueryHistoryTask.REDACTED_STATEMENT_TEXT;
import static com.google.edwmigration.dumper.application.dumper.utils.ArchiveNameUtil.getEntryFileNameWithTimestamp;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.support.Header;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
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
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Databricks logs connector. */
@AutoService(Connector.class)
@Description(
    "Dumps the query history of a Databricks workspace from system.query.history using a SQL"
        + " warehouse, or from the query history API without a SQL warehouse.")
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
        "The ID of the Databricks SQL warehouse that reads system.query.history. Without it, the"
            + " query history API is read, which has the queries of the last 30 days of the --url"
            + " workspace. The API is also read if system.query.history can't be read or its"
            + " statement text is <REDACTED>, unless -Ddatabricks.logs.workspace-id is set.")
public class DatabricksLogsConnector extends AbstractConnector implements LogsConnector {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksLogsConnector.class);

  public static final String CONNECTOR_NAME = "databricks-logs";

  private static final String CURRENT_USER_PATH = "/api/2.0/preview/scim/v2/Me";
  private static final String WORKSPACE_ID_HEADER = "X-Databricks-Org-Id";

  /** Response of {@link #CURRENT_USER_PATH}, of which only the workspace ID header is read. */
  public static final class WorkspaceIdResponse {

    @Header(WORKSPACE_ID_HEADER)
    private String workspaceId;
  }

  /** Properties of the Databricks logs connector, passed as {@code -D<name>=<value>}. */
  enum DatabricksLogsProperty implements ConnectorProperty {
    WORKSPACE_ID(
        "workspace-id",
        "The IDs of the workspaces whose queries are read from system.query.history, separated by"
            + " commas, or 'all' for every workspace in the region. Needs --warehouse. When it is"
            + " set, the query history API is never read instead, because it has only the queries"
            + " of the --url workspace. Default: the --url workspace.");

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
      return name;
    }

    @Override
    public String getDescription() {
      return description;
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
    if (parseWorkspaceIds(arguments) != null && findWarehouse(arguments) == null) {
      throw new MetadataDumperUsageException(
          String.format(
              "Option '%s' needs --warehouse, because the query history API has only the queries"
                  + " of the --url workspace.",
              DatabricksLogsProperty.WORKSPACE_ID.getName()));
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
   * Opens the handle of the run. Without a SQL warehouse, the query history API is read. With a SQL
   * warehouse, system.query.history is read, unless one probe query fails or shows that the
   * statement text is {@code <REDACTED>}: then the query history API is read instead, if {@code
   * -Ddatabricks.logs.workspace-id} isn't set.
   */
  @Override
  public Handle open(ConnectorArguments arguments) throws IOException {
    Preconditions.checkNotNull(arguments, "ConnectorArguments was null.");
    String url = requireUrl(arguments);
    WorkspaceClient client = createWorkspaceClient(arguments);
    String urlWorkspaceId = findWorkspaceId(client);
    String warehouseId = findWarehouse(arguments);
    if (warehouseId == null) {
      logger.info(
          "Reading the query history API, which has the queries of the last 30 days of the --url"
              + " workspace.");
      return new DatabricksLogsHandle(null, client, urlWorkspaceId, ImmutableList.of());
    }

    ImmutableList<String> workspaceIds = parseWorkspaceIds(arguments);
    String reasonApiCannotBeRead = null;
    if (workspaceIds != null) {
      reasonApiCannotBeRead =
          String.format(
              "it has only the queries of the --url workspace %s, but -D%s asks for %s",
              url,
              DatabricksLogsProperty.WORKSPACE_ID.getName(),
              workspaceIds.isEmpty()
                  ? "every workspace in the region"
                  : String.join(", ", workspaceIds));
    } else if (urlWorkspaceId != null) {
      workspaceIds = ImmutableList.of(urlWorkspaceId);
    } else {
      throw new MetadataDumperUsageException(
          String.format(
              "Failed to find the ID of the workspace %s. Set -D%s to the ID of the workspace, or"
                  + " to '%s' for the queries of every workspace in the region.",
              url,
              DatabricksLogsProperty.WORKSPACE_ID.getName(),
              DatabricksLogsProperty.ALL_WORKSPACES));
    }

    DatabricksSqlHelper sqlHelper = new DatabricksSqlHelper(client, warehouseId);
    ZonedIntervalIterable intervals = createIntervals(arguments);
    ZonedInterval range = new ZonedInterval(intervals.getStart(), intervals.getEnd());
    boolean readSystemTable =
        probeSystemTable(sqlHelper, range, workspaceIds, reasonApiCannotBeRead);
    return new DatabricksLogsHandle(
        readSystemTable ? sqlHelper : null, client, urlWorkspaceId, workspaceIds);
  }

  // Returns true to read system.query.history, or false to read the query history API instead,
  // which is possible only if reasonApiCannotBeRead is null.
  private static boolean probeSystemTable(
      DatabricksSqlHelper sqlHelper,
      ZonedInterval range,
      ImmutableList<String> workspaceIds,
      String reasonApiCannotBeRead)
      throws IOException {
    String statementText;
    try {
      statementText = QueryHistoryTask.probeStatementText(sqlHelper, range, workspaceIds);
    } catch (IOException | DatabricksException e) {
      if (reasonApiCannotBeRead != null) {
        throw new IOException(
            String.format(
                "Failed to read system.query.history, and the query history API can't be read"
                    + " instead, because %s. Cause: %s",
                reasonApiCannotBeRead, e.getMessage()),
            e);
      }
      logger.warn(
          "Failed to read system.query.history, so the query history API is read instead. It has"
              + " the queries of the last 30 days of the --url workspace. Cause: {}",
          e.getMessage());
      return false;
    }

    if (!REDACTED_STATEMENT_TEXT.equals(statementText)) {
      logger.info("Reading system.query.history.");
      return true;
    }

    if (reasonApiCannotBeRead != null) {
      logger.warn(
          "The statement text of system.query.history is {} unless the user is an account admin or"
              + " a member of the databricks_pii_access group. The queries are dumped without their"
              + " text, because the query history API can't be read instead: {}.",
          REDACTED_STATEMENT_TEXT,
          reasonApiCannotBeRead);
      return true;
    }

    logger.warn(
        "The statement text of system.query.history is {} unless the user is an account admin or a"
            + " member of the databricks_pii_access group, so the query history API is read"
            + " instead. It has the queries of the last 30 days of the --url workspace.",
        REDACTED_STATEMENT_TEXT);
    return false;
  }

  /** Returns the ID of the --url workspace, or null if the API doesn't send it. */
  private static String findWorkspaceId(WorkspaceClient client) throws IOException {
    Request request =
        new Request("GET", CURRENT_USER_PATH).withHeader("Accept", "application/json");
    WorkspaceIdResponse response = client.apiClient().execute(request, WorkspaceIdResponse.class);
    return Strings.emptyToNull(response.workspaceId);
  }

  private static ZonedIntervalIterable createIntervals(ConnectorArguments arguments) {
    Duration rotation = arguments.getQueryLogRotationFrequency();
    return ZonedIntervalIterableGenerator.forConnectorArguments(
        arguments, rotation, IntervalExpander.createBasedOnDuration(rotation));
  }

  private static ImmutableList<String> parseWorkspaceIds(ConnectorArguments arguments) {
    String value =
        Strings.nullToEmpty(arguments.getDefinition(DatabricksLogsProperty.WORKSPACE_ID)).trim();
    if (value.isEmpty()) {
      return null;
    }

    if (DatabricksLogsProperty.ALL_WORKSPACES.equalsIgnoreCase(value)) {
      return ImmutableList.of();
    }

    if (!value.matches("\\d+(\\s*,\\s*\\d+)*")) {
      throw new MetadataDumperUsageException(
          String.format(
              "Option '%s' accepts '%s' or workspace IDs separated by commas, such as"
                  + " 1234567890123456. Actual: '%s'.",
              DatabricksLogsProperty.WORKSPACE_ID.getName(),
              DatabricksLogsProperty.ALL_WORKSPACES,
              value));
    }
    return ImmutableList.copyOf(value.split("\\s*,\\s*"));
  }
}
