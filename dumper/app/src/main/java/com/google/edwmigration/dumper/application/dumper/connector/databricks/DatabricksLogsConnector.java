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

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksQueryHistoryTasks.PLACEHOLDER;
import static com.google.edwmigration.dumper.application.dumper.utils.ArchiveNameUtil.getEntryFileNameWithTimestamp;
import static org.apache.commons.lang3.StringUtils.countMatches;

import com.databricks.sdk.WorkspaceClient;
import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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
import com.google.edwmigration.dumper.application.dumper.connector.ZonedIntervalIterableGenerator;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksQueryHistoryTasks.DatabricksQueryHistoryTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksQueryHistoryTasks.QueryHistorySession;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Databricks logs connector.
 *
 * <p>Dumps the queries of a Databricks workspace from {@code system.query.history}, which is read
 * on a SQL warehouse, or from the query history API. {@code system.query.history} has the queries
 * of every workspace of the account in the same region, so only the queries of the {@code --url}
 * workspace are dumped unless {@link DatabricksLogsProperty#WORKSPACE_ID} asks for others. The
 * statements of the dumper are left out.
 *
 * <p>The files are described in {@link DatabricksLogsDumpFormat}.
 */
@AutoService(Connector.class)
@Description(
    "Dumps the query history of a Databricks workspace from system.query.history using a SQL"
        + " warehouse, or from the query history API.")
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
    description = "The Databricks Personal Access Token (PAT) or OAuth token.")
@RespectsInput(
    order = 300,
    arg = ConnectorArguments.OPT_WAREHOUSE,
    description =
        "The ID of the Databricks SQL warehouse that reads system.query.history. Not needed with"
            + " -Ddatabricks.logs.strategy=rest-only.")
public class DatabricksLogsConnector extends AbstractConnector implements LogsConnector {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksLogsConnector.class);

  public static final String CONNECTOR_NAME = "databricks-logs";

  /** The value of {@link DatabricksLogsProperty#WORKSPACE_ID} for every workspace in the region. */
  static final String ALL_WORKSPACES = "all";

  private static final Pattern WORKSPACE_ID_PATTERN = Pattern.compile("\\d+");

  /**
   * The hosts that have the workspace ID: {@code adb-<id>.<n>.azuredatabricks.net} on Azure, and
   * {@code <id>.<n>.gcp.databricks.com} on Google Cloud.
   */
  private static final ImmutableList<Pattern> HOST_WITH_WORKSPACE_ID_PATTERNS =
      ImmutableList.of(
          Pattern.compile("adb-(\\d+)\\.\\d+\\.azuredatabricks\\.net", Pattern.CASE_INSENSITIVE),
          Pattern.compile("(\\d+)\\.\\d+\\.gcp\\.databricks\\.com", Pattern.CASE_INSENSITIVE));

  /** The values of {@link DatabricksLogsProperty#STRATEGY}. */
  enum Strategy {
    SYSTEM_THEN_REST("system-then-rest"),
    SYSTEM_ONLY("system-only"),
    REST_ONLY("rest-only");

    private final String value;

    Strategy(@Nonnull String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  /** Properties of this connector, passed as {@code -D<name>=<value>}. */
  enum DatabricksLogsProperty implements ConnectorProperty {
    STRATEGY(
        "strategy",
        "Where the query history is read from. system-then-rest reads system.query.history on the"
            + " SQL warehouse, or the query history API if system.query.history can't be read or"
            + " its statement text is <REDACTED>. system-only reads only system.query.history."
            + " rest-only reads only the query history API, which has the queries of the last 30"
            + " days of the --url workspace and needs no SQL warehouse. Default:"
            + " system-then-rest."),
    WORKSPACE_ID(
        "workspace-id",
        "The IDs of the workspaces whose queries are read from system.query.history, separated by"
            + " commas, or 'all' for every workspace in the region. Default: the --url"
            + " workspace."),
    QUERY(
        "query",
        "A custom query of system.query.history that returns the columns of the query history"
            + " files, in order. Its first %s is replaced with the start of each interval and its"
            + " second %s with the end, such as 2026-10-09T13:00:00Z."),
    WHERE(
        "where",
        "A condition that the queries of system.query.history must also meet, such as"
            + " execution_status = 'FINISHED'.");

    private final String name;
    private final String description;

    DatabricksLogsProperty(@Nonnull String name, @Nonnull String description) {
      this.name = "databricks.logs." + name;
      this.description = description;
    }

    @Override
    @Nonnull
    public String getName() {
      return name;
    }

    @Override
    @Nonnull
    public String getDescription() {
      return description;
    }
  }

  public DatabricksLogsConnector() {
    super(CONNECTOR_NAME);
  }

  @Override
  @Nonnull
  protected Class<? extends Enum<? extends ConnectorProperty>> getConnectorProperties() {
    return DatabricksLogsProperty.class;
  }

  @Override
  public void validate(@Nonnull ConnectorArguments arguments) {
    Preconditions.checkNotNull(arguments, "ConnectorArguments was null.");
    DatabricksClientFactory.validateNotAssessment(getName(), arguments);
    DatabricksClientFactory.requireUrl(arguments);
    Strategy strategy = parseStrategy(arguments);
    if (strategy != Strategy.REST_ONLY) {
      DatabricksClientFactory.requireWarehouse(arguments);
    }

    ImmutableList<String> workspaceIds = parseWorkspaceIds(arguments);
    String customQuery = parseCustomQuery(arguments);
    String condition = findDefinition(arguments, DatabricksLogsProperty.WHERE);
    if (customQuery != null && condition != null) {
      throw new MetadataDumperUsageException(
          String.format(
              "Options '%s' and '%s' can't be used together. Add the condition to the query.",
              DatabricksLogsProperty.QUERY.getName(), DatabricksLogsProperty.WHERE.getName()));
    }

    if (strategy == Strategy.REST_ONLY) {
      if (workspaceIds != null) {
        throw restOnlyConflict(
            DatabricksLogsProperty.WORKSPACE_ID, "it has only the queries of the --url workspace");
      }

      if (customQuery != null) {
        throw restOnlyConflict(DatabricksLogsProperty.QUERY, "it can't run SQL");
      }

      if (condition != null) {
        throw restOnlyConflict(DatabricksLogsProperty.WHERE, "it can't run SQL");
      }
    }
  }

  @Override
  public void addTasksTo(
      @Nonnull List<? super Task<?>> out, @Nonnull ConnectorArguments arguments) {
    Preconditions.checkNotNull(out, "Tasks list was null.");
    Preconditions.checkNotNull(arguments, "ConnectorArguments was null.");
    validate(arguments);

    out.add(new DumpMetadataTask(arguments, DatabricksLogsDumpFormat.FORMAT_NAME));
    out.add(new FormatTask(DatabricksLogsDumpFormat.FORMAT_NAME));

    Duration rotation = arguments.getQueryLogRotationFrequency();
    ImmutableList<ZonedInterval> intervals =
        ImmutableList.copyOf(
            ZonedIntervalIterableGenerator.forConnectorArguments(
                arguments, rotation, IntervalExpander.createBasedOnDuration(rotation)));
    if (intervals.isEmpty()) {
      return;
    }

    QueryHistorySession session =
        new QueryHistorySession(
            parseStrategy(arguments),
            parseCustomQuery(arguments),
            findDefinition(arguments, DatabricksLogsProperty.WHERE),
            intervals.get(0).span(intervals.get(intervals.size() - 1)));
    for (ZonedInterval interval : intervals) {
      out.add(
          new DatabricksQueryHistoryTask(
              getEntryFileNameWithTimestamp(DatabricksLogsDumpFormat.ZIP_ENTRY_PREFIX, interval),
              interval,
              session));
    }
  }

  @Override
  @Nonnull
  public Handle open(@Nonnull ConnectorArguments arguments) throws IOException {
    Preconditions.checkNotNull(arguments, "ConnectorArguments was null.");
    validate(arguments);
    WorkspaceClient client = DatabricksClientFactory.createWorkspaceClient(arguments);
    DatabricksSqlHelper sqlHelper =
        parseStrategy(arguments) == Strategy.REST_ONLY
            ? null
            : new DatabricksSqlHelper(client, DatabricksClientFactory.requireWarehouse(arguments));
    return createHandle(
        sqlHelper,
        new DatabricksRestHelper(client),
        DatabricksClientFactory.requireUrl(arguments),
        arguments);
  }

  /**
   * Returns the handle, with the ID of the {@code --url} workspace and the workspaces whose queries
   * are dumped, which are the {@code --url} workspace by default.
   */
  @Nonnull
  @VisibleForTesting
  static DatabricksLogsHandle createHandle(
      @CheckForNull DatabricksSqlHelper sqlHelper,
      @Nonnull DatabricksRestHelper restHelper,
      @Nonnull String url,
      @Nonnull ConnectorArguments arguments)
      throws IOException {
    String urlWorkspaceId = findUrlWorkspaceId(restHelper, url);
    ImmutableList<String> workspaceIds = parseWorkspaceIds(arguments);
    if (workspaceIds == null) {
      if (urlWorkspaceId != null) {
        workspaceIds = ImmutableList.of(urlWorkspaceId);
      } else if (parseStrategy(arguments) == Strategy.REST_ONLY) {
        workspaceIds = ImmutableList.of();
      } else {
        throw new MetadataDumperUsageException(
            String.format(
                "Failed to find the ID of the workspace %s. Set -D%s to the ID of the workspace,"
                    + " or to '%s' for the queries of every workspace in the region.",
                url, DatabricksLogsProperty.WORKSPACE_ID.getName(), ALL_WORKSPACES));
      }
    }

    return new DatabricksLogsHandle(sqlHelper, restHelper, urlWorkspaceId, workspaceIds);
  }

  /**
   * Returns the ID of the {@code --url} workspace, which Databricks sends in a response header, or
   * which the host has on Azure and Google Cloud. Returns null if neither has it.
   */
  @CheckForNull
  @VisibleForTesting
  static String findUrlWorkspaceId(@Nonnull DatabricksRestHelper restHelper, @Nonnull String url)
      throws IOException {
    try {
      String workspaceId = restHelper.findWorkspaceId();
      if (workspaceId != null) {
        return workspaceId;
      }

      logger.warn(
          "Databricks didn't send the ID of the workspace {} in the {} header.",
          url,
          DatabricksRestHelper.WORKSPACE_ID_HEADER);
    } catch (IOException e) {
      if (Thread.currentThread().isInterrupted()) {
        throw e;
      }

      logger.warn("Failed to find the ID of the workspace {}: {}", url, e.getMessage());
    }

    return parseWorkspaceIdOfHost(url);
  }

  /** Returns the workspace ID that the host of the URL has, or null if it has none. */
  @CheckForNull
  private static String parseWorkspaceIdOfHost(@Nonnull String url) {
    String host;
    try {
      host = URI.create(url).getHost();
    } catch (IllegalArgumentException e) {
      return null;
    }

    if (host == null) {
      return null;
    }

    for (Pattern pattern : HOST_WITH_WORKSPACE_ID_PATTERNS) {
      Matcher matcher = pattern.matcher(host);
      if (matcher.matches()) {
        return matcher.group(1);
      }
    }
    return null;
  }

  /** Returns the value of {@link DatabricksLogsProperty#STRATEGY}, the default if absent. */
  @Nonnull
  private static Strategy parseStrategy(@Nonnull ConnectorArguments arguments) {
    String value = findDefinition(arguments, DatabricksLogsProperty.STRATEGY);
    if (value == null) {
      return Strategy.SYSTEM_THEN_REST;
    }

    String normalized = value.toLowerCase(Locale.ROOT);
    for (Strategy strategy : Strategy.values()) {
      if (strategy.toString().equals(normalized)) {
        return strategy;
      }
    }
    throw new MetadataDumperUsageException(
        String.format(
            "Option '%s' accepts only %s, %s or %s. Actual: '%s'.",
            DatabricksLogsProperty.STRATEGY.getName(),
            Strategy.SYSTEM_THEN_REST,
            Strategy.SYSTEM_ONLY,
            Strategy.REST_ONLY,
            value));
  }

  /**
   * Returns the workspace IDs of {@link DatabricksLogsProperty#WORKSPACE_ID}: an empty list for
   * every workspace in the region, or null if the option is absent.
   */
  @CheckForNull
  private static ImmutableList<String> parseWorkspaceIds(@Nonnull ConnectorArguments arguments) {
    String value = findDefinition(arguments, DatabricksLogsProperty.WORKSPACE_ID);
    if (value == null) {
      return null;
    }

    if (ALL_WORKSPACES.equalsIgnoreCase(value)) {
      return ImmutableList.of();
    }

    ImmutableList.Builder<String> workspaceIds = ImmutableList.builder();
    for (String part : value.split(",", -1)) {
      String workspaceId = part.trim();
      if (!WORKSPACE_ID_PATTERN.matcher(workspaceId).matches()) {
        throw new MetadataDumperUsageException(
            String.format(
                "Option '%s' accepts '%s' or workspace IDs separated by commas, such as"
                    + " 1234567890123456. Actual: '%s'.",
                DatabricksLogsProperty.WORKSPACE_ID.getName(), ALL_WORKSPACES, value));
      }

      workspaceIds.add(workspaceId);
    }
    return workspaceIds.build();
  }

  /** Returns the value of {@link DatabricksLogsProperty#QUERY}, or null if it is absent. */
  @CheckForNull
  private static String parseCustomQuery(@Nonnull ConnectorArguments arguments) {
    String query = findDefinition(arguments, DatabricksLogsProperty.QUERY);
    if (query != null && countMatches(query, PLACEHOLDER) != 2) {
      throw new MetadataDumperUsageException(
          String.format(
              "Option '%s' needs two %s, which are replaced with the start and the end of each"
                  + " interval. Actual: %d.",
              DatabricksLogsProperty.QUERY.getName(),
              PLACEHOLDER,
              countMatches(query, PLACEHOLDER)));
    }

    return query;
  }

  /** Returns the trimmed value of a property, or null if it is absent or blank. */
  @CheckForNull
  private static String findDefinition(
      @Nonnull ConnectorArguments arguments, @Nonnull DatabricksLogsProperty property) {
    String value = arguments.getDefinition(property);
    return value == null || value.trim().isEmpty() ? null : value.trim();
  }

  @Nonnull
  private static MetadataDumperUsageException restOnlyConflict(
      @Nonnull DatabricksLogsProperty property, @Nonnull String reason) {
    return new MetadataDumperUsageException(
        String.format(
            "Option '%s' can't be used with -D%s=%s, because the query history API %s.",
            property.getName(),
            DatabricksLogsProperty.STRATEGY.getName(),
            Strategy.REST_ONLY,
            reason));
  }
}
