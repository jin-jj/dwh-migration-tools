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

import static com.google.edwmigration.dumper.application.dumper.utils.ArchiveNameUtil.getEntryFileNameWithTimestamp;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
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
import com.google.edwmigration.dumper.application.dumper.connector.ZonedIntervalIterable;
import com.google.edwmigration.dumper.application.dumper.connector.ZonedIntervalIterableGenerator;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksLogsDumpFormat;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connector for dumping query history logs from Databricks Unity Catalog system tables ({@code
 * system.query.history}), with automatic fallback to the Workspace Query History REST API ({@code
 * GET /api/2.0/sql/history/queries}) when the caller lacks permission on the {@code system} catalog
 * or when {@code statement_text} is {@code <REDACTED>}.
 */
@AutoService({Connector.class, LogsConnector.class})
@Description(
    "Dumps query logs from Databricks Unity Catalog system tables or Workspace Query History API.")
@RespectsArgumentQueryLogDays
@RespectsArgumentQueryLogStart
@RespectsArgumentQueryLogEnd
@RespectsInput(
    order = 100,
    arg = ConnectorArguments.OPT_URI,
    description = "The URL of the Databricks workspace (e.g. https://<workspace-host>).",
    required = "true")
@RespectsInput(
    order = 200,
    arg = ConnectorArguments.OPT_PASSWORD,
    description = "The Databricks Personal Access Token (PAT) or OAuth token.")
@RespectsInput(
    order = 300,
    arg = ConnectorArguments.OPT_WAREHOUSE,
    description = "The Databricks SQL warehouse ID used to execute queries.",
    required = "true")
@RespectsInput(
    order = 400,
    arg = ConnectorArguments.OPT_WORKSPACE_ID,
    description = "The Databricks workspace ID(s) used to filter system.query.history.")
public class DatabricksLogsConnector extends AbstractConnector
    implements LogsConnector, DatabricksLogsDumpFormat {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksLogsConnector.class);

  public static final String CONNECTOR_NAME = "databricks-logs";

  private static final DateTimeFormatter SQL_FORMAT =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneOffset.UTC);

  private static final Pattern AZURE_WORKSPACE_URL_PATTERN =
      Pattern.compile("(?:^|https?://)adb-(\\d+)\\.");

  private static final String BASE_QUERY =
      "SELECT account_id, "
          + "workspace_id, "
          + "statement_id, "
          + "session_id, "
          + "executed_by, "
          + "statement_text, "
          + "statement_type, "
          + "execution_status, "
          + "error_message, "
          + "compute.warehouse_id AS warehouse_id, "
          + "compute.type AS compute_type, "
          + "start_time, "
          + "end_time, "
          + "total_duration_ms, "
          + "compilation_duration_ms, "
          + "execution_duration_ms, "
          + "waiting_at_capacity_duration_ms, "
          + "read_bytes, "
          + "read_rows, "
          + "written_bytes, "
          + "written_rows, "
          + "spilled_local_bytes, "
          + "client_application "
          + "FROM system.query.history "
          + "WHERE end_time >= to_timestamp_ltz('%s') "
          + "AND end_time <= to_timestamp_ltz('%s') "
          + "AND COALESCE(client_application, '') <> '"
          + DatabricksHandle.CLIENT_APPLICATION
          + "' "
          + "AND COALESCE(query_tags['"
          + DatabricksHandle.QUERY_TAG_KEY
          + "'], '') <> '"
          + DatabricksHandle.CLIENT_APPLICATION
          + "' ";

  public DatabricksLogsConnector() {
    super(CONNECTOR_NAME);
  }

  @Override
  public void validate(@Nonnull ConnectorArguments arguments) {
    if (arguments.isAssessment()) {
      throw new MetadataDumperUsageException(
          "--assessment is not supported by the '"
              + getName()
              + "' connector. Re-run without --assessment.");
    }
    Preconditions.checkArgument(arguments.hasUri(), "--url param is required");
    Preconditions.checkArgument(
        isRestOnly(arguments)
            || (arguments.getWarehouse() != null && !arguments.getWarehouse().isEmpty()),
        "--warehouse <warehouse_id> is required to query Databricks query history unless"
            + " -Ddatabricks.logs.strategy=rest-only is set.");
  }

  @Override
  @Nonnull
  public ImmutableList<ConnectorProperty> getPropertyConstants() {
    return DatabricksLogsConnectorProperty.getConstants();
  }

  @CheckForNull
  static String resolveWorkspaceId(@Nonnull ConnectorArguments arguments) {
    String cliWorkspaceId = arguments.getWorkspaceId();
    if (cliWorkspaceId != null && !cliWorkspaceId.trim().isEmpty()) {
      return cliWorkspaceId.trim();
    }
    String propWorkspaceId = arguments.getDefinition(DatabricksLogsConnectorProperty.WORKSPACE_ID);
    if (propWorkspaceId != null && !propWorkspaceId.trim().isEmpty()) {
      return propWorkspaceId.trim();
    }
    return null;
  }

  @CheckForNull
  @VisibleForTesting
  static String resolveRestWorkspaceId(@Nonnull ConnectorArguments arguments) {
    String explicit = resolveWorkspaceId(arguments);
    if (explicit != null) {
      List<String> ids = parseWorkspaceIds(explicit);
      if (!ids.isEmpty()) {
        return ids.get(0);
      }
    }
    if (arguments.hasUri()) {
      Matcher matcher = AZURE_WORKSPACE_URL_PATTERN.matcher(arguments.getUri());
      if (matcher.find()) {
        return matcher.group(1);
      }
    }
    return null;
  }

  @Nonnull
  private static List<String> parseWorkspaceIds(@Nonnull String raw) {
    List<String> ids = new ArrayList<>();
    for (String part : raw.split(",")) {
      String trimmed = part.trim();
      if (!trimmed.isEmpty()) {
        ids.add(trimmed);
      }
    }
    return ids;
  }

  private static boolean isRestOnly(@Nonnull ConnectorArguments arguments) {
    String strategy = arguments.getDefinition(DatabricksLogsConnectorProperty.STRATEGY);
    if (strategy == null) {
      return false;
    }
    String normalized = strategy.trim().toLowerCase().replace('_', '-');
    return "rest-only".equals(normalized) || "rest".equals(normalized);
  }

  private static boolean isSystemOnly(@Nonnull ConnectorArguments arguments) {
    String strategy = arguments.getDefinition(DatabricksLogsConnectorProperty.STRATEGY);
    if (strategy == null) {
      return false;
    }
    String normalized = strategy.trim().toLowerCase().replace('_', '-');
    return "system-only".equals(normalized) || "system".equals(normalized);
  }

  @Nonnull
  @VisibleForTesting
  static String buildQueryTemplate(@Nonnull ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    String overrideQuery = arguments.getDefinition(DatabricksLogsConnectorProperty.OVERRIDE_QUERY);
    if (overrideQuery != null) {
      if (StringUtils.countMatches(overrideQuery, "%s") != 2) {
        throw new MetadataDumperUsageException(
            "Custom query for log dump needs two \"%s\" expansions, they will be expanded to"
                + " end_time lower and upper boundaries.");
      }
      return overrideQuery;
    }
    StringBuilder queryBuilder = new StringBuilder(BASE_QUERY);
    String workspaceIdsRaw = resolveWorkspaceId(arguments);
    if (workspaceIdsRaw != null) {
      List<String> ids = parseWorkspaceIds(workspaceIdsRaw);
      if (ids.size() == 1) {
        queryBuilder
            .append("AND workspace_id = ")
            .append(DatabricksFilter.stringLiteral(ids.get(0)))
            .append(" ");
      } else if (ids.size() > 1) {
        StringBuilder inList = new StringBuilder();
        for (int i = 0; i < ids.size(); i++) {
          if (i > 0) {
            inList.append(", ");
          }
          inList.append(DatabricksFilter.stringLiteral(ids.get(i)));
        }
        queryBuilder.append("AND workspace_id IN (").append(inList).append(") ");
      }
    }
    String overrideWhere = arguments.getDefinition(DatabricksLogsConnectorProperty.OVERRIDE_WHERE);
    if (overrideWhere != null) {
      queryBuilder.append("AND ").append(overrideWhere).append(" ");
    }
    return queryBuilder.toString();
  }

  @Override
  public void addTasksTo(@Nonnull List<? super Task<?>> out, @Nonnull ConnectorArguments arguments)
      throws MetadataDumperUsageException {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));

    Duration rotationDuration = arguments.getQueryLogRotationFrequency();
    ZonedIntervalIterable queryLogIntervals =
        ZonedIntervalIterableGenerator.forConnectorArguments(
            arguments, rotationDuration, IntervalExpander.createBasedOnDuration(rotationDuration));
    logger.info("Exporting Databricks query log for {}", queryLogIntervals);

    String queryTemplate = buildQueryTemplate(arguments);
    String restWorkspaceId = resolveRestWorkspaceId(arguments);
    boolean allowRestFallback = !isSystemOnly(arguments);
    for (ZonedInterval interval : queryLogIntervals) {
      String file = getEntryFileNameWithTimestamp(ZIP_ENTRY_PREFIX, interval);
      String query =
          String.format(
              queryTemplate,
              SQL_FORMAT.format(interval.getStart()),
              SQL_FORMAT.format(interval.getEndInclusive()));
      out.add(
          new DatabricksQueryHistoryTask(
              file, query, interval, restWorkspaceId, allowRestFallback));
    }
  }

  @Nonnull
  @Override
  public Handle open(@Nonnull ConnectorArguments arguments) throws Exception {
    DatabricksHandle.registerUserAgent();
    DatabricksConfig config = new DatabricksConfig();
    if (arguments.hasUri()) {
      config.setHost(arguments.getUri());
    }
    if (arguments.isPasswordFlagProvided()) {
      config.setToken(arguments.getPasswordOrPrompt());
    }
    WorkspaceClient client = new WorkspaceClient(config);
    String warehouse = arguments.getWarehouse();
    DatabricksHandle handle =
        new DatabricksHandle(client, warehouse == null ? "" : warehouse, 20.0);
    if (isRestOnly(arguments) || !handle.hasWarehouseId()) {
      handle.markSystemQueryHistoryUnavailable();
    }
    return handle;
  }
}
