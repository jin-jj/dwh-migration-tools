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

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.DatabricksHandle.DatabricksFilter.HIVE_METASTORE_CATALOG;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.compute.ClusterDetails;
import com.databricks.sdk.service.compute.DataSecurityMode;
import com.databricks.sdk.service.compute.State;
import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentSchemaPredicate;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnector;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.ClusterSqlRunner;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlRunner;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.DatabricksHandle;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.DatabricksHandle.DatabricksFilter;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.HiveMetastoreTasks;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.UnityCatalogTasks;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.CatalogsFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Databricks metadata connector. */
@AutoService(Connector.class)
@Description(
    "Dumps metadata from the Databricks Unity Catalog catalogs using the Unity Catalog API, and"
        + " from the hive_metastore catalog using a SQL warehouse.")
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
        "The ID of the Databricks SQL warehouse that runs the metadata statements of the"
            + " hive_metastore catalog. Without it or --cluster, the hive_metastore catalog is"
            + " skipped, and only the Unity Catalog catalogs are dumped.")
@RespectsInput(
    order = 350,
    arg = ConnectorArguments.OPT_CLUSTER,
    description =
        "The ID of a running all-purpose cluster that runs the metadata statements of the"
            + " hive_metastore catalog instead of a SQL warehouse, such as the cluster of the"
            + " notebook that runs the dumper. The cluster must run Databricks Runtime 16.2 or"
            + " above; dedicated access mode is supported.")
@RespectsInput(
    order = 600,
    arg = ConnectorArguments.OPT_DATABASE,
    description =
        "The list of catalogs to dump, separated by commas, such as main,hive_metastore. By"
            + " default, every catalog except the system, samples and __databricks_internal"
            + " catalogs.")
@RespectsArgumentSchemaPredicate
public class DatabricksMetadataConnector extends AbstractConnector implements MetadataConnector {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksMetadataConnector.class);

  public static final String CONNECTOR_NAME = "databricks";

  // DESCRIBE TABLE ... AS JSON needs Databricks Runtime 16.2 or above.
  private static final int MIN_RUNTIME_MAJOR_VERSION = 16;
  private static final int MIN_RUNTIME_MINOR_VERSION = 2;
  // The spark_version of a cluster starts with the runtime version, such as 16.4.x-scala2.12, or
  // with x as the minor version for the latest release of a major version, such as 18.x-scala2.13.
  private static final Pattern RUNTIME_VERSION_PATTERN = Pattern.compile("(\\d+)\\.(\\d+|x)");
  private static final String LATEST_MINOR_VERSION = "x";
  private static final ImmutableSet<DataSecurityMode> DEDICATED_ACCESS_MODES =
      ImmutableSet.of(
          DataSecurityMode.SINGLE_USER,
          DataSecurityMode.DATA_SECURITY_MODE_DEDICATED,
          DataSecurityMode.LEGACY_SINGLE_USER);

  public DatabricksMetadataConnector() {
    super(CONNECTOR_NAME);
  }

  @Override
  public void validate(ConnectorArguments arguments) {
    Preconditions.checkNotNull(arguments, "ConnectorArguments was null.");
    validateNotAssessment(getName(), arguments);
    requireUrl(arguments);
    boolean hasWarehouse = findWarehouse(arguments) != null;
    boolean hasCluster = findCluster(arguments) != null;
    if (hasWarehouse && hasCluster) {
      throw new MetadataDumperUsageException(
          "The hive_metastore statements run on either a SQL warehouse or a cluster. Please"
              + " specify either '--warehouse <warehouse-id>' or '--cluster <cluster-id>', not"
              + " both.");
    }

    if (!hasWarehouse
        && !hasCluster
        && !DatabricksFilter.fromArguments(arguments).acceptsUnityCatalog()) {
      throw new MetadataDumperUsageException(
          "The hive_metastore catalog needs a SQL warehouse or a cluster, because its metadata is"
              + " read with SQL statements. Please specify '--warehouse <warehouse-id>' or"
              + " '--cluster <cluster-id>'.");
    }
  }

  @Override
  public void addTasksTo(List<? super Task<?>> out, ConnectorArguments arguments) {
    Preconditions.checkNotNull(out, "Tasks list was null.");
    Preconditions.checkNotNull(arguments, "ConnectorArguments was null.");

    out.add(new DumpMetadataTask(arguments, DatabricksMetadataDumpFormat.FORMAT_NAME));
    out.add(new FormatTask(DatabricksMetadataDumpFormat.FORMAT_NAME));

    DatabricksFilter filter = DatabricksFilter.fromArguments(arguments);
    List<Task<?>> metadataTasks = new ArrayList<>();
    if (filter.acceptsUnityCatalog()) {
      metadataTasks.addAll(UnityCatalogTasks.create());
    }

    if (filter.acceptsCatalog(HIVE_METASTORE_CATALOG)) {
      if (findWarehouse(arguments) != null || findCluster(arguments) != null) {
        metadataTasks.addAll(HiveMetastoreTasks.create());
      } else {
        logger.warn(
            "Skipping the hive_metastore catalog because no SQL warehouse or cluster was given with"
                + " '--warehouse' or '--cluster'.");
      }
    }

    out.addAll(metadataTasks);

    // catalogs-hms.csv is written without a statement, so it doesn't count as a success.
    out.add(
        DatabricksHandle.createAllFailedCheckTask(
            ImmutableList.copyOf(
                Iterables.filter(
                    metadataTasks,
                    task -> !CatalogsFormat.HMS_ZIP_ENTRY_NAME.equals(task.getTargetPath())))));
  }

  @Override
  public Handle open(ConnectorArguments arguments) {
    Preconditions.checkNotNull(arguments, "ConnectorArguments was null.");
    WorkspaceClient client = createWorkspaceClient(arguments);
    DatabricksFilter filter = DatabricksFilter.fromArguments(arguments);
    DatabricksSqlRunner sqlRunner =
        filter.acceptsCatalog(HIVE_METASTORE_CATALOG) ? createSqlRunner(client, arguments) : null;
    return new DatabricksHandle(sqlRunner, client, filter);
  }

  /**
   * Returns the SQL runner of the SQL warehouse of --warehouse or of the cluster of --cluster, or
   * null when neither was given. Throws a usage error when the cluster can't run the statements.
   */
  private static DatabricksSqlRunner createSqlRunner(
      WorkspaceClient client, ConnectorArguments arguments) {
    String warehouseId = findWarehouse(arguments);
    if (warehouseId != null) {
      return new DatabricksSqlHelper(client, warehouseId);
    }

    String clusterId = findCluster(arguments);
    if (clusterId == null) {
      return null;
    }

    checkCluster(client.clusters().get(clusterId));
    return new ClusterSqlRunner(client, clusterId);
  }

  /**
   * Throws a usage error when a cluster isn't running or runs a Databricks Runtime below 16.2, and
   * warns when its access mode isn't dedicated, the only mode tried so far.
   */
  @VisibleForTesting
  static void checkCluster(ClusterDetails cluster) {
    if (cluster.getState() != State.RUNNING) {
      throw new MetadataDumperUsageException(
          String.format(
              "The cluster %s is %s. Please start it, or run the dumper from a notebook attached to"
                  + " it.",
              cluster.getClusterId(), cluster.getState()));
    }

    String sparkVersion = Strings.nullToEmpty(cluster.getSparkVersion());
    Matcher version = RUNTIME_VERSION_PATTERN.matcher(sparkVersion);
    if (!version.lookingAt()) {
      logger.warn(
          "Can't read the Databricks Runtime version of cluster {} from '{}'. The hive_metastore"
              + " statements need Databricks Runtime 16.2 or above.",
          cluster.getClusterId(),
          sparkVersion);
    } else if (isBelowMinimumRuntime(Integer.parseInt(version.group(1)), version.group(2))) {
      throw new MetadataDumperUsageException(
          String.format(
              "The cluster %s runs Databricks Runtime %s, but the hive_metastore statements need"
                  + " 16.2 or above for DESCRIBE TABLE ... AS JSON. Please use a cluster on"
                  + " Databricks Runtime 16.4 LTS or above.",
              cluster.getClusterId(), sparkVersion));
    }

    if (!DEDICATED_ACCESS_MODES.contains(cluster.getDataSecurityMode())) {
      logger.warn(
          "The cluster {} uses the access mode {}. Only dedicated access mode was tried so far.",
          cluster.getClusterId(),
          cluster.getDataSecurityMode());
    }
  }

  /**
   * Returns whether a runtime version is below 16.2. The minor version x is the latest release of
   * its major version, which is 16.4 for 16.
   */
  private static boolean isBelowMinimumRuntime(int major, String minor) {
    if (major != MIN_RUNTIME_MAJOR_VERSION) {
      return major < MIN_RUNTIME_MAJOR_VERSION;
    }

    return !LATEST_MINOR_VERSION.equals(minor)
        && Integer.parseInt(minor) < MIN_RUNTIME_MINOR_VERSION;
  }

  static void validateNotAssessment(String connectorName, ConnectorArguments arguments) {
    if (arguments.isAssessment()) {
      throw new MetadataDumperUsageException(
          String.format(
              "The '--assessment' flag is not supported by the '%s' connector. "
                  + "Databricks hardware telemetry and DBU usage assessment are collected via the "
                  + "Databricks Discovery Notebook.",
              connectorName));
    }
  }

  static String requireUrl(ConnectorArguments arguments) {
    String url = arguments.getUri();
    if (url == null || url.trim().isEmpty()) {
      throw new MetadataDumperUsageException(
          "Required workspace URL was not provided. Please specify '--url <https://workspace-host>'.");
    }
    return url.trim();
  }

  /** Returns the ID of the SQL warehouse of --warehouse, or null if none. */
  static String findWarehouse(ConnectorArguments arguments) {
    String warehouse = arguments.getWarehouse();
    return warehouse == null || warehouse.trim().isEmpty() ? null : warehouse.trim();
  }

  /** Returns the ID of the cluster of --cluster, or null if none. */
  static String findCluster(ConnectorArguments arguments) {
    String cluster = arguments.getCluster();
    return cluster == null || cluster.trim().isEmpty() ? null : cluster.trim();
  }

  /**
   * Creates the client of the workspace of --url. Without --password, the client uses the
   * Databricks SDK default authentication, such as the DATABRICKS_TOKEN environment variable.
   */
  static WorkspaceClient createWorkspaceClient(ConnectorArguments arguments) {
    DatabricksConfig config = new DatabricksConfig().setHost(requireUrl(arguments));
    if (arguments.isPasswordFlagProvided()) {
      String token = arguments.getPasswordOrPrompt().trim();
      if (!token.isEmpty()) {
        config.setToken(token);
      }
    }

    return new WorkspaceClient(config);
  }
}
