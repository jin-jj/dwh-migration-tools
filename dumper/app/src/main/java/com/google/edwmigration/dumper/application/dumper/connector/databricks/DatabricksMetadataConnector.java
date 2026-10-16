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

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.DatabricksFilter.HIVE_METASTORE_CATALOG;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.HIVE_METASTORE_CATALOGS_ENTRY_NAME;

import com.databricks.sdk.WorkspaceClient;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentSchemaPredicate;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnector;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksClients;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksRestHelper;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.DatabricksHandle;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.DatabricksHandle.MetadataTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.HiveMetastoreTasks;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.UnityCatalogTasks;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.DatabricksFilter;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import java.util.ArrayList;
import java.util.List;
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
    description = "The Databricks Personal Access Token (PAT) or OAuth token.")
@RespectsInput(
    order = 300,
    arg = ConnectorArguments.OPT_WAREHOUSE,
    description =
        "The ID of the Databricks SQL warehouse that runs the metadata statements of the"
            + " hive_metastore catalog. Without it, the hive_metastore catalog is skipped, and only"
            + " the Unity Catalog catalogs are dumped.")
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

  public DatabricksMetadataConnector() {
    super(CONNECTOR_NAME);
  }

  @Override
  public void validate(ConnectorArguments arguments) {
    Preconditions.checkNotNull(arguments, "ConnectorArguments was null.");
    DatabricksClients.validateNotAssessment(getName(), arguments);
    DatabricksClients.requireUrl(arguments);
    if (DatabricksClients.findWarehouse(arguments) == null
        && !DatabricksFilter.fromArguments(arguments).acceptsUnityCatalog()) {
      throw new MetadataDumperUsageException(
          "The hive_metastore catalog needs a SQL warehouse, because its metadata is read with SQL"
              + " statements. Please specify '--warehouse <warehouse-id>'.");
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
      if (DatabricksClients.findWarehouse(arguments) != null) {
        metadataTasks.addAll(HiveMetastoreTasks.create());
      } else {
        logger.warn(
            "Skipping the hive_metastore catalog because no SQL warehouse was given with"
                + " '--warehouse'.");
      }
    }

    out.addAll(metadataTasks);

    // catalogs-hms.csv is written without a statement, so it doesn't count as a success.
    List<Task<?>> networkTasks = new ArrayList<>(metadataTasks.size());
    for (Task<?> task : metadataTasks) {
      if (!HIVE_METASTORE_CATALOGS_ENTRY_NAME.equals(task.getTargetPath())) {
        networkTasks.add(task);
      }
    }
    out.add(MetadataTask.createAllFailedCheckTask(networkTasks));
  }

  @Override
  public Handle open(ConnectorArguments arguments) {
    Preconditions.checkNotNull(arguments, "ConnectorArguments was null.");
    WorkspaceClient client = DatabricksClients.createWorkspaceClient(arguments);
    String warehouseId = DatabricksClients.findWarehouse(arguments);
    DatabricksSqlHelper sqlHelper =
        warehouseId == null ? null : new DatabricksSqlHelper(client, warehouseId);
    return new DatabricksHandle(
        sqlHelper, new DatabricksRestHelper(client), DatabricksFilter.fromArguments(arguments));
  }
}
