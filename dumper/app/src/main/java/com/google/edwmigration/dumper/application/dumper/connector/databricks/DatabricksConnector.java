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

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnector;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat;
import java.util.List;
import java.util.function.Predicate;
import javax.annotation.Nonnull;

/** Connector for dumping metadata from Databricks Unity Catalog and Hive Metastore. */
@AutoService({Connector.class, MetadataConnector.class})
@Description("Dumps metadata from Databricks Unity Catalog and Hive Metastore.")
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
    description = "Optional Databricks SQL warehouse ID (required for legacy hive_metastore).")
@RespectsInput(
    order = 400,
    arg = ConnectorArguments.OPT_DATABASE,
    description = "The list of catalogs to dump, separated by commas.")
@RespectsInput(
    order = 500,
    arg = ConnectorArguments.OPT_SCHEMA,
    description = "The list of schemas to dump, separated by commas.")
public class DatabricksConnector extends AbstractConnector
    implements MetadataConnector, DatabricksMetadataDumpFormat {

  public static final String CONNECTOR_NAME = "databricks";

  public DatabricksConnector() {
    super(CONNECTOR_NAME);
  }

  @Override
  public void validate(@Nonnull ConnectorArguments arguments) {
    Preconditions.checkArgument(arguments.hasUri(), "--url param is required");
  }

  @Override
  public void addTasksTo(
      @Nonnull List<? super Task<?>> out, @Nonnull ConnectorArguments arguments) {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));

    Predicate<String> catalogPredicate = arguments.getDatabasePredicate();
    Predicate<String> schemaPredicate = arguments.getSchemaPredicate();

    out.add(new DatabricksCatalogsTask(catalogPredicate));
    out.add(new DatabricksSchemasTask(catalogPredicate, schemaPredicate));
    out.add(new DatabricksTablesTask(catalogPredicate, schemaPredicate));
    out.add(new DatabricksColumnsTask(catalogPredicate, schemaPredicate));
    out.add(new DatabricksViewsTask(catalogPredicate, schemaPredicate));
    out.add(new DatabricksTableConstraintsTask(catalogPredicate, schemaPredicate));
    out.add(new DatabricksFunctionsTask(catalogPredicate, schemaPredicate));

    if (arguments.getWarehouse() != null && catalogPredicate.test("hive_metastore")) {
      out.add(new DatabricksHiveMetastoreSchemataTask(schemaPredicate));
      out.add(new DatabricksHiveMetastoreTablesTask(schemaPredicate));
      out.add(new DatabricksHiveMetastoreColumnsTask(schemaPredicate));
      out.add(new DatabricksHiveMetastoreViewsTask(schemaPredicate));
    }
  }

  @Nonnull
  @Override
  public Handle open(@Nonnull ConnectorArguments arguments) throws Exception {
    DatabricksConfig config = new DatabricksConfig();
    if (arguments.hasUri()) {
      config.setHost(arguments.getUri());
    }
    if (arguments.isPasswordFlagProvided()) {
      config.setToken(arguments.getPasswordOrPrompt());
    }
    WorkspaceClient client = new WorkspaceClient(config);
    return new DatabricksHandle(client, arguments.getWarehouse());
  }

  @Nonnull
  @Override
  public Iterable<ConnectorProperty> getPropertyConstants() {
    return ImmutableList.of();
  }
}
