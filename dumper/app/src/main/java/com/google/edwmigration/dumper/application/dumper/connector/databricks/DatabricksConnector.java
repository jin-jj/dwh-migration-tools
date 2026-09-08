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
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnector;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorPropertyWithDefault;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Connector for dumping metadata from Databricks using SQL Warehouse queries. */
@AutoService({Connector.class, MetadataConnector.class})
@Description("Dumps metadata from Databricks Unity Catalog and Hive Metastore via SQL Warehouse.")
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
    arg = ConnectorArguments.OPT_DATABASE,
    description = "The list of catalogs to dump, separated by commas.")
@RespectsInput(
    order = 500,
    arg = ConnectorArguments.OPT_SCHEMA,
    description = "The list of schemas to dump, separated by commas.")
@RespectsInput(
    order = 600,
    arg = ConnectorArguments.OPT_SKIP_HIVE_METASTORE,
    description = "Whether to skip dumping legacy Databricks Hive Metastore metadata.")
public class DatabricksConnector extends AbstractConnector
    implements MetadataConnector, DatabricksMetadataDumpFormat {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksConnector.class);

  public static final String CONNECTOR_NAME = "databricks";

  public enum DatabricksConnectorProperty implements ConnectorPropertyWithDefault {
    SKIP_HIVE_METASTORE(
        "databricks.skip-hive-metastore",
        "Whether to skip dumping legacy Databricks Hive Metastore metadata.",
        "false");

    private final String name;
    private final String description;
    private final String defaultValue;

    DatabricksConnectorProperty(String name, String description, String defaultValue) {
      this.name = name;
      this.description = description;
      this.defaultValue = defaultValue;
    }

    @Nonnull
    @Override
    public String getName() {
      return name;
    }

    @Nonnull
    @Override
    public String getDescription() {
      return description;
    }

    @Nonnull
    @Override
    public String getDefaultValue() {
      return defaultValue;
    }
  }

  public DatabricksConnector() {
    super(CONNECTOR_NAME);
  }

  protected DatabricksConnector(@Nonnull String name) {
    super(name);
  }

  @Override
  public void validate(@Nonnull ConnectorArguments arguments) {
    Preconditions.checkArgument(arguments.hasUri(), "--url param is required");
    Preconditions.checkArgument(
        arguments.getWarehouse() != null && !arguments.getWarehouse().isEmpty(),
        "--warehouse <warehouse_id> is required for SQL-based Databricks metadata extraction");
  }

  @Override
  public void addTasksTo(
      @Nonnull List<? super Task<?>> out, @Nonnull ConnectorArguments arguments) {
    out.add(new DumpMetadataTask(arguments, FORMAT_NAME));
    out.add(new FormatTask(FORMAT_NAME));

    Predicate<String> catalogPredicate = arguments.getDatabasePredicate();
    if (arguments.getDatabases().isEmpty()) {
      catalogPredicate =
          catalogPredicate.and(
              name ->
                  !name.equalsIgnoreCase(AbstractDatabricksSqlTask.SAMPLES)
                      && !name.equalsIgnoreCase(AbstractDatabricksSqlTask.SYSTEM));
    }
    boolean skipHive =
        arguments.isSkipHiveMetastore()
            || Boolean.parseBoolean(
                arguments.getDefinitionOrDefault(DatabricksConnectorProperty.SKIP_HIVE_METASTORE));
    if (skipHive) {
      catalogPredicate =
          catalogPredicate.and(
              name -> !name.equalsIgnoreCase(AbstractDatabricksSqlTask.HIVE_METASTORE));
    }
    Predicate<String> schemaPredicate = arguments.getSchemaPredicate();

    out.add(new DatabricksSqlCatalogsTask(catalogPredicate));
    out.add(new DatabricksSqlSchemataTask(catalogPredicate, schemaPredicate));
    out.add(new DatabricksSqlTablesTask(catalogPredicate, schemaPredicate));
    out.add(new DatabricksSqlColumnsTask(catalogPredicate, schemaPredicate));
    out.add(new DatabricksSqlViewsTask(catalogPredicate, schemaPredicate));
    out.add(new DatabricksSqlTableConstraintsTask(catalogPredicate, schemaPredicate));
    out.add(new DatabricksSqlFunctionsTask(catalogPredicate, schemaPredicate));

    boolean includesHiveMetastore =
        !skipHive
            && (catalogPredicate.test(AbstractDatabricksSqlTask.HIVE_METASTORE)
                || arguments.getDatabases().stream()
                    .anyMatch(d -> d.equalsIgnoreCase(AbstractDatabricksSqlTask.HIVE_METASTORE)));
    if (includesHiveMetastore) {
      if (arguments.getWarehouse() != null) {
        out.add(new DatabricksHiveMetastoreSchemataTask(schemaPredicate));
        out.add(new DatabricksHiveMetastoreTablesTask(schemaPredicate));
        out.add(new DatabricksHiveMetastoreColumnsTask(schemaPredicate));
        out.add(new DatabricksHiveMetastoreViewsTask(schemaPredicate));
      }
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
  public Class<? extends Enum<? extends ConnectorProperty>> getConnectorProperties() {
    return DatabricksConnectorProperty.class;
  }
}
