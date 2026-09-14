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
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
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
    STRATEGY(
        "databricks.metadata.strategy",
        "Extraction strategy: system-then-catalog-then-rest (default), system-then-catalog,"
            + " system-only, catalog-only, or rest-only. Each tier runs only if the tiers before"
            + " it failed. rest-only does not need a SQL warehouse.",
        "system-then-catalog-then-rest"),
    REST_REQUESTS_PER_SECOND(
        "databricks.rest.requests-per-second",
        "Maximum Unity Catalog REST requests per second issued by the REST tier.",
        String.valueOf(DatabricksHandle.DEFAULT_REST_REQUESTS_PER_SECOND)),
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

  private final DatabricksInput inputSource;

  public DatabricksConnector() {
    this(CONNECTOR_NAME, DatabricksInput.SYSTEM_THEN_CATALOG_THEN_REST);
  }

  protected DatabricksConnector(@Nonnull String name) {
    this(name, DatabricksInput.SYSTEM_THEN_CATALOG_THEN_REST);
  }

  public DatabricksConnector(@Nonnull String name, @Nonnull DatabricksInput inputSource) {
    super(name);
    this.inputSource = Preconditions.checkNotNull(inputSource, "DatabricksInput cannot be null.");
  }

  @Override
  public void validate(@Nonnull ConnectorArguments arguments) {
    Preconditions.checkArgument(arguments.hasUri(), "--url param is required");
    Preconditions.checkArgument(
        !resolveStrategy(arguments).requiresWarehouse()
            || (arguments.getWarehouse() != null && !arguments.getWarehouse().isEmpty()),
        "--warehouse <warehouse_id> is required unless"
            + " -Ddatabricks.metadata.strategy=rest-only is set");
  }

  /** Returns the strategy the user asked for, or the connector's own default. */
  @Nonnull
  private DatabricksInput resolveStrategy(@Nonnull ConnectorArguments arguments) {
    String strategyDefinition = arguments.getDefinition(DatabricksConnectorProperty.STRATEGY);
    return strategyDefinition == null
        ? inputSource
        : DatabricksInput.fromString(strategyDefinition);
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
                  !name.equalsIgnoreCase(DatabricksCatalogNames.SAMPLES)
                      && !name.equalsIgnoreCase(DatabricksCatalogNames.SYSTEM));
    }
    boolean skipHive =
        arguments.isSkipHiveMetastore()
            || Boolean.parseBoolean(
                arguments.getDefinitionOrDefault(DatabricksConnectorProperty.SKIP_HIVE_METASTORE));
    if (skipHive) {
      catalogPredicate =
          catalogPredicate.and(
              name -> !name.equalsIgnoreCase(DatabricksCatalogNames.HIVE_METASTORE));
    }
    Predicate<String> schemaPredicate = arguments.getSchemaPredicate();

    DatabricksInput strategy = resolveStrategy(arguments);

    out.addAll(
        strategy.tasks(
            new DatabricksSystemSqlCatalogsTask(catalogPredicate),
            new DatabricksSqlCatalogsTask(catalogPredicate),
            new DatabricksRestCatalogsTask(catalogPredicate)));
    out.addAll(
        strategy.tasks(
            new DatabricksSystemSqlSchemataTask(catalogPredicate, schemaPredicate),
            new DatabricksSqlSchemataTask(catalogPredicate, schemaPredicate),
            new DatabricksRestSchemataTask(catalogPredicate, schemaPredicate)));
    out.addAll(
        strategy.tasks(
            new DatabricksSystemSqlTablesTask(catalogPredicate, schemaPredicate),
            new DatabricksSqlTablesTask(catalogPredicate, schemaPredicate),
            new DatabricksRestTablesTask(catalogPredicate, schemaPredicate)));
    out.addAll(
        strategy.tasks(
            new DatabricksSystemSqlColumnsTask(catalogPredicate, schemaPredicate),
            new DatabricksSqlColumnsTask(catalogPredicate, schemaPredicate),
            new DatabricksRestColumnsTask(catalogPredicate, schemaPredicate)));
    out.addAll(
        strategy.tasks(
            new DatabricksSystemSqlViewsTask(catalogPredicate, schemaPredicate),
            new DatabricksSqlViewsTask(catalogPredicate, schemaPredicate),
            new DatabricksRestViewsTask(catalogPredicate, schemaPredicate)));
    out.addAll(
        strategy.tasks(
            new DatabricksSystemSqlTableConstraintsTask(catalogPredicate, schemaPredicate),
            new DatabricksSqlTableConstraintsTask(catalogPredicate, schemaPredicate),
            new DatabricksRestTableConstraintsTask(catalogPredicate, schemaPredicate)));
    out.addAll(
        strategy.tasks(
            new DatabricksSystemSqlFunctionsTask(catalogPredicate, schemaPredicate),
            new DatabricksSqlFunctionsTask(catalogPredicate, schemaPredicate),
            new DatabricksRestFunctionsTask(catalogPredicate, schemaPredicate)));

    boolean includesHiveMetastore =
        !skipHive
            && (catalogPredicate.test(DatabricksCatalogNames.HIVE_METASTORE)
                || arguments.getDatabases().stream()
                    .anyMatch(d -> d.equalsIgnoreCase(DatabricksCatalogNames.HIVE_METASTORE)));
    if (includesHiveMetastore) {
      out.add(new DatabricksHiveMetastoreCatalogsTask());
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
    String warehouse = arguments.getWarehouse();
    return new DatabricksHandle(
        client, warehouse == null ? "" : warehouse, restRequestsPerSecond(arguments));
  }

  private static double restRequestsPerSecond(@Nonnull ConnectorArguments arguments) {
    String value =
        arguments.getDefinitionOrDefault(DatabricksConnectorProperty.REST_REQUESTS_PER_SECOND);
    try {
      double parsed = Double.parseDouble(value.trim());
      if (parsed > 0) {
        return parsed;
      }
    } catch (NumberFormatException e) {
      // Reported below, together with the out-of-range case.
    }
    throw new MetadataDumperUsageException(
        "Property "
            + DatabricksConnectorProperty.REST_REQUESTS_PER_SECOND.getName()
            + " must be a positive number, but was '"
            + value
            + "'");
  }

  @Nonnull
  @Override
  public Class<? extends Enum<? extends ConnectorProperty>> getConnectorProperties() {
    return DatabricksConnectorProperty.class;
  }
}
