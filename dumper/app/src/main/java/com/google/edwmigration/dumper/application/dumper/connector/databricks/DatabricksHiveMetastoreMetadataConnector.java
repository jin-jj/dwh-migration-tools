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

import static com.google.edwmigration.dumper.application.dumper.utils.PropertyParser.parseNumber;
import static java.lang.Math.toIntExact;

import com.databricks.sdk.WorkspaceClient;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsArgumentSchemaPredicate;
import com.google.edwmigration.dumper.application.dumper.annotations.RespectsInput;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnector;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import com.google.edwmigration.dumper.application.dumper.connector.MetadataConnector;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreCatalogsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreColumnsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreDdlsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreFunctionDdlsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreFunctionsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreSchemataTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreTableConstraintsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreTablesTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreViewsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.HiveMetastoreSessionCache;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.plugin.ext.jdk.annotation.Description;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Standalone Databricks {@code hive_metastore} metadata connector using {@code SHOW TABLE
 * EXTENDED}, {@code DESCRIBE TABLE}, {@code SHOW CREATE TABLE}, and {@code DESCRIBE FUNCTION
 * EXTENDED} via a Databricks SQL Warehouse.
 */
@AutoService(Connector.class)
@Description("Dumps metadata and DDLs from Databricks legacy hive_metastore using SQL Warehouse.")
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
    description = "The Databricks SQL Warehouse ID used for hive_metastore SQL extraction.")
@RespectsArgumentSchemaPredicate
public class DatabricksHiveMetastoreMetadataConnector extends AbstractConnector
    implements MetadataConnector {

  public static final String CONNECTOR_NAME = "databricks-hive-metastore";

  /**
   * Allowed values of {@link DatabricksHiveMetastoreProperty#MAX_CONCURRENT_STATEMENTS}. The HTTP
   * client of the Databricks SDK keeps at most 20 connections per host, so more threads would only
   * wait for a connection.
   */
  static final Range<Long> MAX_CONCURRENT_STATEMENTS_RANGE = Range.closed(1L, 20L);

  private static final int DEFAULT_MAX_CONCURRENT_STATEMENTS = 8;

  /** Properties of this connector, passed as {@code -D<name>=<value>}. */
  enum DatabricksHiveMetastoreProperty implements ConnectorProperty {
    MAX_CONCURRENT_STATEMENTS(
        "max-concurrent-statements",
        "Max number of SQL statements, such as SHOW CREATE TABLE for each table, that run"
            + " concurrently on the SQL warehouse. Allowed range: "
            + MAX_CONCURRENT_STATEMENTS_RANGE
            + ". Default: "
            + DEFAULT_MAX_CONCURRENT_STATEMENTS
            + ".");

    private final String name;
    private final String description;

    DatabricksHiveMetastoreProperty(@Nonnull String name, @Nonnull String description) {
      this.name = CONNECTOR_NAME + "." + name;
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

  public DatabricksHiveMetastoreMetadataConnector() {
    super(CONNECTOR_NAME);
  }

  @Override
  @Nonnull
  protected Class<? extends Enum<? extends ConnectorProperty>> getConnectorProperties() {
    return DatabricksHiveMetastoreProperty.class;
  }

  @Override
  public void validate(@Nonnull ConnectorArguments arguments) {
    Preconditions.checkNotNull(arguments, "ConnectorArguments was null.");
    DatabricksClientFactory.validateNotAssessment(getName(), arguments);
    DatabricksClientFactory.requireUrl(arguments);
    DatabricksClientFactory.requireWarehouse(arguments);
    parseMaxConcurrentStatements(arguments);
  }

  @Override
  public void addTasksTo(
      @Nonnull List<? super Task<?>> out, @Nonnull ConnectorArguments arguments) {
    Preconditions.checkNotNull(out, "Tasks list was null.");
    Preconditions.checkNotNull(arguments, "ConnectorArguments was null.");
    validate(arguments);

    out.add(new DumpMetadataTask(arguments, DatabricksHiveMetastoreDumpFormat.FORMAT_NAME));
    out.add(new FormatTask(DatabricksHiveMetastoreDumpFormat.FORMAT_NAME));

    HiveMetastoreSessionCache sessionCache = new HiveMetastoreSessionCache();
    out.add(new DatabricksHiveMetastoreCatalogsTask(sessionCache));
    out.add(new DatabricksHiveMetastoreSchemataTask(sessionCache));
    out.add(new DatabricksHiveMetastoreTablesTask(sessionCache));
    out.add(new DatabricksHiveMetastoreColumnsTask(sessionCache));
    out.add(new DatabricksHiveMetastoreViewsTask(sessionCache));
    out.add(new DatabricksHiveMetastoreTableConstraintsTask(sessionCache));
    out.add(new DatabricksHiveMetastoreFunctionsTask(sessionCache));
    out.add(new DatabricksHiveMetastoreDdlsTask(sessionCache));
    out.add(new DatabricksHiveMetastoreFunctionDdlsTask(sessionCache));
  }

  @Override
  @Nonnull
  public Handle open(@Nonnull ConnectorArguments arguments) {
    Preconditions.checkNotNull(arguments, "ConnectorArguments was null.");
    validate(arguments);
    String warehouseId = DatabricksClientFactory.requireWarehouse(arguments);
    WorkspaceClient client = DatabricksClientFactory.createWorkspaceClient(arguments);
    return new DatabricksHandle(
        new DatabricksSqlHelper(client, warehouseId),
        DatabricksFilter.fromArguments(arguments),
        parseMaxConcurrentStatements(arguments));
  }

  private static int parseMaxConcurrentStatements(@Nonnull ConnectorArguments arguments) {
    return toIntExact(
        parseNumber(
                arguments,
                DatabricksHiveMetastoreProperty.MAX_CONCURRENT_STATEMENTS,
                MAX_CONCURRENT_STATEMENTS_RANGE)
            .orElse(DEFAULT_MAX_CONCURRENT_STATEMENTS));
  }
}
