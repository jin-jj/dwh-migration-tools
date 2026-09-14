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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksConnectorTest {

  private final DatabricksConnector connector = new DatabricksConnector();

  @Test
  public void serviceLoader_findsConnector() {
    boolean foundDatabricks = false;
    boolean foundDatabricksSql = false;
    boolean foundDatabricksSystem = false;
    boolean foundDatabricksCatalog = false;
    for (Connector c : ServiceLoader.load(Connector.class)) {
      if ("databricks".equals(c.getName())) {
        foundDatabricks = true;
      }
      if ("databricks-sql".equals(c.getName())) {
        foundDatabricksSql = true;
      }
      if ("databricks-system-metadata".equals(c.getName())) {
        foundDatabricksSystem = true;
      }
      if ("databricks-catalog-metadata".equals(c.getName())) {
        foundDatabricksCatalog = true;
      }
    }
    assertTrue("DatabricksConnector should be discoverable via ServiceLoader", foundDatabricks);
    assertTrue(
        "DatabricksSqlConnector should be discoverable via ServiceLoader", foundDatabricksSql);
    assertTrue(
        "DatabricksSystemMetadataConnector should be discoverable via ServiceLoader",
        foundDatabricksSystem);
    assertTrue(
        "DatabricksCatalogMetadataConnector should be discoverable via ServiceLoader",
        foundDatabricksCatalog);
  }

  @Test
  public void getName_returnsDatabricks() {
    assertEquals("databricks", connector.getName());
    assertEquals("databricks-sql", new DatabricksSqlConnector().getName());
    assertEquals("databricks-system-metadata", new DatabricksSystemMetadataConnector().getName());
    assertEquals("databricks-catalog-metadata", new DatabricksCatalogMetadataConnector().getName());
  }

  @Test(expected = IllegalArgumentException.class)
  public void validate_missingUri_throwsException() throws Exception {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector", "databricks",
            "--warehouse", "warehouse123");
    connector.validate(arguments);
  }

  @Test(expected = IllegalArgumentException.class)
  public void validate_missingWarehouse_throwsException() throws Exception {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector", "databricks",
            "--url", "https://dbc-test.cloud.databricks.com");
    connector.validate(arguments);
  }

  @Test
  public void validate_validArguments_succeeds() throws Exception {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector", "databricks",
            "--url", "https://dbc-test.cloud.databricks.com",
            "--warehouse", "warehouse123");
    connector.validate(arguments);
  }

  @Test
  public void addTasksTo_defaultArguments_addsSystemAndFallbackCatalogTasks() throws Exception {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector", "databricks",
            "--url", "https://dbc-test.cloud.databricks.com",
            "--warehouse", "warehouse123");
    List<Task<?>> tasks = new ArrayList<>();
    connector.addTasksTo(tasks, arguments);

    // 2 setup tasks + 14 UC tasks (7 system + 7 catalog fallback) + 4 HMS tasks = 20 tasks
    assertEquals(20, tasks.size());
    assertTrue(tasks.get(0) instanceof DumpMetadataTask);
    assertTrue(tasks.get(1) instanceof FormatTask);
    assertTrue(tasks.get(2) instanceof DatabricksSystemSqlCatalogsTask);
    assertTrue(tasks.get(3) instanceof DatabricksSqlCatalogsTask);
    assertTrue(tasks.get(4) instanceof DatabricksSystemSqlSchemataTask);
    assertTrue(tasks.get(5) instanceof DatabricksSqlSchemataTask);
    assertTrue(tasks.get(6) instanceof DatabricksSystemSqlTablesTask);
    assertTrue(tasks.get(7) instanceof DatabricksSqlTablesTask);
    assertTrue(tasks.get(8) instanceof DatabricksSystemSqlColumnsTask);
    assertTrue(tasks.get(9) instanceof DatabricksSqlColumnsTask);
    assertTrue(tasks.get(10) instanceof DatabricksSystemSqlViewsTask);
    assertTrue(tasks.get(11) instanceof DatabricksSqlViewsTask);
    assertTrue(tasks.get(12) instanceof DatabricksSystemSqlTableConstraintsTask);
    assertTrue(tasks.get(13) instanceof DatabricksSqlTableConstraintsTask);
    assertTrue(tasks.get(14) instanceof DatabricksSystemSqlFunctionsTask);
    assertTrue(tasks.get(15) instanceof DatabricksSqlFunctionsTask);
    assertTrue(tasks.get(16) instanceof DatabricksHiveMetastoreSchemataTask);
    assertTrue(tasks.get(17) instanceof DatabricksHiveMetastoreTablesTask);
    assertTrue(tasks.get(18) instanceof DatabricksHiveMetastoreColumnsTask);
    assertTrue(tasks.get(19) instanceof DatabricksHiveMetastoreViewsTask);

    // Verify conditions: each catalog task should depend on failure of its corresponding system
    // task
    assertTrue(tasks.get(3).getConditions().length > 0);
    assertTrue(tasks.get(5).getConditions().length > 0);
    assertTrue(tasks.get(7).getConditions().length > 0);
    assertTrue(tasks.get(9).getConditions().length > 0);
    assertTrue(tasks.get(11).getConditions().length > 0);
    assertTrue(tasks.get(13).getConditions().length > 0);
    assertTrue(tasks.get(15).getConditions().length > 0);
  }

  @Test
  public void addTasksTo_excludingHiveMetastore_addsSystemAndFallbackCatalogTasks()
      throws Exception {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector", "databricks",
            "--url", "https://dbc-test.cloud.databricks.com",
            "--warehouse", "warehouse123",
            "--database", "main");
    List<Task<?>> tasks = new ArrayList<>();
    connector.addTasksTo(tasks, arguments);

    // 2 setup tasks + 14 UC tasks = 16 tasks
    assertEquals(16, tasks.size());
    assertTrue(tasks.get(0) instanceof DumpMetadataTask);
    assertTrue(tasks.get(1) instanceof FormatTask);
    assertTrue(tasks.get(2) instanceof DatabricksSystemSqlCatalogsTask);
    assertTrue(tasks.get(3) instanceof DatabricksSqlCatalogsTask);
    assertFalse(tasks.stream().anyMatch(t -> t instanceof DatabricksHiveMetastoreTablesTask));
  }

  @Test
  public void addTasksTo_catalogOnlyStrategy_addsOnlyCatalogTasks() throws Exception {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector",
            "databricks-catalog-metadata",
            "--url",
            "https://dbc-test.cloud.databricks.com",
            "--warehouse",
            "warehouse123",
            "--skip-hive-metastore");
    List<Task<?>> tasks = new ArrayList<>();
    new DatabricksCatalogMetadataConnector().addTasksTo(tasks, arguments);

    // 2 setup tasks + 7 catalog tasks = 9 tasks
    assertEquals(9, tasks.size());
    assertTrue(tasks.get(2) instanceof DatabricksSqlCatalogsTask);
    assertTrue(tasks.get(3) instanceof DatabricksSqlSchemataTask);
    assertTrue(tasks.get(4) instanceof DatabricksSqlTablesTask);
    assertTrue(tasks.get(5) instanceof DatabricksSqlColumnsTask);
    assertTrue(tasks.get(6) instanceof DatabricksSqlViewsTask);
    assertTrue(tasks.get(7) instanceof DatabricksSqlTableConstraintsTask);
    assertTrue(tasks.get(8) instanceof DatabricksSqlFunctionsTask);
    assertFalse(tasks.stream().anyMatch(t -> t instanceof AbstractDatabricksSystemSqlTask));
  }

  @Test
  public void addTasksTo_systemOnlyStrategy_addsOnlySystemTasks() throws Exception {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector",
            "databricks-system-metadata",
            "--url",
            "https://dbc-test.cloud.databricks.com",
            "--warehouse",
            "warehouse123",
            "--skip-hive-metastore");
    List<Task<?>> tasks = new ArrayList<>();
    new DatabricksSystemMetadataConnector().addTasksTo(tasks, arguments);

    // 2 setup tasks + 7 system tasks = 9 tasks
    assertEquals(9, tasks.size());
    assertTrue(tasks.get(2) instanceof DatabricksSystemSqlCatalogsTask);
    assertTrue(tasks.get(3) instanceof DatabricksSystemSqlSchemataTask);
    assertTrue(tasks.get(4) instanceof DatabricksSystemSqlTablesTask);
    assertTrue(tasks.get(5) instanceof DatabricksSystemSqlColumnsTask);
    assertTrue(tasks.get(6) instanceof DatabricksSystemSqlViewsTask);
    assertTrue(tasks.get(7) instanceof DatabricksSystemSqlTableConstraintsTask);
    assertTrue(tasks.get(8) instanceof DatabricksSystemSqlFunctionsTask);
    assertFalse(tasks.stream().anyMatch(t -> t instanceof DatabricksSqlCatalogsTask));
  }

  @Test
  public void addTasksTo_strategyProperty_overridesStrategy() throws Exception {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector",
            "databricks",
            "--url",
            "https://dbc-test.cloud.databricks.com",
            "--warehouse",
            "warehouse123",
            "--skip-hive-metastore",
            "-Ddatabricks.metadata.strategy=catalog-only");
    List<Task<?>> tasks = new ArrayList<>();
    connector.addTasksTo(tasks, arguments);

    // 2 setup tasks + 7 catalog tasks = 9 tasks
    assertEquals(9, tasks.size());
    assertTrue(tasks.get(2) instanceof DatabricksSqlCatalogsTask);
    assertFalse(tasks.stream().anyMatch(t -> t instanceof AbstractDatabricksSystemSqlTask));
  }

  @Test
  public void addTasksTo_withHiveMetastore_addsHmsTasks() throws Exception {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector", "databricks",
            "--url", "https://dbc-test.cloud.databricks.com",
            "--warehouse", "warehouse123",
            "--database", "hive_metastore");
    List<Task<?>> tasks = new ArrayList<>();
    connector.addTasksTo(tasks, arguments);

    // 2 setup tasks + 14 UC tasks + 4 HMS tasks = 20 tasks
    assertEquals(20, tasks.size());
    assertTrue(tasks.stream().anyMatch(t -> t instanceof DatabricksHiveMetastoreSchemataTask));
    assertTrue(tasks.stream().anyMatch(t -> t instanceof DatabricksHiveMetastoreTablesTask));
    assertTrue(tasks.stream().anyMatch(t -> t instanceof DatabricksHiveMetastoreColumnsTask));
    assertTrue(tasks.stream().anyMatch(t -> t instanceof DatabricksHiveMetastoreViewsTask));
  }

  @Test
  public void addTasksTo_withSkipHiveMetastoreFlag_addsOnlyUcTasks() throws Exception {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector",
            "databricks",
            "--url",
            "https://dbc-test.cloud.databricks.com",
            "--warehouse",
            "warehouse123",
            "--skip-hive-metastore");
    List<Task<?>> tasks = new ArrayList<>();
    connector.addTasksTo(tasks, arguments);

    assertEquals(16, tasks.size());
    assertTrue(tasks.get(2) instanceof DatabricksSystemSqlCatalogsTask);
    assertTrue(tasks.get(3) instanceof DatabricksSqlCatalogsTask);
    assertFalse(tasks.stream().anyMatch(t -> t instanceof DatabricksHiveMetastoreTablesTask));
  }

  @Test
  public void addTasksTo_withSkipHiveMetastoreProperty_addsOnlyUcTasks() throws Exception {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector",
            "databricks",
            "--url",
            "https://dbc-test.cloud.databricks.com",
            "--warehouse",
            "warehouse123",
            "-Ddatabricks.skip-hive-metastore=true");
    List<Task<?>> tasks = new ArrayList<>();
    connector.addTasksTo(tasks, arguments);

    assertEquals(16, tasks.size());
    assertFalse(tasks.stream().anyMatch(t -> t instanceof DatabricksHiveMetastoreTablesTask));
  }

  @Test
  public void getPropertyConstants_returnsStrategyAndSkipHiveMetastore() {
    assertNotNull(connector.getPropertyConstants());
    assertEquals(
        ImmutableList.of(
            DatabricksConnector.DatabricksConnectorProperty.STRATEGY,
            DatabricksConnector.DatabricksConnectorProperty.SKIP_HIVE_METASTORE),
        ImmutableList.copyOf(connector.getPropertyConstants()));
  }
}
