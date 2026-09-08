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
    for (Connector c : ServiceLoader.load(Connector.class)) {
      if ("databricks".equals(c.getName())) {
        foundDatabricks = true;
      }
      if ("databricks-sql".equals(c.getName())) {
        foundDatabricksSql = true;
      }
    }
    assertTrue("DatabricksConnector should be discoverable via ServiceLoader", foundDatabricks);
    assertTrue(
        "DatabricksSqlConnector should be discoverable via ServiceLoader", foundDatabricksSql);
  }

  @Test
  public void getName_returnsDatabricks() {
    assertEquals("databricks", connector.getName());
    assertEquals("databricks-sql", new DatabricksSqlConnector().getName());
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
  public void addTasksTo_defaultArguments_addsExpectedTasks() throws Exception {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector", "databricks",
            "--url", "https://dbc-test.cloud.databricks.com",
            "--warehouse", "warehouse123");
    List<Task<?>> tasks = new ArrayList<>();
    connector.addTasksTo(tasks, arguments);

    // 2 setup tasks + 7 UC tasks + 4 HMS tasks = 13 tasks
    assertEquals(13, tasks.size());
    assertTrue(tasks.get(0) instanceof DumpMetadataTask);
    assertTrue(tasks.get(1) instanceof FormatTask);
    assertTrue(tasks.get(2) instanceof DatabricksSqlCatalogsTask);
    assertTrue(tasks.get(3) instanceof DatabricksSqlSchemataTask);
    assertTrue(tasks.get(4) instanceof DatabricksSqlTablesTask);
    assertTrue(tasks.get(5) instanceof DatabricksSqlColumnsTask);
    assertTrue(tasks.get(6) instanceof DatabricksSqlViewsTask);
    assertTrue(tasks.get(7) instanceof DatabricksSqlTableConstraintsTask);
    assertTrue(tasks.get(8) instanceof DatabricksSqlFunctionsTask);
    assertTrue(tasks.get(9) instanceof DatabricksHiveMetastoreSchemataTask);
    assertTrue(tasks.get(10) instanceof DatabricksHiveMetastoreTablesTask);
    assertTrue(tasks.get(11) instanceof DatabricksHiveMetastoreColumnsTask);
    assertTrue(tasks.get(12) instanceof DatabricksHiveMetastoreViewsTask);
  }

  @Test
  public void addTasksTo_excludingHiveMetastore_addsOnlyUcTasks() throws Exception {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector", "databricks",
            "--url", "https://dbc-test.cloud.databricks.com",
            "--warehouse", "warehouse123",
            "--database", "main");
    List<Task<?>> tasks = new ArrayList<>();
    connector.addTasksTo(tasks, arguments);

    assertEquals(9, tasks.size());
    assertTrue(tasks.get(0) instanceof DumpMetadataTask);
    assertTrue(tasks.get(1) instanceof FormatTask);
    assertTrue(tasks.get(2) instanceof DatabricksSqlCatalogsTask);
    assertTrue(tasks.get(3) instanceof DatabricksSqlSchemataTask);
    assertTrue(tasks.get(4) instanceof DatabricksSqlTablesTask);
    assertTrue(tasks.get(5) instanceof DatabricksSqlColumnsTask);
    assertTrue(tasks.get(6) instanceof DatabricksSqlViewsTask);
    assertTrue(tasks.get(7) instanceof DatabricksSqlTableConstraintsTask);
    assertTrue(tasks.get(8) instanceof DatabricksSqlFunctionsTask);
    assertFalse(tasks.stream().anyMatch(t -> t instanceof DatabricksHiveMetastoreTablesTask));
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

    // 2 setup tasks + 7 UC tasks + 4 HMS tasks = 13 tasks
    assertEquals(13, tasks.size());
    assertTrue(tasks.stream().anyMatch(t -> t instanceof DatabricksHiveMetastoreSchemataTask));
    assertTrue(tasks.stream().anyMatch(t -> t instanceof DatabricksHiveMetastoreTablesTask));
    assertTrue(tasks.stream().anyMatch(t -> t instanceof DatabricksHiveMetastoreColumnsTask));
    assertTrue(tasks.stream().anyMatch(t -> t instanceof DatabricksHiveMetastoreViewsTask));
  }

  @Test
  public void getPropertyConstants_returnsEmpty() {
    assertNotNull(connector.getPropertyConstants());
    assertEquals(ImmutableList.of(), ImmutableList.copyOf(connector.getPropertyConstants()));
  }
}
