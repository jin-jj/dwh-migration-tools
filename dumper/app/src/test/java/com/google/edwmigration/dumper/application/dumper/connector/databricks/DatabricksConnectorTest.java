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

import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.ConnectorRepository;
import com.google.edwmigration.dumper.application.dumper.connector.AbstractConnectorTest;
import com.google.edwmigration.dumper.application.dumper.connector.Connector;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksConnectorTest extends AbstractConnectorTest {

  private final DatabricksConnector connector = new DatabricksConnector();

  @Test
  public void getName_returnsDatabricks() {
    assertEquals("databricks", connector.getName());
  }

  @Test
  public void getDefaultFileName_returnsExpected() {
    String name = connector.getDefaultFileName(false, Clock.systemUTC());
    assertTrue(name.startsWith("dwh-migration-databricks-metadata"));
    assertTrue(name.endsWith(".zip"));
  }

  @Test
  public void serviceLoader_registersConnector() {
    Connector resolved = ConnectorRepository.getInstance().getByName("databricks");
    assertNotNull(resolved);
    assertEquals(DatabricksConnector.class, resolved.getClass());
  }

  @Test(expected = IllegalArgumentException.class)
  public void validate_missingUrl_throws() throws Exception {
    ConnectorArguments arguments = new ConnectorArguments("--connector", "databricks");
    connector.validate(arguments);
  }

  @Test
  public void validate_withUrl_success() throws Exception {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector", "databricks", "--url", "https://test.cloud.databricks.com");
    connector.validate(arguments);
  }

  @Test
  public void addTasksTo_withoutWarehouse_addsOnlyUcTasks() throws Exception {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector", "databricks", "--url", "https://test.cloud.databricks.com");
    List<Task<?>> tasks = new ArrayList<>();
    connector.addTasksTo(tasks, arguments);

    List<String> targetPaths = new ArrayList<>();
    for (Task<?> task : tasks) {
      targetPaths.add(task.getTargetPath());
    }

    assertTrue(targetPaths.contains("compilerworks-metadata.yaml"));
    assertTrue(targetPaths.contains("compilerworks-format.txt"));
    assertTrue(targetPaths.contains("catalogs.csv"));
    assertTrue(targetPaths.contains("schemata.csv"));
    assertTrue(targetPaths.contains("tables.csv"));
    assertTrue(targetPaths.contains("columns.csv"));
    assertTrue(targetPaths.contains("views.csv"));
    assertTrue(targetPaths.contains("table_constraints.csv"));
    assertTrue(targetPaths.contains("functions.csv"));
    assertFalse(targetPaths.contains("tables-hms.csv"));
  }

  @Test
  public void addTasksTo_withWarehouse_addsHmsTasks() throws Exception {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector",
            "databricks",
            "--url",
            "https://test.cloud.databricks.com",
            "--warehouse",
            "test-warehouse-id");
    List<Task<?>> tasks = new ArrayList<>();
    connector.addTasksTo(tasks, arguments);

    List<String> targetPaths = new ArrayList<>();
    for (Task<?> task : tasks) {
      targetPaths.add(task.getTargetPath());
    }

    assertTrue(targetPaths.contains("catalogs.csv"));
    assertTrue(targetPaths.contains("schemata-hms.csv"));
    assertTrue(targetPaths.contains("tables-hms.csv"));
    assertTrue(targetPaths.contains("columns-hms.csv"));
    assertTrue(targetPaths.contains("views-hms.csv"));
  }

  @Test
  public void addTasksTo_withWarehouseAndDatabaseFilterExcludingHms_skipsHmsTasks()
      throws Exception {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector",
            "databricks",
            "--url",
            "https://test.cloud.databricks.com",
            "--warehouse",
            "test-warehouse-id",
            "--database",
            "main");
    List<Task<?>> tasks = new ArrayList<>();
    connector.addTasksTo(tasks, arguments);

    List<String> targetPaths = new ArrayList<>();
    for (Task<?> task : tasks) {
      targetPaths.add(task.getTargetPath());
    }

    assertTrue(targetPaths.contains("catalogs.csv"));
    assertFalse(targetPaths.contains("tables-hms.csv"));
  }
}
