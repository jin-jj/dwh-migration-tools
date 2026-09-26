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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.ConnectorRepository;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksHiveMetastoreMetadataConnectorTest {

  private static final String CONNECTOR_NAME =
      DatabricksHiveMetastoreMetadataConnector.CONNECTOR_NAME;

  @Test
  public void connector_isDiscoverableViaConnectorRepository() {
    assertNotNull(ConnectorRepository.getInstance().getByName(CONNECTOR_NAME));
  }

  @Test
  public void validate_rejectsAssessmentFlag() throws IOException {
    DatabricksHiveMetastoreMetadataConnector connector =
        new DatabricksHiveMetastoreMetadataConnector();
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector",
            CONNECTOR_NAME,
            "--url",
            "https://adb-123.4.azuredatabricks.net",
            "--warehouse",
            "wh-1",
            "--assessment");

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> connector.validate(arguments));
    assertTrue(exception.getMessage().contains("--assessment"));
  }

  @Test
  public void validate_requiresWarehouse() throws IOException {
    DatabricksHiveMetastoreMetadataConnector connector =
        new DatabricksHiveMetastoreMetadataConnector();
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector", CONNECTOR_NAME, "--url", "https://workspace.cloud.databricks.com");

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> connector.validate(arguments));
    assertTrue(exception.getMessage().contains("--warehouse"));
  }

  @Test
  public void addTasksTo_addsCanonicalCsvAndDdlTasks() throws Exception {
    DatabricksHiveMetastoreMetadataConnector connector =
        new DatabricksHiveMetastoreMetadataConnector();
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector",
            CONNECTOR_NAME,
            "--url",
            "https://workspace.cloud.databricks.com",
            "--warehouse",
            "wh-123");
    ImmutableList<String> expectedEntries =
        ImmutableList.of(
            DatabricksMetadataDumpFormat.CATALOGS_ENTRY_NAME,
            DatabricksMetadataDumpFormat.SCHEMATA_ENTRY_NAME,
            DatabricksMetadataDumpFormat.TABLES_ENTRY_NAME,
            DatabricksMetadataDumpFormat.COLUMNS_ENTRY_NAME,
            DatabricksMetadataDumpFormat.VIEWS_ENTRY_NAME,
            DatabricksMetadataDumpFormat.TABLE_CONSTRAINTS_ENTRY_NAME,
            DatabricksMetadataDumpFormat.FUNCTIONS_ENTRY_NAME,
            DatabricksMetadataDumpFormat.DDLS_ENTRY_NAME);
    int metadataAndFormatTaskCount = 2;

    List<Task<?>> tasks = new ArrayList<>();
    connector.addTasksTo(tasks, arguments);

    assertEquals(metadataAndFormatTaskCount + expectedEntries.size(), tasks.size());
    List<String> targetPaths = new ArrayList<>();
    for (Task<?> task : tasks) {
      targetPaths.add(task.getTargetPath());
    }

    assertTrue(targetPaths.containsAll(expectedEntries));
  }
}
