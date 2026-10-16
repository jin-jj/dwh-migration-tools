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

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.google.common.collect.Iterables.getLast;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.DatabricksFilter.HIVE_METASTORE_CATALOG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ObjectArrays;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.ConnectorRepository;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.DatabricksHandle;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata.DatabricksHandle.MetadataTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.application.dumper.task.TaskSetState;
import com.google.edwmigration.dumper.application.dumper.task.TaskState;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksMetadataConnectorTest {

  private static final String CONNECTOR_NAME = DatabricksMetadataConnector.CONNECTOR_NAME;
  private static final String URL = "https://workspace.cloud.databricks.com";
  private static final String WAREHOUSE = "wh-123";

  private static final ImmutableList<String> UNITY_CATALOG_ENTRIES =
      ImmutableList.of(
          DatabricksMetadataDumpFormat.UNITY_CATALOG_CATALOGS_ENTRY_NAME,
          DatabricksMetadataDumpFormat.UNITY_CATALOG_SCHEMATA_ENTRY_NAME,
          DatabricksMetadataDumpFormat.UNITY_CATALOG_TABLES_ENTRY_NAME,
          DatabricksMetadataDumpFormat.UNITY_CATALOG_COLUMNS_ENTRY_NAME,
          DatabricksMetadataDumpFormat.UNITY_CATALOG_VIEWS_ENTRY_NAME,
          DatabricksMetadataDumpFormat.UNITY_CATALOG_TABLE_CONSTRAINTS_ENTRY_NAME,
          DatabricksMetadataDumpFormat.UNITY_CATALOG_COLUMN_MASKS_ENTRY_NAME,
          DatabricksMetadataDumpFormat.UNITY_CATALOG_ROW_FILTERS_ENTRY_NAME);

  private static final ImmutableList<String> HIVE_METASTORE_ENTRIES =
      ImmutableList.of(
          DatabricksMetadataDumpFormat.HIVE_METASTORE_CATALOGS_ENTRY_NAME,
          DatabricksMetadataDumpFormat.HIVE_METASTORE_SCHEMATA_ENTRY_NAME,
          DatabricksMetadataDumpFormat.HIVE_METASTORE_TABLES_ENTRY_NAME,
          DatabricksMetadataDumpFormat.HIVE_METASTORE_COLUMNS_ENTRY_NAME,
          DatabricksMetadataDumpFormat.HIVE_METASTORE_VIEWS_ENTRY_NAME);

  private static WireMockServer server;

  private final DatabricksMetadataConnector connector = new DatabricksMetadataConnector();

  @BeforeClass
  public static void startServer() {
    server = new WireMockServer(wireMockConfig().dynamicPort());
    server.start();
  }

  @AfterClass
  public static void stopServer() {
    server.stop();
  }

  @Before
  public void resetServer() {
    server.resetAll();
  }

  @Test
  public void validate_checksFlagsAndArguments() throws IOException {
    assertNotNull(ConnectorRepository.getInstance().getByName(CONNECTOR_NAME));
    assertEquals(CONNECTOR_NAME + ".dump.zip", DatabricksMetadataDumpFormat.FORMAT_NAME);
    assertTrue(
        assertThrows(
                MetadataDumperUsageException.class,
                () -> connector.validate(arguments("--assessment")))
            .getMessage()
            .contains("--assessment"));
    assertTrue(
        assertThrows(
                MetadataDumperUsageException.class,
                () -> connector.validate(new ConnectorArguments("--connector", CONNECTOR_NAME)))
            .getMessage()
            .contains("--url"));
  }

  @Test
  public void validate_withoutWarehouse_rejectsOnlyHiveMetastore() throws IOException {
    connector.validate(argumentsWithoutWarehouse());
    connector.validate(argumentsWithoutWarehouse("--database", "main"));
    connector.validate(argumentsWithoutWarehouse("--database", "main," + HIVE_METASTORE_CATALOG));

    ConnectorArguments hiveMetastoreOnly =
        argumentsWithoutWarehouse("--database", HIVE_METASTORE_CATALOG.toUpperCase(Locale.ROOT));
    MetadataDumperUsageException exception =
        assertThrows(
            MetadataDumperUsageException.class, () -> connector.validate(hiveMetastoreOnly));
    assertTrue(exception.getMessage(), exception.getMessage().contains("--warehouse"));
    assertTrue(exception.getMessage(), exception.getMessage().contains(HIVE_METASTORE_CATALOG));
  }

  @Test
  public void open_createsSqlHelperOnlyWithWarehouse() throws Exception {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector",
            CONNECTOR_NAME,
            "--url",
            server.baseUrl(),
            "--password",
            "dapi-token",
            "--warehouse",
            WAREHOUSE);
    try (DatabricksHandle handle = (DatabricksHandle) connector.open(arguments)) {
      assertNotNull(handle.sqlHelper());
    }

    ConnectorArguments withoutWarehouse =
        new ConnectorArguments(
            "--connector", CONNECTOR_NAME, "--url", server.baseUrl(), "--password", "dapi-token");
    try (DatabricksHandle handle = (DatabricksHandle) connector.open(withoutWarehouse)) {
      assertNull(handle.sqlHelper());
    }
  }

  @Test
  public void addTasksTo_selectsCatalogsAndCategories() throws Exception {
    ImmutableMultiset<String> allEntries =
        ImmutableMultiset.<String>builder()
            .addAll(UNITY_CATALOG_ENTRIES)
            .addAll(HIVE_METASTORE_ENTRIES)
            .build();
    assertEquals(allEntries, dataTargetPaths(arguments()));
    assertEquals(
        ImmutableMultiset.copyOf(UNITY_CATALOG_ENTRIES),
        dataTargetPaths(arguments("--database", "main")));
    assertEquals(
        ImmutableMultiset.copyOf(HIVE_METASTORE_ENTRIES),
        dataTargetPaths(arguments("--database", HIVE_METASTORE_CATALOG.toUpperCase(Locale.ROOT))));
    // Without a SQL warehouse, the hive_metastore catalog is skipped.
    assertEquals(
        ImmutableMultiset.copyOf(UNITY_CATALOG_ENTRIES),
        dataTargetPaths(argumentsWithoutWarehouse()));
    assertEquals(
        ImmutableMultiset.copyOf(UNITY_CATALOG_ENTRIES),
        dataTargetPaths(argumentsWithoutWarehouse("--database", "main," + HIVE_METASTORE_CATALOG)));

    List<Task<?>> tasks = new ArrayList<>();
    connector.addTasksTo(tasks, arguments());
    ImmutableSet<String> requiredTaskNames =
        ImmutableSet.of(
            new DumpMetadataTask(arguments(), DatabricksMetadataDumpFormat.FORMAT_NAME).getName(),
            new FormatTask(DatabricksMetadataDumpFormat.FORMAT_NAME).getName(),
            MetadataTask.CHECK_TASK_TARGET_PATH);
    for (Task<?> task : tasks) {
      assertEquals(
          task.getName(),
          requiredTaskNames.contains(task.getName())
              ? TaskCategory.REQUIRED
              : TaskCategory.OPTIONAL,
          task.getCategory());
    }

    // The Unity Catalog tasks keep their all-failed check task without a SQL warehouse.
    List<Task<?>> tasksWithoutWarehouse = new ArrayList<>();
    connector.addTasksTo(tasksWithoutWarehouse, argumentsWithoutWarehouse());
    assertEquals(
        MetadataTask.CHECK_TASK_TARGET_PATH, getLast(tasksWithoutWarehouse).getTargetPath());
  }

  @Test
  public void allFailedCheckTask_failsOnlyWhenEveryMetadataTaskFails() throws Exception {
    List<Task<?>> tasks = new ArrayList<>();
    connector.addTasksTo(tasks, arguments());
    Task<?> checkTask = getLast(tasks);
    TaskSetState partialFailure = mock(TaskSetState.class);
    when(partialFailure.getTaskState(any())).thenReturn(TaskState.FAILED, TaskState.SUCCEEDED);
    assertFalse(checkTask.getConditions()[0].evaluate(partialFailure));

    TaskSetState allFailed = mock(TaskSetState.class);
    when(allFailed.getTaskState(any())).thenReturn(TaskState.FAILED);
    assertTrue(checkTask.getConditions()[0].evaluate(allFailed));

    IOException thrown =
        assertThrows(IOException.class, () -> checkTask.run(mock(TaskRunContext.class)));
    assertTrue(
        thrown.getMessage(), thrown.getMessage().contains("All Databricks metadata tasks failed."));
  }

  private ImmutableMultiset<String> dataTargetPaths(ConnectorArguments arguments) {
    List<Task<?>> tasks = new ArrayList<>();
    connector.addTasksTo(tasks, arguments);
    ImmutableSet<String> bookkeepingPaths =
        ImmutableSet.of(
            new DumpMetadataTask(arguments, DatabricksMetadataDumpFormat.FORMAT_NAME)
                .getTargetPath(),
            new FormatTask(DatabricksMetadataDumpFormat.FORMAT_NAME).getTargetPath(),
            MetadataTask.CHECK_TASK_TARGET_PATH);
    ImmutableMultiset.Builder<String> targetPaths = ImmutableMultiset.builder();
    for (Task<?> task : tasks) {
      if (!bookkeepingPaths.contains(task.getTargetPath())) {
        targetPaths.add(task.getTargetPath());
      }
    }
    return targetPaths.build();
  }

  private static ConnectorArguments arguments(String... extraArguments) throws IOException {
    return argumentsWithoutWarehouse(
        ObjectArrays.concat(new String[] {"--warehouse", WAREHOUSE}, extraArguments, String.class));
  }

  private static ConnectorArguments argumentsWithoutWarehouse(String... extraArguments)
      throws IOException {
    return new ConnectorArguments(
        ObjectArrays.concat(
            new String[] {"--connector", CONNECTOR_NAME, "--url", URL},
            extraArguments,
            String.class));
  }
}
