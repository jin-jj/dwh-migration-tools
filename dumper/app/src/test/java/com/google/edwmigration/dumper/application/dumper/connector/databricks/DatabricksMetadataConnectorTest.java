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

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksFilter.HIVE_METASTORE_CATALOG;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataConnector.DatabricksProperty.HIVE_METASTORE_ONLY;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataConnector.DatabricksProperty.MAX_CONCURRENT_STATEMENTS;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataConnector.MAX_CONCURRENT_STATEMENTS_RANGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.ConnectorRepository;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import com.google.edwmigration.dumper.application.dumper.task.DumpMetadataTask;
import com.google.edwmigration.dumper.application.dumper.task.FormatTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
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
          DatabricksMetadataDumpFormat.UNITY_CATALOG_FUNCTIONS_ENTRY_NAME);

  private static final ImmutableList<String> HIVE_METASTORE_ENTRIES =
      ImmutableList.of(
          DatabricksMetadataDumpFormat.HIVE_METASTORE_CATALOGS_ENTRY_NAME,
          DatabricksMetadataDumpFormat.HIVE_METASTORE_SCHEMATA_ENTRY_NAME,
          DatabricksMetadataDumpFormat.HIVE_METASTORE_TABLES_ENTRY_NAME,
          DatabricksMetadataDumpFormat.HIVE_METASTORE_COLUMNS_ENTRY_NAME,
          DatabricksMetadataDumpFormat.HIVE_METASTORE_VIEWS_ENTRY_NAME,
          DatabricksMetadataDumpFormat.HIVE_METASTORE_TABLE_CONSTRAINTS_ENTRY_NAME,
          DatabricksMetadataDumpFormat.HIVE_METASTORE_DDLS_ENTRY_NAME,
          DatabricksMetadataDumpFormat.HIVE_METASTORE_FUNCTIONS_ENTRY_NAME,
          DatabricksMetadataDumpFormat.HIVE_METASTORE_FUNCTION_DDLS_ENTRY_NAME);

  private final DatabricksMetadataConnector connector = new DatabricksMetadataConnector();

  @Test
  public void connector_isDiscoverableViaConnectorRepository() {
    assertNotNull(ConnectorRepository.getInstance().getByName(CONNECTOR_NAME));
  }

  @Test
  public void formatName_isSpecificToConnector() {
    assertEquals(CONNECTOR_NAME + ".dump.zip", DatabricksMetadataDumpFormat.FORMAT_NAME);
  }

  @Test
  public void validate_rejectsAssessmentFlag() throws IOException {
    ConnectorArguments arguments = arguments("--assessment");

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> connector.validate(arguments));
    assertTrue(exception.getMessage().contains("--assessment"));
  }

  @Test
  public void validate_requiresWarehouse() throws IOException {
    ConnectorArguments arguments =
        new ConnectorArguments("--connector", CONNECTOR_NAME, "--url", URL);

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> connector.validate(arguments));
    assertTrue(exception.getMessage().contains("--warehouse"));
  }

  @Test
  public void validate_acceptsMaxConcurrentStatementsInRange() throws IOException {
    connector.validate(
        argumentsWithMaxConcurrentStatements(MAX_CONCURRENT_STATEMENTS_RANGE.lowerEndpoint()));
    connector.validate(
        argumentsWithMaxConcurrentStatements(MAX_CONCURRENT_STATEMENTS_RANGE.upperEndpoint()));
  }

  @Test
  public void validate_rejectsMaxConcurrentStatementsOutOfRange() throws IOException {
    for (long value :
        ImmutableList.of(
            MAX_CONCURRENT_STATEMENTS_RANGE.lowerEndpoint() - 1,
            MAX_CONCURRENT_STATEMENTS_RANGE.upperEndpoint() + 1)) {
      ConnectorArguments arguments = argumentsWithMaxConcurrentStatements(value);

      MetadataDumperUsageException exception =
          assertThrows(MetadataDumperUsageException.class, () -> connector.validate(arguments));
      assertTrue(exception.getMessage().contains(MAX_CONCURRENT_STATEMENTS.getName()));
      assertTrue(exception.getMessage().contains(MAX_CONCURRENT_STATEMENTS_RANGE.toString()));
    }
  }

  @Test
  public void maxConcurrentStatements_isPrefixedWithConnectorName() {
    assertEquals(
        CONNECTOR_NAME + ".max-concurrent-statements", MAX_CONCURRENT_STATEMENTS.getName());
  }

  @Test
  public void validate_acceptsDatabaseListWithHiveMetastore() throws IOException {
    connector.validate(
        arguments("--database", "Main," + HIVE_METASTORE_CATALOG.toUpperCase(Locale.ROOT)));
  }

  @Test
  public void validate_acceptsDatabaseListWithoutHiveMetastore() throws IOException {
    connector.validate(arguments("--database", "main"));
  }

  @Test
  public void validate_rejectsHiveMetastoreOnlyWithDatabaseListWithoutHiveMetastore()
      throws IOException {
    String catalog = "main";
    ConnectorArguments arguments =
        arguments("--database", catalog, hiveMetastoreOnlyDefinition("true"));

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> connector.validate(arguments));
    assertTrue(exception.getMessage().contains(catalog));
    assertTrue(exception.getMessage().contains(HIVE_METASTORE_ONLY.getName()));
  }

  @Test
  public void validate_rejectsHiveMetastoreOnlyThatIsNotABoolean() throws IOException {
    String value = "yes";
    ConnectorArguments arguments = arguments(hiveMetastoreOnlyDefinition(value));

    MetadataDumperUsageException exception =
        assertThrows(MetadataDumperUsageException.class, () -> connector.validate(arguments));
    assertTrue(exception.getMessage().contains(HIVE_METASTORE_ONLY.getName()));
    assertTrue(exception.getMessage().contains(value));
  }

  @Test
  public void hiveMetastoreOnly_isPrefixedWithConnectorName() {
    assertEquals(CONNECTOR_NAME + ".hive-metastore-only", HIVE_METASTORE_ONLY.getName());
  }

  @Test
  public void addTasksTo_addsUnityCatalogAndHiveMetastoreTasksByDefault() throws IOException {
    assertEquals(
        ImmutableMultiset.<String>builder()
            .addAll(UNITY_CATALOG_ENTRIES)
            .addAll(HIVE_METASTORE_ENTRIES)
            .build(),
        dataTargetPaths(arguments()));
  }

  @Test
  public void addTasksTo_addsOnlyHiveMetastoreTasksWhenHiveMetastoreOnly() throws IOException {
    assertEquals(
        ImmutableMultiset.copyOf(HIVE_METASTORE_ENTRIES),
        dataTargetPaths(arguments(hiveMetastoreOnlyDefinition("TRUE"))));
  }

  @Test
  public void addTasksTo_addsBothTasksWhenNotHiveMetastoreOnly() throws IOException {
    assertEquals(
        ImmutableMultiset.<String>builder()
            .addAll(UNITY_CATALOG_ENTRIES)
            .addAll(HIVE_METASTORE_ENTRIES)
            .build(),
        dataTargetPaths(arguments(hiveMetastoreOnlyDefinition("false"))));
  }

  @Test
  public void addTasksTo_addsOnlyUnityCatalogTasksForDatabaseListWithoutHiveMetastore()
      throws IOException {
    assertEquals(
        ImmutableMultiset.copyOf(UNITY_CATALOG_ENTRIES),
        dataTargetPaths(arguments("--database", "main")));
  }

  @Test
  public void addTasksTo_addsOnlyHiveMetastoreTasksForDatabaseListOfHiveMetastore()
      throws IOException {
    assertEquals(
        ImmutableMultiset.copyOf(HIVE_METASTORE_ENTRIES),
        dataTargetPaths(arguments("--database", HIVE_METASTORE_CATALOG.toUpperCase(Locale.ROOT))));
  }

  /** Returns the target paths of the tasks, without those of the metadata and format tasks. */
  private ImmutableMultiset<String> dataTargetPaths(ConnectorArguments arguments) {
    List<Task<?>> tasks = new ArrayList<>();
    connector.addTasksTo(tasks, arguments);
    ImmutableSet<String> bookkeepingPaths =
        ImmutableSet.of(
            new DumpMetadataTask(arguments, DatabricksMetadataDumpFormat.FORMAT_NAME)
                .getTargetPath(),
            new FormatTask(DatabricksMetadataDumpFormat.FORMAT_NAME).getTargetPath());
    ImmutableMultiset.Builder<String> targetPaths = ImmutableMultiset.<String>builder();
    for (Task<?> task : tasks) {
      if (!bookkeepingPaths.contains(task.getTargetPath())) {
        targetPaths.add(task.getTargetPath());
      }
    }
    return targetPaths.build();
  }

  private static String hiveMetastoreOnlyDefinition(String value) {
    return "-D" + HIVE_METASTORE_ONLY.getName() + "=" + value;
  }

  private static ConnectorArguments arguments(String... extraArguments) throws IOException {
    List<String> allArguments = new ArrayList<>();
    allArguments.add("--connector");
    allArguments.add(CONNECTOR_NAME);
    allArguments.add("--url");
    allArguments.add(URL);
    allArguments.add("--warehouse");
    allArguments.add(WAREHOUSE);
    for (String argument : extraArguments) {
      allArguments.add(argument);
    }
    return new ConnectorArguments(allArguments.toArray(new String[0]));
  }

  private static ConnectorArguments argumentsWithMaxConcurrentStatements(long value)
      throws IOException {
    return arguments("-D" + MAX_CONCURRENT_STATEMENTS.getName() + "=" + value);
  }
}
