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
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
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

  /** {@link DumpMetadataTask} and {@link FormatTask}, which every dump carries. */
  private static final int SETUP_TASKS = 2;

  /** catalogs, schemata, tables, columns, views, table constraints and functions. */
  private static final int DATASETS = 7;

  /** catalogs, schemata, tables, columns and views, dumped separately for the legacy metastore. */
  private static final int HIVE_METASTORE_TASKS = 5;

  /** The raw table listing, which only the REST tier can produce. */
  private static final int REST_ONLY_TASKS = 1;

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
  public void validate_assessment_isRejectedAndSaysWhy() throws Exception {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector",
            "databricks",
            "--url",
            "https://dbc-test.cloud.databricks.com",
            "--warehouse",
            "warehouse123",
            "--assessment");

    try {
      connector.validate(arguments);
      fail("--assessment must not be accepted by the databricks connector.");
    } catch (MetadataDumperUsageException e) {
      // A bare rejection would leave the user guessing, so the message has to say what the
      // connector is for and how to proceed.
      assertTrue(e.getMessage(), e.getMessage().contains("--assessment"));
      assertTrue(e.getMessage(), e.getMessage().contains("migration"));
    }
  }

  @Test
  public void validate_assessment_isRejectedByEveryVariant() throws Exception {
    // The variants inherit validate(), so this guards against one of them overriding it later and
    // quietly reopening the hole.
    for (DatabricksConnector variant :
        ImmutableList.of(
            new DatabricksConnector(),
            new DatabricksSqlConnector(),
            new DatabricksSystemMetadataConnector(),
            new DatabricksCatalogMetadataConnector())) {
      ConnectorArguments arguments =
          new ConnectorArguments(
              "--connector",
              variant.getName(),
              "--url",
              "https://dbc-test.cloud.databricks.com",
              "--warehouse",
              "warehouse123",
              "--assessment");
      try {
        variant.validate(arguments);
        fail("--assessment must not be accepted by '" + variant.getName() + "'.");
      } catch (MetadataDumperUsageException e) {
        assertTrue(e.getMessage(), e.getMessage().contains(variant.getName()));
      }
    }
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

    assertEquals(SETUP_TASKS + DATASETS * 3 + REST_ONLY_TASKS + HIVE_METASTORE_TASKS, tasks.size());
    assertTrue(tasks.get(0) instanceof DumpMetadataTask);
    assertTrue(tasks.get(1) instanceof FormatTask);
    assertTrue(tasks.get(2) instanceof DatabricksSystemSqlCatalogsTask);
    assertTrue(tasks.get(3) instanceof DatabricksSqlCatalogsTask);
    assertTrue(tasks.get(4) instanceof DatabricksRestCatalogsTask);
    assertTrue(tasks.get(5) instanceof DatabricksSystemSqlSchemataTask);
    assertTrue(tasks.get(6) instanceof DatabricksSqlSchemataTask);
    assertTrue(tasks.get(7) instanceof DatabricksRestSchemataTask);
    assertTrue(tasks.get(8) instanceof DatabricksSystemSqlTablesTask);
    assertTrue(tasks.get(9) instanceof DatabricksSqlTablesTask);
    assertTrue(tasks.get(10) instanceof DatabricksRestTablesTask);
    assertTrue(tasks.get(11) instanceof DatabricksSystemSqlColumnsTask);
    assertTrue(tasks.get(12) instanceof DatabricksSqlColumnsTask);
    assertTrue(tasks.get(13) instanceof DatabricksRestColumnsTask);
    assertTrue(tasks.get(14) instanceof DatabricksSystemSqlViewsTask);
    assertTrue(tasks.get(15) instanceof DatabricksSqlViewsTask);
    assertTrue(tasks.get(16) instanceof DatabricksRestViewsTask);
    assertTrue(tasks.get(17) instanceof DatabricksSystemSqlTableConstraintsTask);
    assertTrue(tasks.get(18) instanceof DatabricksSqlTableConstraintsTask);
    assertTrue(tasks.get(19) instanceof DatabricksRestTableConstraintsTask);
    assertTrue(tasks.get(20) instanceof DatabricksSystemSqlFunctionsTask);
    assertTrue(tasks.get(21) instanceof DatabricksSqlFunctionsTask);
    assertTrue(tasks.get(22) instanceof DatabricksRestFunctionsTask);
    assertTrue(tasks.get(23) instanceof DatabricksRestRawTablesTask);
    assertTrue(tasks.get(24) instanceof DatabricksHiveMetastoreCatalogsTask);
    assertTrue(tasks.get(25) instanceof DatabricksHiveMetastoreSchemataTask);
    assertTrue(tasks.get(26) instanceof DatabricksHiveMetastoreTablesTask);
    assertTrue(tasks.get(27) instanceof DatabricksHiveMetastoreColumnsTask);
    assertTrue(tasks.get(28) instanceof DatabricksHiveMetastoreViewsTask);

    // The first tier of each dataset is unconditional; the two fallback tiers behind it are gated
    // on their predecessors having failed.
    for (int dataset = 0; dataset < DATASETS; dataset++) {
      int systemTier = SETUP_TASKS + dataset * 3;
      assertEquals(0, tasks.get(systemTier).getConditions().length);
      assertTrue(tasks.get(systemTier + 1).getConditions().length > 0);
      assertTrue(tasks.get(systemTier + 2).getConditions().length > 0);
    }
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

    assertEquals(SETUP_TASKS + DATASETS * 3 + REST_ONLY_TASKS, tasks.size());
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
            "-Ddatabricks.skip-hive-metastore=true");
    List<Task<?>> tasks = new ArrayList<>();
    new DatabricksCatalogMetadataConnector().addTasksTo(tasks, arguments);

    assertEquals(SETUP_TASKS + DATASETS, tasks.size());
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
            "-Ddatabricks.skip-hive-metastore=true");
    List<Task<?>> tasks = new ArrayList<>();
    new DatabricksSystemMetadataConnector().addTasksTo(tasks, arguments);

    assertEquals(SETUP_TASKS + DATASETS, tasks.size());
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
            "-Ddatabricks.skip-hive-metastore=true",
            "-Ddatabricks.metadata.strategy=catalog-only");
    List<Task<?>> tasks = new ArrayList<>();
    connector.addTasksTo(tasks, arguments);

    assertEquals(SETUP_TASKS + DATASETS, tasks.size());
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

    assertEquals(SETUP_TASKS + DATASETS * 3 + REST_ONLY_TASKS + HIVE_METASTORE_TASKS, tasks.size());
    assertTrue(tasks.stream().anyMatch(t -> t instanceof DatabricksHiveMetastoreSchemataTask));
    assertTrue(tasks.stream().anyMatch(t -> t instanceof DatabricksHiveMetastoreTablesTask));
    assertTrue(tasks.stream().anyMatch(t -> t instanceof DatabricksHiveMetastoreColumnsTask));
    assertTrue(tasks.stream().anyMatch(t -> t instanceof DatabricksHiveMetastoreViewsTask));
  }

  /**
   * Guards the removal of a bespoke workaround.
   *
   * <p>{@code hive_metastore} used to be case-folded by a dedicated {@code equalsIgnoreCase} scan
   * of the raw arguments, bolted on beside the case-sensitive predicate. That made this one catalog
   * work while every other catalog stayed broken. The workaround is gone, so this asserts the
   * general fix covers what the special case used to.
   */
  @Test
  public void addTasksTo_withDifferentlyCasedDatabase_stillMatches() throws Exception {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector", "databricks",
            "--url", "https://dbc-test.cloud.databricks.com",
            "--warehouse", "warehouse123",
            "--database", "HIVE_Metastore");
    List<Task<?>> tasks = new ArrayList<>();
    connector.addTasksTo(tasks, arguments);

    assertEquals(SETUP_TASKS + DATASETS * 3 + REST_ONLY_TASKS + HIVE_METASTORE_TASKS, tasks.size());
    assertTrue(tasks.stream().anyMatch(t -> t instanceof DatabricksHiveMetastoreTablesTask));
  }

  /**
   * Databricks identifiers are case-insensitive, so the filter handed to the tasks has to be too.
   *
   * <p>The shared {@code getDatabasePredicate()} compares with {@code equals}, so before the fix
   * {@code --database MyCatalog} selected nothing at all and the dump quietly wrote empty files.
   */
  @Test
  public void addTasksTo_withDifferentlyCasedDatabase_passesAFoldingFilter() throws Exception {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector", "databricks",
            "--url", "https://dbc-test.cloud.databricks.com",
            "--warehouse", "warehouse123",
            "--database", "MyCatalog");
    List<Task<?>> tasks = new ArrayList<>();
    connector.addTasksTo(tasks, arguments);

    DatabricksRestSchemataTask task =
        tasks.stream()
            .filter(t -> t instanceof DatabricksRestSchemataTask)
            .map(t -> (DatabricksRestSchemataTask) t)
            .findFirst()
            .orElseThrow(() -> new AssertionError("No REST schemata task was added."));

    assertTrue("Should match the catalog as spelled", task.filter.matchesCatalog("MyCatalog"));
    assertTrue("Should match the lower-cased catalog", task.filter.matchesCatalog("mycatalog"));
    assertTrue("Should match the upper-cased catalog", task.filter.matchesCatalog("MYCATALOG"));
    assertFalse("Should not match an unrelated catalog", task.filter.matchesCatalog("other"));
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

    assertEquals(SETUP_TASKS + DATASETS * 3 + REST_ONLY_TASKS, tasks.size());
    assertTrue(tasks.get(2) instanceof DatabricksSystemSqlCatalogsTask);
    assertTrue(tasks.get(3) instanceof DatabricksSqlCatalogsTask);
    assertFalse(tasks.stream().anyMatch(t -> t instanceof DatabricksHiveMetastoreTablesTask));
  }

  @Test
  public void addTasksTo_restOnlyStrategy_addsOnlyRestTasks() throws Exception {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector",
            "databricks",
            "--url",
            "https://dbc-test.cloud.databricks.com",
            "-Ddatabricks.skip-hive-metastore=true",
            "-Ddatabricks.metadata.strategy=rest-only");
    List<Task<?>> tasks = new ArrayList<>();
    connector.addTasksTo(tasks, arguments);

    assertEquals(SETUP_TASKS + DATASETS + REST_ONLY_TASKS, tasks.size());
    assertTrue(tasks.get(2) instanceof DatabricksRestCatalogsTask);
    assertTrue(tasks.get(3) instanceof DatabricksRestSchemataTask);
    assertTrue(tasks.get(4) instanceof DatabricksRestTablesTask);
    assertTrue(tasks.get(5) instanceof DatabricksRestColumnsTask);
    assertTrue(tasks.get(6) instanceof DatabricksRestViewsTask);
    assertTrue(tasks.get(7) instanceof DatabricksRestTableConstraintsTask);
    assertTrue(tasks.get(8) instanceof DatabricksRestFunctionsTask);
    assertTrue(tasks.get(9) instanceof DatabricksRestRawTablesTask);
    assertFalse(tasks.stream().anyMatch(t -> t instanceof AbstractDatabricksSqlTask));
    // The REST tier never runs behind another tier, so it must not be gated on anything.
    assertEquals(0, tasks.get(2).getConditions().length);
  }

  @Test
  public void validate_restOnlyStrategyWithoutWarehouse_succeeds() throws Exception {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector",
            "databricks",
            "--url",
            "https://dbc-test.cloud.databricks.com",
            "-Ddatabricks.metadata.strategy=rest-only");
    connector.validate(arguments);
  }

  @Test
  public void getPropertyConstants_returnsStrategyAndSkipHiveMetastore() {
    assertNotNull(connector.getPropertyConstants());
    assertEquals(
        ImmutableList.of(
            DatabricksConnector.DatabricksConnectorProperty.STRATEGY,
            DatabricksConnector.DatabricksConnectorProperty.REST_REQUESTS_PER_SECOND,
            DatabricksConnector.DatabricksConnectorProperty.SKIP_HIVE_METASTORE),
        ImmutableList.copyOf(connector.getPropertyConstants()));
  }

  /**
   * The raw listing is worth nothing if it costs a metastore walk on a healthy run.
   *
   * <p>It shares the REST tier's walk, so as long as it is gated on the same condition as the other
   * REST tasks it is free. Ungated, it would walk the whole metastore over the REST API on every
   * dump, including the overwhelming majority where the SQL tiers succeed and no other REST task
   * runs at all.
   */
  @Test
  public void addTasksTo_defaultArguments_gatesTheRawListingBehindBothSqlTiers() throws Exception {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector", "databricks",
            "--url", "https://dbc-test.cloud.databricks.com",
            "--warehouse", "warehouse123");
    List<Task<?>> tasks = new ArrayList<>();
    connector.addTasksTo(tasks, arguments);

    Task<?> rawTables = onlyTaskOfType(tasks, DatabricksRestRawTablesTask.class);
    Task<?> restTables = onlyTaskOfType(tasks, DatabricksRestTablesTask.class);
    assertEquals(
        "The raw listing should be gated exactly as the REST tables task is",
        ImmutableList.copyOf(restTables.getConditions()).size(),
        ImmutableList.copyOf(rawTables.getConditions()).size());
    assertTrue(rawTables.getConditions().length > 0);

    // The SQL tiers must not be gated twice over by the second call that adds the raw listing.
    Task<?> systemTables = onlyTaskOfType(tasks, DatabricksSystemSqlTablesTask.class);
    Task<?> catalogTables = onlyTaskOfType(tasks, DatabricksSqlTablesTask.class);
    assertEquals(0, systemTables.getConditions().length);
    assertEquals(1, catalogTables.getConditions().length);
  }

  @Test
  public void addTasksTo_restOnlyStrategy_addsTheRawListingUngated() throws Exception {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector",
            "databricks",
            "--url",
            "https://dbc-test.cloud.databricks.com",
            "-Ddatabricks.skip-hive-metastore=true",
            "-Ddatabricks.metadata.strategy=rest-only");
    List<Task<?>> tasks = new ArrayList<>();
    connector.addTasksTo(tasks, arguments);

    assertEquals(
        0, onlyTaskOfType(tasks, DatabricksRestRawTablesTask.class).getConditions().length);
  }

  @Test
  public void addTasksTo_strategiesThatNeverReachRest_omitTheRawListing() throws Exception {
    for (String strategy : new String[] {"system-only", "catalog-only", "system-then-catalog"}) {
      ConnectorArguments arguments =
          new ConnectorArguments(
              "--connector",
              "databricks",
              "--url",
              "https://dbc-test.cloud.databricks.com",
              "--warehouse",
              "warehouse123",
              "-Ddatabricks.skip-hive-metastore=true",
              "-Ddatabricks.metadata.strategy=" + strategy);
      List<Task<?>> tasks = new ArrayList<>();
      connector.addTasksTo(tasks, arguments);

      assertFalse(
          strategy + " should not produce a raw listing",
          tasks.stream().anyMatch(t -> t instanceof DatabricksRestRawTablesTask));
    }
  }

  private static Task<?> onlyTaskOfType(List<Task<?>> tasks, Class<?> type) {
    List<Task<?>> found = new ArrayList<>();
    for (Task<?> task : tasks) {
      if (type.isInstance(task)) {
        found.add(task);
      }
    }
    assertEquals("Expected exactly one " + type.getSimpleName(), 1, found.size());
    return found.get(0);
  }
}
