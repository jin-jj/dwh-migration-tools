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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksFilterTest {

  @Test
  public void defaultFilter_acceptsAllSchemasExceptInformationSchema() {
    DatabricksFilter filter = new DatabricksFilter(ImmutableList.of(), ImmutableList.of());

    assertTrue(filter.acceptsSchema("default"));
    assertTrue(filter.acceptsSchema("Sales_Schema"));
    assertFalse(filter.acceptsSchema("information_schema"));
    assertFalse(filter.acceptsSchema("INFORMATION_SCHEMA"));
    assertFalse(filter.acceptsSchema(""));
    assertFalse(filter.acceptsSchema(null));
  }

  @Test
  public void defaultFilter_acceptsAllCatalogsExceptDatabricksManagedCatalogs() {
    DatabricksFilter filter = new DatabricksFilter(ImmutableList.of(), ImmutableList.of());

    assertTrue(filter.acceptsCatalog(HIVE_METASTORE_CATALOG));
    assertTrue(filter.acceptsCatalog("Main"));
    assertFalse(filter.acceptsCatalog("system"));
    assertFalse(filter.acceptsCatalog("Samples"));
    assertFalse(filter.acceptsCatalog("__Databricks_Internal"));
    assertFalse(filter.acceptsCatalog(" "));
    assertFalse(filter.acceptsCatalog(null));
  }

  @Test
  public void catalogFilter_acceptsDatabricksManagedCatalogsWhenListed() {
    DatabricksFilter filter =
        new DatabricksFilter(
            ImmutableList.of("System", "samples", "__databricks_internal"), ImmutableList.of());

    assertTrue(filter.acceptsCatalog("system"));
    assertTrue(filter.acceptsCatalog("SAMPLES"));
    assertTrue(filter.acceptsCatalog("__DATABRICKS_INTERNAL"));
    assertFalse(filter.acceptsCatalog("main"));
  }

  @Test
  public void schemaFilter_foldsCase() {
    DatabricksFilter filter =
        new DatabricksFilter(ImmutableList.of(), ImmutableList.of("Core_Schema"));

    assertTrue(filter.acceptsSchema("core_schema"));
    assertTrue(filter.acceptsSchema("CORE_SCHEMA"));
    assertFalse(filter.acceptsSchema("default"));
    assertFalse(filter.acceptsSchema("information_schema"));
  }

  @Test
  public void catalogFilter_foldsCaseAndTrims() {
    DatabricksFilter filter =
        new DatabricksFilter(ImmutableList.of(" Hive_Metastore ", "Main"), ImmutableList.of());

    assertTrue(filter.acceptsCatalog(HIVE_METASTORE_CATALOG));
    assertTrue(filter.acceptsCatalog("MAIN"));
    assertFalse(filter.acceptsCatalog("samples"));
  }

  @Test
  public void acceptsAnyCatalogExcept_isTrueForEmptyOrOtherCatalogs() {
    assertTrue(
        new DatabricksFilter(ImmutableList.of(), ImmutableList.of())
            .acceptsAnyCatalogExcept(HIVE_METASTORE_CATALOG));
    assertTrue(
        new DatabricksFilter(ImmutableList.of(HIVE_METASTORE_CATALOG, "main"), ImmutableList.of())
            .acceptsAnyCatalogExcept(HIVE_METASTORE_CATALOG));
  }

  @Test
  public void acceptsAnyCatalogExcept_isFalseWhenOnlyThatCatalogIsListed() {
    DatabricksFilter filter =
        new DatabricksFilter(ImmutableList.of(" Hive_Metastore "), ImmutableList.of());

    assertFalse(filter.acceptsAnyCatalogExcept(HIVE_METASTORE_CATALOG));
  }

  @Test
  public void fromArguments_readsDatabaseAndSchemaFlags() throws Exception {
    ConnectorArguments arguments =
        new ConnectorArguments(
            "--connector",
            DatabricksMetadataConnector.CONNECTOR_NAME,
            "--database",
            "main,hive_metastore",
            "--schema",
            "sales");

    DatabricksFilter filter = DatabricksFilter.fromArguments(arguments);

    assertTrue(filter.acceptsCatalog(HIVE_METASTORE_CATALOG));
    assertTrue(filter.acceptsCatalog("main"));
    assertFalse(filter.acceptsCatalog("dev"));
    assertTrue(filter.acceptsSchema("sales"));
    assertFalse(filter.acceptsSchema("default"));
  }
}
