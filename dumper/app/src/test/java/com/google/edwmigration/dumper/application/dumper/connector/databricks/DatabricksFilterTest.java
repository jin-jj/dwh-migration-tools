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
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Covers the two renderings of the catalog and schema filter.
 *
 * <p>The point of most of these is that {@link DatabricksFilter#whereClause} and {@link
 * DatabricksFilter#matchesCatalog} agree. A disagreement would not fail loudly: it would show up as
 * a dump whose contents depend on which tier of the fallback chain happened to run.
 */
@RunWith(JUnit4.class)
public class DatabricksFilterTest {

  private static final ImmutableList<String> NONE = ImmutableList.of();

  private static final String CATALOG_COLUMN = "table_catalog";
  private static final String SCHEMA_COLUMN = "table_schema";

  @Test
  public void whereClause_withoutRestrictions_isEmpty() {
    DatabricksFilter filter = DatabricksFilter.all();

    assertEquals("", filter.whereClause(CATALOG_COLUMN, SCHEMA_COLUMN));
  }

  @Test
  public void whereClause_withCatalogs_restrictsTheCatalogColumn() {
    DatabricksFilter filter = new DatabricksFilter(ImmutableList.of("main", "prod"), NONE, NONE);

    assertEquals(
        " WHERE lower(" + CATALOG_COLUMN + ") IN ('main', 'prod')",
        filter.whereClause(CATALOG_COLUMN, SCHEMA_COLUMN));
  }

  @Test
  public void whereClause_withSchemata_restrictsTheSchemaColumn() {
    DatabricksFilter filter = new DatabricksFilter(NONE, ImmutableList.of("sales"), NONE);

    assertEquals(
        " WHERE lower(" + SCHEMA_COLUMN + ") IN ('sales')",
        filter.whereClause(CATALOG_COLUMN, SCHEMA_COLUMN));
  }

  @Test
  public void whereClause_withExclusions_negatesThem() {
    DatabricksFilter filter =
        new DatabricksFilter(NONE, NONE, ImmutableList.of("samples", "system"));

    assertEquals(
        " WHERE lower(" + CATALOG_COLUMN + ") NOT IN ('samples', 'system')",
        filter.whereClause(CATALOG_COLUMN, SCHEMA_COLUMN));
  }

  @Test
  public void whereClause_withEverything_conjoinsTheRestrictions() {
    DatabricksFilter filter =
        new DatabricksFilter(
            ImmutableList.of("main"), ImmutableList.of("sales"), ImmutableList.of("system"));

    assertEquals(
        " WHERE lower("
            + CATALOG_COLUMN
            + ") IN ('main') AND lower("
            + CATALOG_COLUMN
            + ") NOT IN ('system') AND lower("
            + SCHEMA_COLUMN
            + ") IN ('sales')",
        filter.whereClause(CATALOG_COLUMN, SCHEMA_COLUMN));
  }

  /**
   * The per-catalog tier runs one statement per catalog, so its statements are already scoped and
   * must not carry a catalog restriction.
   */
  @Test
  public void whereClause_withoutACatalogColumn_omitsTheCatalogRestriction() {
    DatabricksFilter filter =
        new DatabricksFilter(
            ImmutableList.of("main"), ImmutableList.of("sales"), ImmutableList.of("system"));

    assertEquals(
        " WHERE lower(" + SCHEMA_COLUMN + ") IN ('sales')",
        filter.whereClause(/* catalogColumn= */ null, SCHEMA_COLUMN));
  }

  /** A query over catalogs has no schema column to restrict. */
  @Test
  public void whereClause_withoutASchemaColumn_omitsTheSchemaRestriction() {
    DatabricksFilter filter = new DatabricksFilter(NONE, ImmutableList.of("sales"), NONE);

    assertEquals("", filter.whereClause(CATALOG_COLUMN, /* schemaColumn= */ null));
  }

  /**
   * Unity Catalog stores and matches names in lower case, so the literals have to be folded or a
   * differently cased {@code --database} would select nothing.
   */
  @Test
  public void whereClause_withMixedCaseNames_foldsThemToMatchTheJavaFilter() {
    DatabricksFilter filter = new DatabricksFilter(ImmutableList.of("MyCatalog"), NONE, NONE);

    assertEquals(
        " WHERE lower(" + CATALOG_COLUMN + ") IN ('mycatalog')",
        filter.whereClause(CATALOG_COLUMN, SCHEMA_COLUMN));
    assertTrue(
        "The Java filter must accept what the clause accepts", filter.matchesCatalog("MYCATALOG"));
  }

  @Test
  public void stringLiteral_withAQuote_doublesIt() {
    String name = "o'brien";

    assertEquals("'o''brien'", DatabricksFilter.stringLiteral(name));
  }

  @Test
  public void stringLiteral_withABackslash_doublesIt() {
    String name = "a\\b";

    assertEquals("'a\\\\b'", DatabricksFilter.stringLiteral(name));
  }

  @Test
  public void matchesCatalog_withoutRestrictions_acceptsAnything() {
    DatabricksFilter filter = DatabricksFilter.all();

    assertTrue(filter.matchesCatalog("anything"));
    assertTrue(filter.matchesSchema("anything"));
  }

  @Test
  public void matchesCatalog_withNull_isFalse() {
    assertFalse(DatabricksFilter.all().matchesCatalog(null));
    assertFalse(DatabricksFilter.all().matchesSchema(null));
  }

  /**
   * An excluded catalog stays excluded even if it was also named, which is what the composed
   * predicate this replaced did. The combination only arises from contradictory arguments, such as
   * naming {@code hive_metastore} while also asking to skip it.
   */
  @Test
  public void matchesCatalog_withANamedButExcludedCatalog_isFalse() {
    String name = DatabricksCatalogNames.HIVE_METASTORE;
    DatabricksFilter filter =
        new DatabricksFilter(ImmutableList.of(name), NONE, ImmutableList.of(name));

    assertFalse(filter.matchesCatalog(name));
  }
}
