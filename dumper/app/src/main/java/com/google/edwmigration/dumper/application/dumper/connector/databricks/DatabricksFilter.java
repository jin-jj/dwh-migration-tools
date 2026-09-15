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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * The set of catalogs and schemas the user asked for, in both of the forms the connector needs.
 *
 * <p>A dump is restricted by {@code --database} and {@code --schema}. Those restrictions have to be
 * applied in two different ways depending on how a task reads its data. A task that issues a query
 * against an {@code information_schema} view should express them as a {@code WHERE} clause, so that
 * the warehouse never materialises the rows the dump is going to throw away. A task that reads the
 * REST API, or one that scrapes a {@code SHOW} command, has no such clause available and has to
 * test each name as it arrives.
 *
 * <p>Holding both forms in one object is what keeps them honest: {@link #whereClause} and {@link
 * #matchesCatalog} are two renderings of the same sets, so the tiers of the fallback chain cannot
 * disagree about which catalogs belong in the dump. The SQL rendering lower-cases the column rather
 * than relying on Unity Catalog's convention of storing names in lower case, which makes it an
 * exact translation of the Java comparison rather than merely an equivalent one.
 *
 * <p>Filtering is applied a second time in Java even when it has been pushed into SQL. That is
 * deliberate redundancy: it costs a set lookup per row, and it means a statement that is later
 * edited without its {@code WHERE} placeholder still produces a correctly scoped dump.
 */
final class DatabricksFilter {

  /** Placeholder in a statement template, replaced by the generated {@code WHERE} clause. */
  static final String WHERE = "$where";

  private final ImmutableSet<String> catalogs;
  private final ImmutableSet<String> schemata;
  private final ImmutableSet<String> excludedCatalogs;

  /**
   * @param catalogs the catalogs to dump, or empty to dump all of them.
   * @param schemata the schemas to dump, or empty to dump all of them.
   * @param excludedCatalogs catalogs to drop even if they match, such as {@code system}.
   */
  DatabricksFilter(
      @Nonnull Collection<String> catalogs,
      @Nonnull Collection<String> schemata,
      @Nonnull Collection<String> excludedCatalogs) {
    Preconditions.checkNotNull(catalogs, "Catalogs were null.");
    Preconditions.checkNotNull(schemata, "Schemata were null.");
    Preconditions.checkNotNull(excludedCatalogs, "Excluded catalogs were null.");
    this.catalogs = fold(catalogs);
    this.schemata = fold(schemata);
    this.excludedCatalogs = fold(excludedCatalogs);
  }

  /** Returns a filter that accepts everything. */
  @Nonnull
  static DatabricksFilter all() {
    return new DatabricksFilter(
        ImmutableSet.<String>of(), ImmutableSet.<String>of(), ImmutableSet.<String>of());
  }

  @Nonnull
  private static ImmutableSet<String> fold(Collection<String> names) {
    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    for (String name : names) {
      if (name != null) {
        builder.add(name.toLowerCase(Locale.ROOT));
      }
    }
    return builder.build();
  }

  /**
   * Returns whether {@code name} is a catalog the user asked for.
   *
   * <p>Unity Catalog lower-cases the names it is given, and matches them without regard to case, so
   * a filter that did not fold case would reject {@code --database MyCatalog} outright and produce
   * an empty dump rather than an error.
   */
  boolean matchesCatalog(@CheckForNull String name) {
    if (name == null) {
      return false;
    }
    String folded = name.toLowerCase(Locale.ROOT);
    if (excludedCatalogs.contains(folded)) {
      return false;
    }
    return catalogs.isEmpty() || catalogs.contains(folded);
  }

  /** Returns whether {@code name} is a schema the user asked for. */
  boolean matchesSchema(@CheckForNull String name) {
    if (name == null) {
      return false;
    }
    return schemata.isEmpty() || schemata.contains(name.toLowerCase(Locale.ROOT));
  }

  @Nonnull
  Predicate<String> catalogPredicate() {
    return this::matchesCatalog;
  }

  @Nonnull
  Predicate<String> schemaPredicate() {
    return this::matchesSchema;
  }

  /** Returns whether any catalog was named explicitly, as opposed to the dump covering them all. */
  boolean hasCatalogs() {
    return !catalogs.isEmpty();
  }

  /**
   * Returns the {@code WHERE} clause restricting a query to the requested catalogs and schemas, or
   * an empty string if it would restrict nothing.
   *
   * <p>The returned clause is prefixed with a space and the {@code WHERE} keyword so that it can be
   * substituted directly into a statement template.
   *
   * @param catalogColumn the column holding the catalog name, or null if the query is already
   *     scoped to a single catalog and needs no catalog restriction.
   * @param schemaColumn the column holding the schema name, or null if the query has no schema to
   *     restrict, as for a query over catalogs.
   */
  @Nonnull
  String whereClause(@CheckForNull String catalogColumn, @CheckForNull String schemaColumn) {
    List<String> conditions = new ArrayList<>();
    if (catalogColumn != null) {
      if (!catalogs.isEmpty()) {
        conditions.add(in(catalogColumn, catalogs, /* negated= */ false));
      }
      if (!excludedCatalogs.isEmpty()) {
        conditions.add(in(catalogColumn, excludedCatalogs, /* negated= */ true));
      }
    }
    if (schemaColumn != null && !schemata.isEmpty()) {
      conditions.add(in(schemaColumn, schemata, /* negated= */ false));
    }
    if (conditions.isEmpty()) {
      return "";
    }
    StringBuilder clause = new StringBuilder(" WHERE ");
    for (int i = 0; i < conditions.size(); i++) {
      if (i > 0) {
        clause.append(" AND ");
      }
      clause.append(conditions.get(i));
    }
    return clause.toString();
  }

  @Nonnull
  private static String in(String column, ImmutableSet<String> values, boolean negated) {
    StringBuilder condition = new StringBuilder("lower(").append(column).append(')');
    condition.append(negated ? " NOT IN (" : " IN (");
    boolean first = true;
    for (String value : values) {
      if (!first) {
        condition.append(", ");
      }
      condition.append(stringLiteral(value));
      first = false;
    }
    return condition.append(')').toString();
  }

  /**
   * Renders {@code value} as a SQL string literal.
   *
   * <p>Databricks SQL treats a backslash as an escape character inside a literal unless {@code
   * spark.sql.parser.escapedStringLiterals} has been enabled, so both the backslash and the quote
   * have to be doubled. Catalog and schema names containing either are pathological, but a name is
   * user data and interpolating it unescaped would be an injection.
   */
  @Nonnull
  static String stringLiteral(@Nonnull String value) {
    Preconditions.checkNotNull(value, "Value was null.");
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'";
  }

  @Override
  public String toString() {
    return "DatabricksFilter(catalogs="
        + catalogs
        + ", schemata="
        + schemata
        + ", excluded="
        + excludedCatalogs
        + ")";
  }
}
