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
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import java.util.Collection;
import java.util.Locale;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * Case-insensitive filter of the catalogs and schemas to dump. The catalogs come from {@code
 * --database} and the schemas from {@code --schema}; an empty list accepts every name. {@code
 * information_schema} is never accepted.
 */
final class DatabricksFilter {

  static final String HIVE_METASTORE_CATALOG = "hive_metastore";
  private static final String INFORMATION_SCHEMA = "information_schema";

  private final ImmutableSet<String> includedCatalogs;
  private final ImmutableSet<String> includedSchemas;

  DatabricksFilter(
      @Nonnull Collection<String> rawCatalogs, @Nonnull Collection<String> rawSchemas) {
    Preconditions.checkNotNull(rawCatalogs, "Catalogs collection was null.");
    Preconditions.checkNotNull(rawSchemas, "Schemas collection was null.");
    this.includedCatalogs = normalizeSet(rawCatalogs);
    this.includedSchemas = normalizeSet(rawSchemas);
  }

  @Nonnull
  static DatabricksFilter fromArguments(@Nonnull ConnectorArguments arguments) {
    Preconditions.checkNotNull(arguments, "ConnectorArguments was null.");
    return new DatabricksFilter(arguments.getDatabases(), arguments.getSchemata());
  }

  private static ImmutableSet<String> normalizeSet(Collection<String> values) {
    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    for (String value : values) {
      String normalized = normalize(value);
      if (!normalized.isEmpty()) {
        builder.add(normalized);
      }
    }
    return builder.build();
  }

  @Nonnull
  private static String normalize(@CheckForNull String name) {
    return name == null ? "" : name.trim().toLowerCase(Locale.ROOT);
  }

  boolean acceptsCatalog(@CheckForNull String catalogName) {
    String normalized = normalize(catalogName);
    if (normalized.isEmpty()) {
      return false;
    }

    return includedCatalogs.isEmpty() || includedCatalogs.contains(normalized);
  }

  boolean acceptsSchema(@CheckForNull String schemaName) {
    String normalized = normalize(schemaName);
    if (normalized.isEmpty() || INFORMATION_SCHEMA.equals(normalized)) {
      return false;
    }

    return includedSchemas.isEmpty() || includedSchemas.contains(normalized);
  }
}
