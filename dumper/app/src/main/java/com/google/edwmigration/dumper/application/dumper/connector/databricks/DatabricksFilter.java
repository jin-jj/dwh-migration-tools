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

/** Case-folding schema filter for Databricks {@code hive_metastore} extraction. */
final class DatabricksFilter {

  static final String HIVE_METASTORE_CATALOG = "hive_metastore";
  private static final String INFORMATION_SCHEMA = "information_schema";

  private final ImmutableSet<String> includedSchemas;

  DatabricksFilter(@Nonnull Collection<String> rawSchemas) {
    Preconditions.checkNotNull(rawSchemas, "Schemas collection was null.");
    this.includedSchemas = normalizeSet(rawSchemas);
  }

  @Nonnull
  static DatabricksFilter fromArguments(@Nonnull ConnectorArguments arguments) {
    Preconditions.checkNotNull(arguments, "ConnectorArguments was null.");
    return new DatabricksFilter(arguments.getSchemata());
  }

  private static ImmutableSet<String> normalizeSet(Collection<String> values) {
    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    for (String value : values) {
      if (value != null) {
        String trimmed = value.trim().toLowerCase(Locale.ROOT);
        if (!trimmed.isEmpty()) {
          builder.add(trimmed);
        }
      }
    }
    return builder.build();
  }

  boolean acceptsSchema(@CheckForNull String schemaName) {
    if (schemaName == null || schemaName.trim().isEmpty()) {
      return false;
    }
    String lower = schemaName.trim().toLowerCase(Locale.ROOT);
    if (INFORMATION_SCHEMA.equals(lower)) {
      return false;
    }
    if (includedSchemas.isEmpty()) {
      return true;
    }
    return includedSchemas.contains(lower);
  }
}
