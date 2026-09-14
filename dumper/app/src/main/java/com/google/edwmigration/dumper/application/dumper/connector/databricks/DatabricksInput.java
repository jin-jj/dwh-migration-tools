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

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Chooses which of the three extraction tiers run, and in what order they fall back.
 *
 * <p>The tiers, from cheapest and most complete to most expensive and least complete:
 *
 * <ol>
 *   <li><b>system</b> — {@code system.information_schema.*}. One query covers every catalog in the
 *       metastore. Requires the system schema to be enabled and readable.
 *   <li><b>catalog</b> — {@code <catalog>.information_schema.*}, queried one catalog at a time.
 *       Works when the system schema is unavailable, but costs one query per catalog and silently
 *       omits catalogs the caller cannot read.
 *   <li><b>rest</b> — the Unity Catalog REST API. Needs no SQL warehouse at all, which is the whole
 *       point of having it, but it is request-per-schema (and request-per-table for constraints)
 *       and cannot see {@code hive_metastore}.
 * </ol>
 *
 * <p>Each tier writes the same output file, and a tier runs only when every tier before it failed,
 * so at most one of them contributes to the dump.
 */
@ParametersAreNonnullByDefault
enum DatabricksInput {
  /** Try the system schema, then per-catalog schemas, then the REST API. The default. */
  SYSTEM_THEN_CATALOG_THEN_REST {
    @Override
    @Nonnull
    ImmutableList<Task<?>> tasks(
        AbstractTask<?> systemTask, AbstractTask<?> catalogTask, AbstractTask<?> restTask) {
      return ImmutableList.of(
          systemTask,
          catalogTask.onlyIfFailed(systemTask),
          restTask.onlyIfAllFailed(systemTask, catalogTask));
    }
  },
  /** Try the system schema, then per-catalog schemas. Never touch the REST API. */
  SYSTEM_THEN_CATALOG {
    @Override
    @Nonnull
    ImmutableList<Task<?>> tasks(
        AbstractTask<?> systemTask, AbstractTask<?> catalogTask, AbstractTask<?> restTask) {
      return ImmutableList.of(systemTask, catalogTask.onlyIfFailed(systemTask));
    }
  },
  /** Query per-catalog information schemas only. */
  CATALOG_ONLY {
    @Override
    @Nonnull
    ImmutableList<Task<?>> tasks(
        AbstractTask<?> systemTask, AbstractTask<?> catalogTask, AbstractTask<?> restTask) {
      return ImmutableList.of(catalogTask);
    }
  },
  /** Query the system information schema only. */
  SYSTEM_ONLY {
    @Override
    @Nonnull
    ImmutableList<Task<?>> tasks(
        AbstractTask<?> systemTask, AbstractTask<?> catalogTask, AbstractTask<?> restTask) {
      return ImmutableList.of(systemTask);
    }
  },
  /** Use the Unity Catalog REST API only. Does not need a SQL warehouse. */
  REST_ONLY {
    @Override
    @Nonnull
    ImmutableList<Task<?>> tasks(
        AbstractTask<?> systemTask, AbstractTask<?> catalogTask, AbstractTask<?> restTask) {
      return ImmutableList.of(restTask);
    }
  };

  /** Returns whether this strategy runs any tier that needs a SQL warehouse. */
  boolean requiresWarehouse() {
    return this != REST_ONLY;
  }

  @Nonnull
  abstract ImmutableList<Task<?>> tasks(
      AbstractTask<?> systemTask, AbstractTask<?> catalogTask, AbstractTask<?> restTask);

  @Nonnull
  public static DatabricksInput fromString(@Nonnull String value) {
    switch (value.trim().toLowerCase().replace("_", "-")) {
      case "catalog-only":
      case "catalog":
        return CATALOG_ONLY;
      case "system-only":
      case "system":
        return SYSTEM_ONLY;
      case "rest-only":
      case "rest":
        return REST_ONLY;
      case "system-then-catalog":
        return SYSTEM_THEN_CATALOG;
      case "system-then-catalog-then-rest":
      default:
        return SYSTEM_THEN_CATALOG_THEN_REST;
    }
  }
}
