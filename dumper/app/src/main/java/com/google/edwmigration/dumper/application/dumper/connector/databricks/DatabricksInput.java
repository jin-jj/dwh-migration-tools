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
 * Represents a strategy for extracting Databricks metadata.
 *
 * <p>SYSTEM queries global {@code system.information_schema.*} views across all catalogs. CATALOG
 * queries per-catalog {@code <catalog>.information_schema.*} views individually.
 */
@ParametersAreNonnullByDefault
enum DatabricksInput {
  /** Query system.information_schema first, falling back to per-catalog information_schema. */
  SYSTEM_THEN_CATALOG {
    @Override
    @Nonnull
    ImmutableList<Task<?>> tasks(AbstractTask<?> systemTask, AbstractTask<?> catalogTask) {
      return ImmutableList.of(systemTask, catalogTask.onlyIfFailed(systemTask));
    }
  },
  /** Query per-catalog information_schema only. */
  CATALOG_ONLY {
    @Override
    @Nonnull
    ImmutableList<Task<?>> tasks(AbstractTask<?> systemTask, AbstractTask<?> catalogTask) {
      return ImmutableList.of(catalogTask);
    }
  },
  /** Query system.information_schema only. */
  SYSTEM_ONLY {
    @Override
    @Nonnull
    ImmutableList<Task<?>> tasks(AbstractTask<?> systemTask, AbstractTask<?> catalogTask) {
      return ImmutableList.of(systemTask);
    }
  };

  @Nonnull
  abstract ImmutableList<Task<?>> tasks(AbstractTask<?> systemTask, AbstractTask<?> catalogTask);

  @Nonnull
  public static DatabricksInput fromString(@Nonnull String value) {
    switch (value.trim().toLowerCase().replace("_", "-")) {
      case "catalog-only":
      case "catalog":
        return CATALOG_ONLY;
      case "system-only":
      case "system":
        return SYSTEM_ONLY;
      case "system-then-catalog":
      default:
        return SYSTEM_THEN_CATALOG;
    }
  }
}
