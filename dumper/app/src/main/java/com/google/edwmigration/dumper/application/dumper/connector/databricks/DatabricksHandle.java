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
import com.google.edwmigration.dumper.application.dumper.handle.AbstractHandle;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Thread-safe shared {@code Handle} for the Databricks {@code hive_metastore} connector.
 *
 * <p>Holds the {@link DatabricksSqlHelper} bound to the configured SQL Warehouse and the schema
 * {@link DatabricksFilter}.
 */
@ThreadSafe
final class DatabricksHandle extends AbstractHandle {

  private final DatabricksSqlHelper sqlHelper;
  private final DatabricksFilter filter;

  DatabricksHandle(@Nonnull DatabricksSqlHelper sqlHelper, @Nonnull DatabricksFilter filter) {
    this.sqlHelper = Preconditions.checkNotNull(sqlHelper, "DatabricksSqlHelper was null.");
    this.filter = Preconditions.checkNotNull(filter, "DatabricksFilter was null.");
  }

  @Nonnull
  DatabricksSqlHelper sqlHelper() {
    return sqlHelper;
  }

  @Nonnull
  DatabricksFilter filter() {
    return filter;
  }
}
