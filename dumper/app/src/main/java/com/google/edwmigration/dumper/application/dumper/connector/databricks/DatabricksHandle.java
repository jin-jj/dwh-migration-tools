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

import com.databricks.sdk.WorkspaceClient;
import com.google.common.base.Preconditions;
import com.google.edwmigration.dumper.application.dumper.handle.AbstractHandle;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/** Handle holding the Databricks WorkspaceClient and optional SQL warehouse identifier. */
public class DatabricksHandle extends AbstractHandle {

  private final WorkspaceClient client;
  @Nullable private final String warehouseId;

  public DatabricksHandle(@Nonnull WorkspaceClient client, @Nullable String warehouseId) {
    this.client = Preconditions.checkNotNull(client, "WorkspaceClient was null.");
    this.warehouseId = warehouseId;
  }

  @Nonnull
  public WorkspaceClient getClient() {
    return client;
  }

  public boolean hasWarehouseId() {
    return !StringUtils.isBlank(warehouseId);
  }

  @CheckForNull
  public String getWarehouseId() {
    return warehouseId;
  }
}
