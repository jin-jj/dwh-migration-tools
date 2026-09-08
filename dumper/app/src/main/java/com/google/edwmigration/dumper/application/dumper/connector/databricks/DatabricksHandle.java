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
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import java.io.IOException;
import javax.annotation.Nonnull;

/** Handle for Databricks SQL Warehouse metadata dumper. */
public class DatabricksHandle implements Handle {

  private final WorkspaceClient client;
  private final String warehouseId;

  public DatabricksHandle(@Nonnull WorkspaceClient client, @Nonnull String warehouseId) {
    this.client = Preconditions.checkNotNull(client, "WorkspaceClient cannot be null.");
    this.warehouseId = Preconditions.checkNotNull(warehouseId, "warehouseId cannot be null.");
  }

  @Nonnull
  public WorkspaceClient getClient() {
    return client;
  }

  @Nonnull
  public String getWarehouseId() {
    return warehouseId;
  }

  public boolean hasWarehouseId() {
    return warehouseId != null && !warehouseId.isEmpty();
  }

  @Override
  public void close() throws IOException {}
}
