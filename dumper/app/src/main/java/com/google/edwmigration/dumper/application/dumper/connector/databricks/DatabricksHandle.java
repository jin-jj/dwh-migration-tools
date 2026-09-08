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
import com.google.common.util.concurrent.RateLimiter;
import com.google.edwmigration.dumper.application.dumper.handle.AbstractHandle;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/** Handle holding the Databricks WorkspaceClient and optional SQL warehouse identifier. */
public class DatabricksHandle extends AbstractHandle {

  private static final double DEFAULT_REQUESTS_PER_SECOND = 20.0;

  private final WorkspaceClient client;
  @Nullable private final String warehouseId;
  private final RateLimiter rateLimiter;

  public DatabricksHandle(@Nonnull WorkspaceClient client, @Nullable String warehouseId) {
    this(client, warehouseId, RateLimiter.create(resolveRequestsPerSecond()));
  }

  public DatabricksHandle(
      @Nonnull WorkspaceClient client,
      @Nullable String warehouseId,
      @Nonnull RateLimiter rateLimiter) {
    this.client = Preconditions.checkNotNull(client, "WorkspaceClient was null.");
    this.warehouseId = warehouseId;
    this.rateLimiter = Preconditions.checkNotNull(rateLimiter, "RateLimiter was null.");
  }

  private static double resolveRequestsPerSecond() {
    String envRateLimit = System.getenv("DATABRICKS_RATE_LIMIT");
    if (envRateLimit != null) {
      try {
        double parsed = Double.parseDouble(envRateLimit.trim());
        if (parsed > 0) {
          return parsed;
        }
      } catch (NumberFormatException ignored) {
        // Fall back to default
      }
    }
    return DEFAULT_REQUESTS_PER_SECOND;
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

  public void acquirePermit() {
    rateLimiter.acquire();
  }
}
