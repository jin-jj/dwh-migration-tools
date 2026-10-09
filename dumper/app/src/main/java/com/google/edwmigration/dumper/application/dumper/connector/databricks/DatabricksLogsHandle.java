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
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.handle.AbstractHandle;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Thread-safe shared {@code Handle} for the Databricks logs connector.
 *
 * <p>Holds the {@link DatabricksSqlHelper} that reads {@code system.query.history} on the SQL
 * warehouse, which is absent when only the query history API is read, the {@link
 * DatabricksRestHelper} that reads the query history API, the ID of the {@code --url} workspace,
 * and the IDs of the workspaces whose queries are dumped.
 */
@ThreadSafe
final class DatabricksLogsHandle extends AbstractHandle {

  @CheckForNull private final DatabricksSqlHelper sqlHelper;
  private final DatabricksRestHelper restHelper;
  @CheckForNull private final String urlWorkspaceId;
  private final ImmutableList<String> workspaceIds;

  /**
   * @param sqlHelper the SQL helper, or null if no SQL warehouse is used
   * @param urlWorkspaceId the ID of the {@code --url} workspace, or null if it isn't known
   * @param workspaceIds the IDs of the workspaces whose queries are read from {@code
   *     system.query.history}, or an empty list for every workspace in the region
   */
  DatabricksLogsHandle(
      @CheckForNull DatabricksSqlHelper sqlHelper,
      @Nonnull DatabricksRestHelper restHelper,
      @CheckForNull String urlWorkspaceId,
      @Nonnull ImmutableList<String> workspaceIds) {
    this.sqlHelper = sqlHelper;
    this.restHelper = Preconditions.checkNotNull(restHelper, "DatabricksRestHelper was null.");
    this.urlWorkspaceId = urlWorkspaceId;
    this.workspaceIds = Preconditions.checkNotNull(workspaceIds, "Workspace IDs were null.");
  }

  @Nonnull
  DatabricksSqlHelper sqlHelper() {
    Preconditions.checkState(
        sqlHelper != null, "No SQL warehouse is used, so system.query.history can't be read.");
    return sqlHelper;
  }

  @Nonnull
  DatabricksRestHelper restHelper() {
    return restHelper;
  }

  /** Returns the ID of the {@code --url} workspace, or null if it isn't known. */
  @CheckForNull
  String urlWorkspaceId() {
    return urlWorkspaceId;
  }

  /**
   * Returns the IDs of the workspaces whose queries are read from {@code system.query.history}, or
   * an empty list for every workspace in the region.
   */
  @Nonnull
  ImmutableList<String> workspaceIds() {
    return workspaceIds;
  }
}
