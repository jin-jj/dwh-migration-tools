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
package com.google.edwmigration.dumper.application.dumper.connector.databricks.output;

import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;

/** Output archive contract and properties for the Databricks logs connector. */
public interface DatabricksLogsDumpFormat {

  String FORMAT_NAME = "databricks.logs.zip";

  String ZIP_ENTRY_PREFIX = "query_history_";

  /** Columns of the query history files, named after {@code system.query.history}. */
  enum Header {
    AccountId,
    WorkspaceId,
    StatementId,
    SessionId,
    ExecutedBy,
    StatementText,
    StatementType,
    ExecutionStatus,
    ErrorMessage,
    WarehouseId,
    ComputeType,
    StartTime,
    EndTime,
    TotalDurationMs,
    CompilationDurationMs,
    ExecutionDurationMs,
    WaitingAtCapacityDurationMs,
    ReadBytes,
    ReadRows,
    ProducedRows,
    WrittenBytes,
    WrittenRows,
    SpilledLocalBytes,
    ClientApplication
  }

  /** Properties of the Databricks logs connector, passed as {@code -D<name>=<value>}. */
  enum DatabricksLogsProperty implements ConnectorProperty {
    WORKSPACE_ID(
        "workspace-id",
        "The IDs of the workspaces whose queries are read from system.query.history, separated by"
            + " commas, or 'all' for every workspace in the region. Needs --warehouse. When it is"
            + " set, the query history API is never read instead, because it has only the queries"
            + " of the --url workspace. Default: the --url workspace.");

    /** The value of {@link #WORKSPACE_ID} for every workspace in the region. */
    public static final String ALL_WORKSPACES = "all";

    private final String name;
    private final String description;

    DatabricksLogsProperty(String name, String description) {
      this.name = "databricks.logs." + name;
      this.description = description;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public String getDescription() {
      return description;
    }
  }
}
