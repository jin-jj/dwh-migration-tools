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
package com.google.edwmigration.dumper.plugin.lib.dumper.spi;

/** Format specification for Databricks query logs dumps. */
public interface DatabricksLogsDumpFormat {

  String FORMAT_NAME = "databricks.logs.zip";

  String ZIP_ENTRY_PREFIX = "query_history_";

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
    WrittenBytes,
    WrittenRows,
    SpilledLocalBytes,
    ClientApplication
  }
}
