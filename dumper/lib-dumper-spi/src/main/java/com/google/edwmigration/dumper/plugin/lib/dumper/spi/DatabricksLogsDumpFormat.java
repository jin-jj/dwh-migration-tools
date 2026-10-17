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

/** Output archive contract of the Databricks logs connector. */
public interface DatabricksLogsDumpFormat {

  String FORMAT_NAME = "databricks.logs.zip";

  String ZIP_ENTRY_PREFIX = "query_history_";

  /**
   * Columns of the query history files, named after {@code system.query.history}.
   *
   * <p>Databricks truncates long SQL statements in both {@code system.query.history} and the query
   * history REST API: {@link #StatementText} is capped at 153,600 characters (150 KiB), or around
   * 64–84 KiB when the compressed statement reaches the storage limit, and ends with {@code "..."}.
   */
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
}
