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

/**
 * Output archive contract for the Databricks logs connector.
 *
 * <p>Each file has the queries that started in one interval, such as an hour, and ended: the file
 * {@code query_history_2026-10-09T130000Z.csv} has the queries that started from 13:00 UTC,
 * inclusive, to 14:00 UTC, exclusive. The queries come from {@code system.query.history}, or from
 * the query history API when {@code system.query.history} can't be used. The statements of the
 * dumper are left out.
 *
 * <p>Times are ISO 8601 instants, such as {@code 2026-10-09T13:00:01.234Z}. Durations are in
 * milliseconds, and sizes in bytes.
 */
interface DatabricksLogsDumpFormat {

  String FORMAT_NAME = "databricks.logs.zip";

  /** The prefix of the files, which is followed by the start of their interval. */
  String ZIP_ENTRY_PREFIX = "query_history_";

  /**
   * Columns of the query history files, named after the columns of {@code system.query.history}.
   *
   * <ul>
   *   <li>{@code StatementText}: {@code <REDACTED>} in {@code system.query.history} unless the user
   *       is an account admin or a member of the {@code databricks_pii_access} group.
   *   <li>{@code WarehouseId} and {@code ComputeType}: the SQL warehouse, if any, and the type of
   *       compute, such as {@code WAREHOUSE} or {@code SERVERLESS_COMPUTE}.
   *   <li>{@code ProducedRows}: the rows that the statement returned. {@code WrittenRows} and
   *       {@code WrittenBytes}: the data that the statement wrote to cloud storage.
   * </ul>
   *
   * <p>The query history API has no {@code AccountId}, {@code WaitingAtCapacityDurationMs} and
   * {@code WrittenRows}, so they are empty in its files, and {@code WorkspaceId} is the ID of the
   * {@code --url} workspace, which is empty if it isn't known. Its {@code ComputeType} is {@code
   * WAREHOUSE} for the queries of a SQL warehouse and empty for the others.
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
