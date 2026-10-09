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

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Arrays.stream;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.connector.ConnectorProperty;
import javax.annotation.Nonnull;

public enum DatabricksLogsConnectorProperty implements ConnectorProperty {
  STRATEGY(
      "databricks.logs.strategy",
      "Extraction strategy for query history: system-then-rest (default), system-only, or"
          + " rest-only. rest-only uses the Workspace Query History REST API"
          + " (/api/2.0/sql/history/queries) and does not require a SQL warehouse or system"
          + " catalog access."),
  WORKSPACE_ID(
      "databricks.logs.workspace-id",
      "Databricks workspace ID(s) to filter system.query.history (comma-separated if multiple)."),
  OVERRIDE_QUERY("databricks.logs.query", "Custom query for Databricks log dump."),
  OVERRIDE_WHERE(
      "databricks.logs.where",
      "Custom WHERE condition to append to query for Databricks log dump.");

  private final String name;
  private final String description;

  DatabricksLogsConnectorProperty(String name, String description) {
    this.name = name;
    this.description = description;
  }

  public static ImmutableList<ConnectorProperty> getConstants() {
    return stream(values()).collect(toImmutableList());
  }

  @Nonnull
  @Override
  public String getName() {
    return name;
  }

  @Nonnull
  @Override
  public String getDescription() {
    return description;
  }
}
