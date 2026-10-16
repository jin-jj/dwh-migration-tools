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
package com.google.edwmigration.dumper.application.dumper.connector.databricks.client;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import java.util.Optional;

/** Validates CLI arguments and creates {@link WorkspaceClient} instances. */
public final class DatabricksClients {

  private DatabricksClients() {}

  public static void validateNotAssessment(String connectorName, ConnectorArguments arguments) {
    if (arguments.isAssessment()) {
      throw new MetadataDumperUsageException(
          String.format(
              "The '--assessment' flag is not supported by the '%s' connector. "
                  + "Databricks hardware telemetry and DBU usage assessment are collected via the "
                  + "Databricks Discovery Notebook.",
              connectorName));
    }
  }

  public static String requireUrl(ConnectorArguments arguments) {
    String url = arguments.getUri();
    if (url == null || url.trim().isEmpty()) {
      throw new MetadataDumperUsageException(
          "Required workspace URL was not provided. Please specify '--url <https://workspace-host>'.");
    }
    return url.trim();
  }

  /** Returns the ID of the SQL warehouse of --warehouse, or null if none. */
  public static String findWarehouse(ConnectorArguments arguments) {
    String warehouse = arguments.getWarehouse();
    return warehouse == null || warehouse.trim().isEmpty() ? null : warehouse.trim();
  }

  public static WorkspaceClient createWorkspaceClient(ConnectorArguments arguments) {
    DatabricksConfig config = new DatabricksConfig().setHost(requireUrl(arguments));
    Optional<String> password = arguments.getPasswordIfFlagProvided();
    if (password.isPresent() && !password.get().trim().isEmpty()) {
      config.setToken(password.get().trim());
    }
    return new WorkspaceClient(config);
  }
}
