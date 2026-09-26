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
import com.databricks.sdk.core.DatabricksConfig;
import com.google.common.base.Preconditions;
import com.google.edwmigration.dumper.application.dumper.ConnectorArguments;
import com.google.edwmigration.dumper.application.dumper.MetadataDumperUsageException;
import java.net.URI;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

/** Validates Databricks CLI arguments and constructs {@link WorkspaceClient} instances. */
final class DatabricksClientFactory {

  private static final Pattern WAREHOUSE_PATH_PATTERN =
      Pattern.compile("/(?:warehouses|endpoints)/([A-Za-z0-9_-]+)");

  private DatabricksClientFactory() {}

  static void validateNotAssessment(
      @Nonnull String connectorName, @Nonnull ConnectorArguments arguments) {
    Preconditions.checkNotNull(connectorName, "Connector name was null.");
    Preconditions.checkNotNull(arguments, "ConnectorArguments was null.");
    if (arguments.isAssessment()) {
      throw new MetadataDumperUsageException(
          String.format(
              "The '--assessment' flag is not supported by the '%s' connector. "
                  + "Databricks hardware telemetry and DBU usage assessment are collected via the "
                  + "Databricks Discovery Notebook.",
              connectorName));
    }
  }

  @Nonnull
  static String requireUrl(@Nonnull ConnectorArguments arguments) {
    Preconditions.checkNotNull(arguments, "ConnectorArguments was null.");
    String url = arguments.getUri();
    if (url == null || url.trim().isEmpty()) {
      throw new MetadataDumperUsageException(
          "Required workspace URL was not provided. Please specify '--url <https://workspace-host>'.");
    }
    return normalizeWorkspaceHost(url.trim());
  }

  @Nonnull
  static String normalizeWorkspaceHost(@Nonnull String rawUrl) {
    String cleaned = rawUrl.trim();
    if (cleaned.regionMatches(true, 0, "jdbc:databricks://", 0, "jdbc:databricks://".length())) {
      cleaned = "https://" + cleaned.substring("jdbc:databricks://".length());
    } else if (!cleaned.contains("://")) {
      cleaned = "https://" + cleaned;
    }
    int semicolonIdx = cleaned.indexOf(';');
    if (semicolonIdx > 0) {
      cleaned = cleaned.substring(0, semicolonIdx);
    }
    try {
      URI uri = URI.create(cleaned);
      if (uri.getScheme() != null && uri.getAuthority() != null) {
        return uri.getScheme() + "://" + uri.getAuthority();
      }
    } catch (IllegalArgumentException ignored) {
      // Fall through to raw trimmed string
    }
    return cleaned;
  }

  /**
   * Returns the SQL Warehouse ID from {@code --warehouse}, or from a {@code /warehouses/<id>} or
   * {@code /endpoints/<id>} path in {@code --url} (e.g. a JDBC {@code httpPath}).
   */
  @Nonnull
  static String requireWarehouse(@Nonnull ConnectorArguments arguments) {
    Preconditions.checkNotNull(arguments, "ConnectorArguments was null.");
    String warehouse = arguments.getWarehouse();
    if (warehouse != null && !warehouse.trim().isEmpty()) {
      return warehouse.trim();
    }
    String rawUri = arguments.getUri();
    if (rawUri != null) {
      Matcher matcher = WAREHOUSE_PATH_PATTERN.matcher(rawUri);
      if (matcher.find()) {
        return matcher.group(1);
      }
    }
    throw new MetadataDumperUsageException(
        "Required SQL Warehouse ID was not provided. Please specify '--warehouse <warehouse-id>'.");
  }

  @Nonnull
  static WorkspaceClient createWorkspaceClient(@Nonnull ConnectorArguments arguments) {
    String host = requireUrl(arguments);
    DatabricksConfig config = new DatabricksConfig().setHost(host);
    Optional<String> passwordOpt = arguments.getPasswordIfFlagProvided();
    if (passwordOpt.isPresent() && !passwordOpt.get().trim().isEmpty()) {
      config.setToken(passwordOpt.get().trim());
    }
    return new WorkspaceClient(config);
  }
}
