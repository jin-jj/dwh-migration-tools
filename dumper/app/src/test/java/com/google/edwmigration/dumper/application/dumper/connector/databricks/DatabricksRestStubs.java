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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.utils.SerDeUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.google.common.collect.ImmutableMap;
import java.io.UncheckedIOException;

/**
 * Responses of the Unity Catalog REST API for a WireMock server, and a client that sends requests
 * to the server.
 */
final class DatabricksRestStubs {

  static final String CATALOGS_PATH = "/api/2.1/unity-catalog/catalogs";
  static final String SCHEMAS_PATH = "/api/2.1/unity-catalog/schemas";
  static final String TABLES_PATH = "/api/2.1/unity-catalog/tables";
  static final String FUNCTIONS_PATH = "/api/2.1/unity-catalog/functions";
  static final String QUERY_HISTORY_PATH = "/api/2.0/sql/history/queries";

  /** A request rate that doesn't slow the tests down. */
  static final double UNTHROTTLED_REQUESTS_PER_SECOND = 1_000_000.0;

  private static final int TOO_MANY_REQUESTS_STATUS = 429;

  /** Writes the SDK models as the SDK reads them. */
  private static final ObjectWriter WRITER = SerDeUtils.createMapper().writer();

  private DatabricksRestStubs() {}

  /** Returns a client that sends requests to {@code baseUrl} with a personal access token. */
  static WorkspaceClient newWorkspaceClient(String baseUrl) {
    return new WorkspaceClient(
        new DatabricksConfig().setHost(baseUrl).setToken("test-token").setAuthType("pat"));
  }

  /** Returns a successful response with an SDK model, such as a list response, as its body. */
  static ResponseDefinitionBuilder okModel(Object model) {
    return okJson(toJson(model));
  }

  /** Returns a throttled response, which the SDK retries a few times without waiting. */
  static ResponseDefinitionBuilder throttled() {
    return error(TOO_MANY_REQUESTS_STATUS, "REQUEST_LIMIT_EXCEEDED", "Too many requests.")
        .withHeader("Retry-After", "0");
  }

  /** Returns an error response with the body that Databricks sends, such as for a 403. */
  static ResponseDefinitionBuilder error(int status, String errorCode, String message) {
    return aResponse()
        .withStatus(status)
        .withHeader("Content-Type", "application/json")
        .withBody(toJson(ImmutableMap.of("error_code", errorCode, "message", message)));
  }

  private static String toJson(Object value) {
    try {
      return WRITER.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException("Failed to write a test response.", e);
    }
  }
}
