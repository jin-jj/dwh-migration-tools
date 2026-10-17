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

import static com.google.common.net.UrlEscapers.urlPathSegmentEscaper;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.service.catalog.CatalogInfo;
import com.databricks.sdk.service.catalog.FunctionInfo;
import com.databricks.sdk.service.catalog.GetFunctionRequest;
import com.databricks.sdk.service.catalog.ListCatalogsRequest;
import com.databricks.sdk.service.catalog.ListFunctionsRequest;
import com.databricks.sdk.service.catalog.ListSchemasRequest;
import com.databricks.sdk.service.catalog.ListTablesRequest;
import com.databricks.sdk.service.catalog.SchemaInfo;
import com.databricks.sdk.service.catalog.TableInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Reads Unity Catalog metadata with the Databricks REST API. Every call sets {@code
 * include_browse=true}, so objects with only the BROWSE privilege are returned too. The calls throw
 * the {@code DatabricksException} of the SDK when the API returns an error.
 */
@ThreadSafe
public final class DatabricksRestHelper {

  private static final long SERVER_PAGE_SIZE = 0L;
  private static final String TABLES_PATH = "/api/2.1/unity-catalog/tables/";
  private static final ObjectReader JSON_READER = new ObjectMapper().readerFor(JsonNode.class);

  private final WorkspaceClient client;

  public DatabricksRestHelper(WorkspaceClient client) {
    this.client = Preconditions.checkNotNull(client, "WorkspaceClient was null.");
  }

  public ImmutableList<CatalogInfo> listCatalogs() {
    return ImmutableList.copyOf(
        client
            .catalogs()
            .list(
                new ListCatalogsRequest().setIncludeBrowse(true).setMaxResults(SERVER_PAGE_SIZE)));
  }

  public ImmutableList<SchemaInfo> listSchemas(String catalogName) {
    return ImmutableList.copyOf(
        client
            .schemas()
            .list(
                new ListSchemasRequest()
                    .setCatalogName(catalogName)
                    .setIncludeBrowse(true)
                    .setMaxResults(SERVER_PAGE_SIZE)));
  }

  public ImmutableList<TableInfo> listTables(String catalogName, String schemaName) {
    return ImmutableList.copyOf(
        client
            .tables()
            .list(
                new ListTablesRequest()
                    .setCatalogName(catalogName)
                    .setSchemaName(schemaName)
                    .setIncludeBrowse(true)
                    .setMaxResults(SERVER_PAGE_SIZE)));
  }

  public ImmutableList<FunctionInfo> listFunctions(String catalogName, String schemaName) {
    return ImmutableList.copyOf(
        client
            .functions()
            .list(
                new ListFunctionsRequest()
                    .setCatalogName(catalogName)
                    .setSchemaName(schemaName)
                    .setIncludeBrowse(true)
                    .setMaxResults(SERVER_PAGE_SIZE)));
  }

  /**
   * Returns the {@code functions.get} response of a function, which has the {@code input_params}
   * that {@code functions.list} doesn't return. The SDK throws an {@link IllegalArgumentException}
   * for a full name that isn't valid in a URL path, such as a name with a space.
   */
  public FunctionInfo getFunction(String catalogName, String schemaName, String functionName) {
    String fullName = catalogName + "." + schemaName + "." + functionName;
    return client
        .functions()
        .get(new GetFunctionRequest().setName(fullName).setIncludeBrowse(true));
  }

  /**
   * Returns the JSON response of the {@code tables.get} call of a table. The JSON isn't read into
   * the SDK {@link TableInfo}, because its {@code TableConstraint} has no field for UNIQUE
   * constraints and the SDK drops unknown fields. The request is the one of {@code TablesAPI.get},
   * except that the full name is escaped as a path segment, so that names with characters such as
   * {@code #} or {@code ?} are read too.
   */
  public JsonNode getTableJson(String catalogName, String schemaName, String tableName)
      throws IOException {
    String fullName = catalogName + "." + schemaName + "." + tableName;
    ApiClient apiClient = client.apiClient();
    // The query is in the URL because Request.getUri() decodes the path to add query parameters.
    Request request =
        new Request(
                Request.GET,
                TABLES_PATH + urlPathSegmentEscaper().escape(fullName) + "?include_browse=true")
            .withHeader("Accept", "application/json");
    if (apiClient.workspaceId() != null) {
      request.withHeader("X-Databricks-Workspace-Id", apiClient.workspaceId());
    }
    try (InputStream body = apiClient.execute(request, InputStream.class)) {
      return JSON_READER.readTree(body);
    }
  }
}
