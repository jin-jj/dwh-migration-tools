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

import com.databricks.sdk.core.commons.CommonsHttpClient;
import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import com.databricks.sdk.service.sql.ExternalLink;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * Downloads {@code EXTERNAL_LINKS} {@code JSON_ARRAY} result chunks from pre-signed cloud storage
 * URLs using the Databricks SDK {@link HttpClient} connection pool and Jackson {@link
 * ObjectReader}.
 */
final class DatabricksChunkDownloader {

  private static final int DEFAULT_TIMEOUT_SECONDS = 60;

  private static final ObjectReader CHUNK_ROWS_READER =
      new ObjectMapper().readerFor(new TypeReference<List<List<String>>>() {});

  private final HttpClient httpClient;

  DatabricksChunkDownloader() {
    this(createDefaultHttpClient());
  }

  DatabricksChunkDownloader(@Nonnull HttpClient httpClient) {
    this.httpClient = Preconditions.checkNotNull(httpClient, "HttpClient was null.");
  }

  private static HttpClient createDefaultHttpClient() {
    return new CommonsHttpClient.Builder().withTimeoutSeconds(DEFAULT_TIMEOUT_SECONDS).build();
  }

  @Nonnull
  List<List<String>> downloadChunkRows(@Nonnull ExternalLink link) throws IOException {
    Preconditions.checkNotNull(link, "ExternalLink was null.");
    String urlString = link.getExternalLink();
    Preconditions.checkNotNull(urlString, "ExternalLink URL was null.");
    Map<String, String> headers = link.getHttpHeaders();

    Request req = new Request(Request.GET, urlString);
    if (headers != null && !headers.isEmpty()) {
      req.withHeaders(headers);
    }
    Response resp = httpClient.execute(req);
    int status = resp.getStatusCode();
    if (status >= 200 && status < 300) {
      try (InputStream in = resp.getBody()) {
        return parseJsonArrayRows(in);
      }
    }
    closeQuietly(resp.getBody());

    throw new IOException(
        String.format(
            "Failed to download external result chunk (HTTP %d) from %s.",
            status, sanitizeUrlForLogging(urlString)));
  }

  private static void closeQuietly(@CheckForNull InputStream in) {
    if (in != null) {
      try {
        in.close();
      } catch (IOException ignored) {
        // Ignored during cleanup
      }
    }
  }

  @VisibleForTesting
  static List<List<String>> parseJsonArrayRows(@CheckForNull InputStream in) throws IOException {
    if (in == null) {
      return Collections.emptyList();
    }
    try {
      List<List<String>> rows = CHUNK_ROWS_READER.readValue(in);
      return rows != null ? rows : Collections.emptyList();
    } catch (MismatchedInputException e) {
      if (e.getMessage() != null && e.getMessage().contains("No content to map")) {
        return Collections.emptyList();
      }
      throw e;
    }
  }

  private static String sanitizeUrlForLogging(String url) {
    int queryIdx = url.indexOf('?');
    return queryIdx >= 0 ? url.substring(0, queryIdx) + "?<redacted>" : url;
  }
}
