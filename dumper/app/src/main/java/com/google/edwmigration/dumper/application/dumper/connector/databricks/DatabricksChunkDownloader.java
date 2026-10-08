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

import com.databricks.sdk.core.ApiClient;
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
import java.time.Clock;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Downloads {@code EXTERNAL_LINKS} {@code JSON_ARRAY} result chunks from pre-signed cloud storage
 * URLs using the Databricks SDK {@link HttpClient} connection pool and Jackson {@link
 * ObjectReader}.
 */
final class DatabricksChunkDownloader {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksChunkDownloader.class);

  private static final int DEFAULT_MAX_RETRIES = 5;
  private static final long DEFAULT_BASE_BACKOFF_MS = 200L;
  private static final long DEFAULT_MAX_BACKOFF_MS = 10_000L;
  private static final int DEFAULT_TIMEOUT_SECONDS = 60;

  private static final ObjectReader CHUNK_ROWS_READER =
      new ObjectMapper().readerFor(new TypeReference<List<List<String>>>() {});

  interface Sleeper {
    void sleep(long millis) throws InterruptedException;
  }

  private final HttpClient httpClient;
  private final int maxRetries;
  private final long baseBackoffMs;
  private final long maxBackoffMs;
  private final Clock clock;
  private final Sleeper sleeper;

  DatabricksChunkDownloader() {
    this(createDefaultHttpClient());
  }

  DatabricksChunkDownloader(@Nonnull HttpClient httpClient) {
    this(
        httpClient,
        DEFAULT_MAX_RETRIES,
        DEFAULT_BASE_BACKOFF_MS,
        DEFAULT_MAX_BACKOFF_MS,
        Clock.systemUTC(),
        Thread::sleep);
  }

  @VisibleForTesting
  DatabricksChunkDownloader(
      int maxRetries, long baseBackoffMs, long maxBackoffMs, Clock clock, Sleeper sleeper) {
    this(createDefaultHttpClient(), maxRetries, baseBackoffMs, maxBackoffMs, clock, sleeper);
  }

  @VisibleForTesting
  DatabricksChunkDownloader(
      @Nonnull HttpClient httpClient,
      int maxRetries,
      long baseBackoffMs,
      long maxBackoffMs,
      @Nonnull Clock clock,
      @Nonnull Sleeper sleeper) {
    this.httpClient = Preconditions.checkNotNull(httpClient, "HttpClient was null.");
    this.maxRetries = maxRetries;
    this.baseBackoffMs = baseBackoffMs;
    this.maxBackoffMs = maxBackoffMs;
    this.clock = Preconditions.checkNotNull(clock, "Clock was null.");
    this.sleeper = Preconditions.checkNotNull(sleeper, "Sleeper was null.");
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

    IOException lastException = null;
    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      Request req = new Request(Request.GET, urlString);
      if (headers != null && !headers.isEmpty()) {
        req.withHeaders(headers);
      }
      try {
        Response resp = httpClient.execute(req);
        int status = resp.getStatusCode();
        if (status >= 200 && status < 300) {
          try (InputStream in = resp.getBody()) {
            return parseJsonArrayRows(in);
          }
        }
        closeQuietly(resp.getBody());

        if (isRetryableStatus(status) && attempt < maxRetries) {
          long delayMs = computeRetryDelayMs(attempt, resp);
          logger.warn(
              "Pre-signed chunk download returned HTTP {} on attempt {}/{}. Retrying in {} ms.",
              status,
              attempt + 1,
              maxRetries + 1,
              delayMs);
          sleepQuietly(delayMs);
          continue;
        }

        throw new IOException(
            String.format(
                "Failed to download external result chunk (HTTP %d) from %s.",
                status, sanitizeUrlForLogging(urlString)));
      } catch (IOException e) {
        lastException = e;
        if (attempt < maxRetries && isTransientIoException(e)) {
          long delayMs = computeEqualJitterBackoffMs(attempt);
          logger.warn(
              "Transient I/O error downloading external chunk on attempt {}/{}: {}. Retrying in {} ms.",
              attempt + 1,
              maxRetries + 1,
              e.getMessage(),
              delayMs);
          sleepQuietly(delayMs);
          continue;
        }
        throw e;
      }
    }
    throw lastException != null
        ? lastException
        : new IOException("Exhausted retries downloading external chunk.");
  }

  private static boolean isRetryableStatus(int status) {
    return status == 429 || (status >= 500 && status <= 599);
  }

  private static boolean isTransientIoException(IOException e) {
    String msg = e.getMessage();
    return msg == null || !msg.startsWith("Failed to download external result chunk (HTTP ");
  }

  private long computeRetryDelayMs(int attempt, @Nonnull Response resp) {
    Optional<Long> sdkBackoffMillis = ApiClient.getBackoffFromRetryAfterHeader(resp);
    if (sdkBackoffMillis.isPresent() && sdkBackoffMillis.get() > 0L) {
      return sdkBackoffMillis.get();
    }
    return computeRetryDelayMs(attempt, resp.getFirstHeader("Retry-After"));
  }

  @VisibleForTesting
  long computeRetryDelayMs(int attempt, @CheckForNull String retryAfterHeader) {
    if (retryAfterHeader != null && !retryAfterHeader.trim().isEmpty()) {
      String trimmed = retryAfterHeader.trim();
      try {
        long seconds = Long.parseLong(trimmed);
        return Math.max(0L, seconds * 1000L);
      } catch (NumberFormatException ignored) {
        try {
          ZonedDateTime retryTime =
              ZonedDateTime.parse(trimmed, DateTimeFormatter.RFC_1123_DATE_TIME);
          long millis = Duration.between(clock.instant(), retryTime.toInstant()).toMillis();
          return Math.max(0L, millis);
        } catch (DateTimeParseException ignoredDate) {
          // Fall back to equal-jitter backoff
        }
      }
    }
    return computeEqualJitterBackoffMs(attempt);
  }

  @VisibleForTesting
  long computeEqualJitterBackoffMs(int attempt) {
    long shifted = baseBackoffMs * (1L << Math.min(attempt, 20));
    long cap = Math.min(maxBackoffMs, Math.max(baseBackoffMs, shifted));
    long half = Math.max(1L, cap / 2L);
    return half + ThreadLocalRandom.current().nextLong(half + 1L);
  }

  private void sleepQuietly(long delayMs) throws IOException {
    if (delayMs <= 0L) {
      return;
    }
    try {
      sleeper.sleep(delayMs);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while waiting to retry chunk download.", ie);
    }
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
