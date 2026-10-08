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

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.quoteIdentifier;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksError;
import com.databricks.sdk.service.catalog.CatalogInfo;
import com.databricks.sdk.service.catalog.FunctionInfo;
import com.databricks.sdk.service.catalog.GetTableRequest;
import com.databricks.sdk.service.catalog.ListCatalogsRequest;
import com.databricks.sdk.service.catalog.ListCatalogsResponse;
import com.databricks.sdk.service.catalog.ListFunctionsRequest;
import com.databricks.sdk.service.catalog.ListFunctionsResponse;
import com.databricks.sdk.service.catalog.ListSchemasRequest;
import com.databricks.sdk.service.catalog.ListSchemasResponse;
import com.databricks.sdk.service.catalog.ListTablesRequest;
import com.databricks.sdk.service.catalog.ListTablesResponse;
import com.databricks.sdk.service.catalog.SchemaInfo;
import com.databricks.sdk.service.catalog.TableInfo;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksChunkDownloader.Sleeper;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads Unity Catalog metadata with the Databricks REST API.
 *
 * <p>Databricks limits the rate of requests to a workspace, so each request waits for a permit of a
 * rate limiter that the tasks share. The SDK retries a throttled request a few times; if it still
 * fails, the request is retried here, after a backoff with jitter so that the requests that were
 * throttled together don't retry together.
 *
 * <p>A list request asks for the page size that the server configures, as the API reference
 * recommends, and the page tokens are followed to the last page.
 */
@ThreadSafe
final class DatabricksRestHelper {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksRestHelper.class);

  /**
   * The rate of requests. Databricks doesn't publish the rate limits of the Unity Catalog
   * endpoints, so the rate is conservative.
   */
  private static final double DEFAULT_REQUESTS_PER_SECOND = 20.0;

  /** The {@code max_results} that makes the server choose the page size. */
  private static final long SERVER_PAGE_SIZE = 0L;

  /** The attempts of a request that keeps failing with a retryable status. */
  static final int MAX_ATTEMPTS = 5;

  static final long INITIAL_BACKOFF_MILLIS = 1_000L;

  static final long MAX_BACKOFF_MILLIS = 60_000L;

  /** The HTTP statuses of a request that may succeed later: too many requests and unavailable. */
  private static final ImmutableSet<Integer> RETRYABLE_STATUS_CODES = ImmutableSet.of(429, 503);

  /** Sends one request, and returns its response, which is null if the response body is empty. */
  private interface Call<T> {

    @CheckForNull
    T call();
  }

  /**
   * Sends the request for one page, given the token of the page, or null for the first page, and
   * returns the page, which is null if the response body is empty.
   */
  private interface PageFetcher<T> {

    @CheckForNull
    Page<T> fetch(@CheckForNull String pageToken);
  }

  /** The items of a page and the token of the next page. */
  private static final class Page<T> {

    final ImmutableList<T> items;
    @CheckForNull final String nextPageToken;

    Page(@CheckForNull Collection<T> items, @CheckForNull String nextPageToken) {
      this.items = items == null ? ImmutableList.of() : ImmutableList.copyOf(items);
      this.nextPageToken = nextPageToken;
    }
  }

  private final WorkspaceClient client;
  private final RateLimiter rateLimiter;
  private final Sleeper sleeper;

  DatabricksRestHelper(@Nonnull WorkspaceClient client) {
    this(client, DEFAULT_REQUESTS_PER_SECOND, Thread::sleep);
  }

  @VisibleForTesting
  DatabricksRestHelper(
      @Nonnull WorkspaceClient client, double requestsPerSecond, @Nonnull Sleeper sleeper) {
    this.client = Preconditions.checkNotNull(client, "WorkspaceClient was null.");
    this.rateLimiter = RateLimiter.create(requestsPerSecond);
    this.sleeper = Preconditions.checkNotNull(sleeper, "Sleeper was null.");
  }

  /** Lists the catalogs, including those that are visible only with the BROWSE privilege. */
  @Nonnull
  ImmutableList<CatalogInfo> listCatalogs() throws IOException {
    return listAllPages(
        "Listing catalogs",
        pageToken -> {
          ListCatalogsResponse response =
              client
                  .catalogs()
                  .impl()
                  .list(
                      new ListCatalogsRequest()
                          .setIncludeBrowse(true)
                          .setMaxResults(SERVER_PAGE_SIZE)
                          .setPageToken(pageToken));
          return response == null
              ? null
              : new Page<>(response.getCatalogs(), response.getNextPageToken());
        });
  }

  @Nonnull
  ImmutableList<SchemaInfo> listSchemas(@Nonnull String catalogName) throws IOException {
    return listAllPages(
        "Listing the schemas of catalog " + quoteIdentifier(catalogName),
        pageToken -> {
          ListSchemasResponse response =
              client
                  .schemas()
                  .impl()
                  .list(
                      new ListSchemasRequest()
                          .setCatalogName(catalogName)
                          .setIncludeBrowse(true)
                          .setMaxResults(SERVER_PAGE_SIZE)
                          .setPageToken(pageToken));
          return response == null
              ? null
              : new Page<>(response.getSchemas(), response.getNextPageToken());
        });
  }

  /**
   * Lists the tables and views of a schema with their columns and properties. The securable kind
   * manifest of a table, which has its securable type and kind, is included only on request. {@code
   * table_constraints} is never included, so it needs {@link #getTable}.
   */
  @Nonnull
  ImmutableList<TableInfo> listTables(@Nonnull String catalogName, @Nonnull String schemaName)
      throws IOException {
    return listAllPages(
        "Listing the tables of schema " + quoteSchemaName(catalogName, schemaName),
        pageToken -> {
          ListTablesResponse response =
              client
                  .tables()
                  .impl()
                  .list(
                      new ListTablesRequest()
                          .setCatalogName(catalogName)
                          .setSchemaName(schemaName)
                          .setIncludeBrowse(true)
                          .setIncludeManifestCapabilities(true)
                          .setMaxResults(SERVER_PAGE_SIZE)
                          .setPageToken(pageToken));
          return response == null
              ? null
              : new Page<>(response.getTables(), response.getNextPageToken());
        });
  }

  @Nonnull
  ImmutableList<FunctionInfo> listFunctions(@Nonnull String catalogName, @Nonnull String schemaName)
      throws IOException {
    return listAllPages(
        "Listing the functions of schema " + quoteSchemaName(catalogName, schemaName),
        pageToken -> {
          ListFunctionsResponse response =
              client
                  .functions()
                  .impl()
                  .list(
                      new ListFunctionsRequest()
                          .setCatalogName(catalogName)
                          .setSchemaName(schemaName)
                          .setIncludeBrowse(true)
                          .setMaxResults(SERVER_PAGE_SIZE)
                          .setPageToken(pageToken));
          return response == null
              ? null
              : new Page<>(response.getFunctions(), response.getNextPageToken());
        });
  }

  /** Gets a table with its primary and foreign keys. */
  @Nonnull
  TableInfo getTable(
      @Nonnull String catalogName, @Nonnull String schemaName, @Nonnull String tableName)
      throws IOException {
    String fullName = catalogName + "." + schemaName + "." + tableName;
    return callWithRetry(
        "Getting table "
            + quoteSchemaName(catalogName, schemaName)
            + "."
            + quoteIdentifier(tableName),
        () ->
            client
                .tables()
                .get(new GetTableRequest().setFullName(fullName).setIncludeBrowse(true)));
  }

  @Nonnull
  private static String quoteSchemaName(@Nonnull String catalogName, @Nonnull String schemaName) {
    return quoteIdentifier(catalogName) + "." + quoteIdentifier(schemaName);
  }

  @Nonnull
  private <T> ImmutableList<T> listAllPages(
      @Nonnull String description, @Nonnull PageFetcher<T> fetcher) throws IOException {
    ImmutableList.Builder<T> items = ImmutableList.builder();
    Set<String> seenPageTokens = new HashSet<>();
    String pageToken = null;
    while (true) {
      String requestedPageToken = pageToken;
      Page<T> page = callWithRetry(description, () -> fetcher.fetch(requestedPageToken));
      items.addAll(page.items);
      pageToken = page.nextPageToken;
      if (Strings.isNullOrEmpty(pageToken)) {
        return items.build();
      }

      if (!seenPageTokens.add(pageToken)) {
        throw new IOException(
            String.format(
                "%s failed: Databricks returned the page token '%s' twice.",
                description, pageToken));
      }
    }
  }

  @Nonnull
  private <T> T callWithRetry(@Nonnull String description, @Nonnull Call<T> call)
      throws IOException {
    for (int attempt = 1; ; attempt++) {
      rateLimiter.acquire();
      T response;
      try {
        response = call.call();
      } catch (RuntimeException e) {
        if (attempt >= MAX_ATTEMPTS || !isRetryable(e)) {
          throw new IOException(description + " failed: " + e.getMessage(), e);
        }

        long backoffMillis = backoffMillis(attempt);
        logger.warn(
            "{} failed on attempt {} of {}, retrying in {} ms: {}",
            description,
            attempt,
            MAX_ATTEMPTS,
            backoffMillis,
            e.getMessage());
        sleep(description, backoffMillis);
        continue;
      }

      if (response == null) {
        throw new IOException(description + " failed: Databricks returned an empty response.");
      }
      return response;
    }
  }

  /** Returns whether the failure, or one of its causes, has a retryable HTTP status. */
  static boolean isRetryable(@Nonnull Throwable failure) {
    for (Throwable cause : Throwables.getCausalChain(failure)) {
      if (cause instanceof DatabricksError
          && RETRYABLE_STATUS_CODES.contains(((DatabricksError) cause).getStatusCode())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns the backoff before the retry after a 1-based attempt: a random time between half of and
   * the whole of a ceiling that starts at {@link #INITIAL_BACKOFF_MILLIS} and doubles with each
   * attempt, up to {@link #MAX_BACKOFF_MILLIS}.
   */
  static long backoffMillis(int attempt) {
    Preconditions.checkArgument(attempt >= 1, "Attempt must be at least 1, but was %s.", attempt);
    long ceiling = INITIAL_BACKOFF_MILLIS;
    for (int doubling = 1; doubling < attempt && ceiling < MAX_BACKOFF_MILLIS; doubling++) {
      ceiling *= 2;
    }

    long half = Math.min(ceiling, MAX_BACKOFF_MILLIS) / 2;
    return half + ThreadLocalRandom.current().nextLong(half + 1);
  }

  private void sleep(@Nonnull String description, long millis) throws InterruptedIOException {
    try {
      sleeper.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      InterruptedIOException interrupted =
          new InterruptedIOException(description + " was interrupted while waiting to retry.");
      interrupted.initCause(e);
      throw interrupted;
    }
  }
}
