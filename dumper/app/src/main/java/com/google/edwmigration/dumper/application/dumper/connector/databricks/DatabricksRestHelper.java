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

import com.databricks.sdk.core.DatabricksError;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared plumbing for the Unity Catalog REST fallback tier: throttling, retry on throttling, and
 * page-at-a-time traversal of the SDK list endpoints.
 *
 * <p>The SDK's {@code list(...)} convenience methods return a lazily paginating {@code Iterable},
 * which hides page boundaries. This class drives {@code impl().list(...)} directly instead, so that
 * one rate limit permit is taken per HTTP request rather than per result row, and so that a page
 * that fails can be retried without re-emitting the rows already written.
 */
final class DatabricksRestHelper {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksRestHelper.class);

  private static final int MAX_ATTEMPTS = 5;
  private static final long INITIAL_BACKOFF_MS = 2_000L;

  /**
   * Requested page size. Leaving {@code max_results} unset makes Databricks return the whole result
   * in one unpaginated response, which the API reference explicitly discourages.
   */
  static final long PAGE_SIZE = 1_000L;

  /** Guards against a server that keeps handing back a page token that never terminates. */
  private static final int MAX_PAGES = 100_000;

  private DatabricksRestHelper() {}

  /** One page of a Unity Catalog list response. */
  static final class Page<T> {

    private final Collection<T> items;
    @Nullable private final String nextPageToken;

    Page(@Nullable Collection<T> items, @Nullable String nextPageToken) {
      this.items = items == null ? Collections.<T>emptyList() : items;
      this.nextPageToken = nextPageToken;
    }

    @Nonnull
    Collection<T> items() {
      return items;
    }

    @CheckForNull
    String nextPageToken() {
      return nextPageToken;
    }
  }

  /** Fetches a single page, given the token returned by the previous page ({@code null} first). */
  interface PageFetcher<T> {
    @Nonnull
    Page<T> fetch(@Nullable String pageToken);
  }

  /** Receives one item at a time. Unlike {@link java.util.function.Consumer} it may write I/O. */
  interface ItemConsumer<T> {
    void accept(@Nonnull T item) throws IOException;
  }

  /** Produces a single non-paginated response, e.g. a {@code get} call. */
  interface Call<T> {
    @CheckForNull
    T call();
  }

  /**
   * Walks every page of a list endpoint, passing each item to {@code consumer} as it arrives.
   *
   * <p>Items are handed over page by page, so the caller can stream them straight to disk. A page
   * is only handed over once it has been fetched successfully, so a retried page never produces
   * duplicate output.
   *
   * @throws IOException if a page still fails after exhausting the retry budget.
   */
  static <T> void forEachItem(
      @Nonnull DatabricksHandle handle,
      @Nonnull String description,
      @Nonnull PageFetcher<T> fetcher,
      @Nonnull ItemConsumer<T> consumer)
      throws IOException {
    Preconditions.checkNotNull(handle, "Handle was null.");
    Preconditions.checkNotNull(description, "Description was null.");
    Preconditions.checkNotNull(fetcher, "Page fetcher was null.");
    Preconditions.checkNotNull(consumer, "Item consumer was null.");

    Set<String> seenTokens = new HashSet<>();
    String pageToken = null;
    for (int pageNumber = 1; ; pageNumber++) {
      String currentToken = pageToken;
      Page<T> page = callWithRetry(handle, description, () -> fetcher.fetch(currentToken));
      for (T item : page.items()) {
        consumer.accept(item);
      }
      String nextPageToken = page.nextPageToken();
      // An empty page may still carry a token, so termination is decided by the token alone.
      if (nextPageToken == null || nextPageToken.isEmpty()) {
        return;
      }
      if (!seenTokens.add(nextPageToken)) {
        throw new IOException(
            "Databricks returned a repeated page token while " + description + ". Aborting.");
      }
      if (pageNumber >= MAX_PAGES) {
        throw new IOException(
            "Databricks returned more than "
                + MAX_PAGES
                + " pages while "
                + description
                + ". Aborting.");
      }
      pageToken = nextPageToken;
    }
  }

  /**
   * Invokes {@code call} once a rate limit permit is available, retrying while Databricks reports
   * throttling.
   *
   * @throws IOException if the call still fails after exhausting the retry budget.
   */
  @CheckForNull
  static <T> T callWithRetry(
      @Nonnull DatabricksHandle handle, @Nonnull String description, @Nonnull Call<T> call)
      throws IOException {
    Preconditions.checkNotNull(handle, "Handle was null.");
    Preconditions.checkNotNull(description, "Description was null.");
    Preconditions.checkNotNull(call, "Call was null.");

    long backoffMs = INITIAL_BACKOFF_MS;
    for (int attempt = 1; ; attempt++) {
      handle.acquireRestPermit();
      try {
        return call.call();
      } catch (RuntimeException e) {
        if (!isRateLimited(e) || attempt >= MAX_ATTEMPTS) {
          throw new IOException("Failed while " + description + ": " + e.getMessage(), e);
        }
        logger.warn(
            "Throttled while {}. Retrying in {}ms (attempt {} of {}).",
            description,
            backoffMs,
            attempt,
            MAX_ATTEMPTS);
        sleep(backoffMs);
        backoffMs *= 2;
      }
    }
  }

  /** Returns whether {@code throwable} or any of its causes indicates Databricks throttling. */
  static boolean isRateLimited(@Nullable Throwable throwable) {
    for (Throwable current = throwable; current != null; current = current.getCause()) {
      if (current instanceof DatabricksError) {
        DatabricksError error = (DatabricksError) current;
        if (error.getStatusCode() == 429
            || "TOO_MANY_REQUESTS".equalsIgnoreCase(error.getErrorCode())) {
          return true;
        }
      }
      String message = current.getMessage();
      if (message != null
          && (message.contains("429")
              || message.contains("TOO_MANY_REQUESTS")
              || message.contains("Current request has to be retried"))) {
        return true;
      }
    }
    return false;
  }

  private static void sleep(long millis) throws InterruptedIOException {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InterruptedIOException("Interrupted while backing off from Databricks throttling.");
    }
  }
}
