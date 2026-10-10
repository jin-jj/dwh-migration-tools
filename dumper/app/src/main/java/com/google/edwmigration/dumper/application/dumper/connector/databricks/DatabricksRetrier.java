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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableSet;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksChunkDownloader.Sleeper;
import java.io.InterruptedIOException;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retries the Databricks API requests that fail with a transient error: HTTP 429 (too many
 * requests), 500 (internal error), 502 (bad gateway), 503 (unavailable) or 504 (gateway timeout).
 *
 * <p>A failed request is retried after a backoff with jitter, so that the requests that failed
 * together don't retry together. The backoff starts at {@link #INITIAL_BACKOFF_MILLIS} and doubles
 * with each attempt, up to {@link #MAX_BACKOFF_MILLIS}. A request gets up to {@link #MAX_ATTEMPTS}
 * attempts, about 3 to 6 minutes, which rides out a short outage.
 *
 * <p>During a longer outage, each request would wait that long before it fails. So once requests
 * have been failing for {@link #MAX_FAILING_DURATION} without a success, a failed request isn't
 * retried, until a request succeeds again.
 *
 * <p>The SDK retries some of these failures itself, but not an HTTP 500 response to a statement
 * submission, because the statement may have started. The statements of the dumper only read
 * metadata, so running one twice is harmless.
 */
@ThreadSafe
final class DatabricksRetrier {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksRetrier.class);

  /** The attempts of a request that keeps failing with a transient error. */
  static final int MAX_ATTEMPTS = 12;

  static final long INITIAL_BACKOFF_MILLIS = 1_000L;

  static final long MAX_BACKOFF_MILLIS = 60_000L;

  /** How long requests may fail without a success before failed requests aren't retried. */
  static final Duration MAX_FAILING_DURATION = Duration.ofMinutes(5);

  /** The HTTP statuses of a request that may succeed later. */
  private static final ImmutableSet<Integer> RETRYABLE_STATUS_CODES =
      ImmutableSet.of(429, 500, 502, 503, 504);

  /** The value of {@link #failingSinceNanos} after a success. */
  private static final long NOT_FAILING = Long.MIN_VALUE;

  /** Sends one request, and returns its response. */
  interface Call<T> {

    @CheckForNull
    T call();
  }

  private final Sleeper sleeper;
  private final Ticker ticker;

  /**
   * When the first transient failure since the last success happened, by {@link #ticker}, or {@link
   * #NOT_FAILING}.
   */
  private final AtomicLong failingSinceNanos = new AtomicLong(NOT_FAILING);

  /** Whether the end of retries was logged since the last success. */
  private final AtomicBoolean retriesStoppedLogged = new AtomicBoolean();

  DatabricksRetrier() {
    this(Thread::sleep, Ticker.systemTicker());
  }

  @VisibleForTesting
  DatabricksRetrier(@Nonnull Sleeper sleeper, @Nonnull Ticker ticker) {
    this.sleeper = Preconditions.checkNotNull(sleeper, "Sleeper was null.");
    this.ticker = Preconditions.checkNotNull(ticker, "Ticker was null.");
  }

  /**
   * Sends a request, and sends it again while it fails with a transient error.
   *
   * @param description the request, for log messages, such as {@code Listing catalogs}
   * @return the response of the request
   * @throws InterruptedIOException if the thread was interrupted while waiting to retry
   * @throws RuntimeException the last failure of the request, if it isn't retried
   */
  @CheckForNull
  <T> T call(@Nonnull String description, @Nonnull Call<T> call) throws InterruptedIOException {
    Preconditions.checkNotNull(description, "Description was null.");
    Preconditions.checkNotNull(call, "Call was null.");
    for (int attempt = 1; ; attempt++) {
      T response;
      try {
        response = call.call();
      } catch (RuntimeException e) {
        if (!isRetryable(e) || !recordTransientFailure() || attempt >= MAX_ATTEMPTS) {
          throw e;
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

      failingSinceNanos.set(NOT_FAILING);
      retriesStoppedLogged.set(false);
      return response;
    }
  }

  /**
   * Records a transient failure, and returns whether failed requests are still retried: they aren't
   * once requests have been failing for {@link #MAX_FAILING_DURATION} without a success.
   */
  private boolean recordTransientFailure() {
    long nowNanos = ticker.read();
    failingSinceNanos.compareAndSet(NOT_FAILING, nowNanos);
    long sinceNanos = failingSinceNanos.get();
    // NOT_FAILING here means that a request succeeded after this one failed.
    if (sinceNanos == NOT_FAILING || nowNanos - sinceNanos < MAX_FAILING_DURATION.toNanos()) {
      return true;
    }

    if (retriesStoppedLogged.compareAndSet(false, true)) {
      logger.warn(
          "Databricks requests have failed for {} minutes without a success, so failed requests"
              + " aren't retried until a request succeeds.",
          MAX_FAILING_DURATION.toMinutes());
    }
    return false;
  }

  /** Returns whether the failure, or one of its causes, has a transient HTTP status. */
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
