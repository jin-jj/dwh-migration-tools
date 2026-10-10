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

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRetrier.INITIAL_BACKOFF_MILLIS;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRetrier.MAX_ATTEMPTS;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRetrier.MAX_BACKOFF_MILLIS;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRetrier.MAX_FAILING_DURATION;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRetrier.backoffMillis;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRetrier.isRetryable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.databricks.sdk.core.DatabricksError;
import com.databricks.sdk.core.DatabricksException;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksChunkDownloader.Sleeper;
import java.io.InterruptedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class DatabricksRetrierTest {

  /** The failures that may succeed later, with the error codes that Databricks sends. */
  @DataPoints("transientFailures")
  public static final ImmutableList<RuntimeException> TRANSIENT_FAILURES =
      ImmutableList.of(
          new DatabricksError("REQUEST_LIMIT_EXCEEDED", "Too many requests.", 429),
          new DatabricksError("INTERNAL_ERROR", "The request failed.", 500),
          new DatabricksError("BAD_GATEWAY", "Bad gateway.", 502),
          new DatabricksError("TEMPORARILY_UNAVAILABLE", "Unavailable.", 503),
          new DatabricksError("DEADLINE_EXCEEDED", "Gateway timeout.", 504),
          // The SDK throws this after its own retries of a throttled request.
          new DatabricksException(
              "Request failed after 4 retries",
              new DatabricksError("REQUEST_LIMIT_EXCEEDED", "Too many requests.", 429)));

  @DataPoints("permanentFailures")
  public static final ImmutableList<RuntimeException> PERMANENT_FAILURES =
      ImmutableList.of(
          new DatabricksError("BAD_REQUEST", "Syntax error.", 400),
          new DatabricksError("PERMISSION_DENIED", "No access.", 403),
          new DatabricksError("RESOURCE_DOES_NOT_EXIST", "No such warehouse.", 404),
          new DatabricksException("Connection reset"),
          new IllegalStateException("Unexpected state."));

  /** The backoffs sampled per attempt, because a backoff is random. */
  private static final int BACKOFF_SAMPLES = 100;

  private static final String RESPONSE = "response";

  private final FakeClock clock = new FakeClock();
  private final DatabricksRetrier retrier = new DatabricksRetrier(clock, clock);

  @Theory
  public void call_retriesATransientFailureUntilTheRequestSucceeds(
      @FromDataPoints("transientFailures") RuntimeException failure) throws Exception {
    AtomicInteger attempts = new AtomicInteger();

    String response = retrier.call("Listing catalogs", () -> failTwice(attempts, failure));

    assertEquals(RESPONSE, response);
    assertEquals(3, attempts.get());
    assertEquals(2, clock.sleeps.size());
    assertBetween(INITIAL_BACKOFF_MILLIS / 2, INITIAL_BACKOFF_MILLIS, clock.sleeps.get(0));
    assertBetween(INITIAL_BACKOFF_MILLIS, 2 * INITIAL_BACKOFF_MILLIS, clock.sleeps.get(1));
    assertTrue(isRetryable(failure));
  }

  @Theory
  public void call_throwsAPermanentFailureWithoutRetrying(
      @FromDataPoints("permanentFailures") RuntimeException failure) {
    AtomicInteger attempts = new AtomicInteger();

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> retrier.call("Getting a table", () -> alwaysFail(attempts, failure)));

    assertSame(failure, thrown);
    assertEquals(1, attempts.get());
    assertEquals(ImmutableList.of(), clock.sleeps);
    assertFalse(isRetryable(failure));
  }

  @Test
  public void call_throwsTheLastFailureAfterTheLastAttempt() {
    AtomicInteger attempts = new AtomicInteger();
    DatabricksError failure = new DatabricksError("INTERNAL_ERROR", "The request failed.", 500);

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> retrier.call("Polling a statement", () -> alwaysFail(attempts, failure)));

    assertSame(failure, thrown);
    assertEquals(MAX_ATTEMPTS, attempts.get());
    assertEquals(MAX_ATTEMPTS - 1, clock.sleeps.size());
    long ceiling = INITIAL_BACKOFF_MILLIS;
    for (long sleep : clock.sleeps) {
      assertBetween(ceiling / 2, ceiling, sleep);
      ceiling = Math.min(2 * ceiling, MAX_BACKOFF_MILLIS);
    }
  }

  @Test
  public void call_stopsRetryingWhileRequestsFailForTheMaxFailingDuration() throws Exception {
    DatabricksError failure = new DatabricksError("INTERNAL_ERROR", "The request failed.", 500);
    Duration attemptDuration = MAX_FAILING_DURATION.dividedBy(2);
    AtomicInteger outageAttempts = new AtomicInteger();

    // Each attempt takes half of the limit, so the third attempt fails at the limit.
    assertThrows(
        RuntimeException.class,
        () ->
            retrier.call(
                "Submitting a statement",
                () -> {
                  clock.advance(attemptDuration);
                  return alwaysFail(outageAttempts, failure);
                }));
    AtomicInteger laterAttempts = new AtomicInteger();
    assertThrows(
        RuntimeException.class,
        () ->
            retrier.call("Submitting another statement", () -> alwaysFail(laterAttempts, failure)));
    String success = retrier.call("Submitting a third statement", () -> RESPONSE);
    AtomicInteger recoveredAttempts = new AtomicInteger();
    String recovered =
        retrier.call("Submitting a fourth statement", () -> failTwice(recoveredAttempts, failure));

    assertEquals(3, outageAttempts.get());
    assertEquals(1, laterAttempts.get());
    assertEquals(RESPONSE, success);
    assertEquals(RESPONSE, recovered);
    assertEquals(3, recoveredAttempts.get());
    // Each attempt that is retried waits once.
    assertEquals(outageAttempts.get() - 1 + recoveredAttempts.get() - 1, clock.sleeps.size());
  }

  @Test
  public void call_stopsWhenInterruptedWhileWaitingToRetry() {
    DatabricksRetrier interruptedRetrier =
        new DatabricksRetrier(
            millis -> {
              throw new InterruptedException("Interrupted by the test.");
            },
            clock);
    AtomicInteger attempts = new AtomicInteger();
    DatabricksError failure = new DatabricksError("INTERNAL_ERROR", "The request failed.", 500);

    InterruptedIOException thrown =
        assertThrows(
            InterruptedIOException.class,
            () -> interruptedRetrier.call("Listing catalogs", () -> alwaysFail(attempts, failure)));

    assertTrue(Thread.interrupted());
    assertEquals(1, attempts.get());
    assertTrue(thrown.getMessage(), thrown.getMessage().startsWith("Listing catalogs"));
  }

  @Test
  public void backoffMillis_isBetweenHalfAndAllOfACeilingThatDoubles() {
    long ceiling = INITIAL_BACKOFF_MILLIS;
    for (int attempt = 1; attempt < MAX_ATTEMPTS; attempt++) {
      assertBackoffsBetween(attempt, ceiling / 2, ceiling);
      ceiling = Math.min(2 * ceiling, MAX_BACKOFF_MILLIS);
    }
  }

  @Test
  public void backoffMillis_doesNotExceedTheMaximum() {
    assertBackoffsBetween(Integer.MAX_VALUE, MAX_BACKOFF_MILLIS / 2, MAX_BACKOFF_MILLIS);
  }

  @Test
  public void backoffMillis_rejectsAnAttemptBeforeTheFirst() {
    assertThrows(IllegalArgumentException.class, () -> backoffMillis(0));
  }

  private static String failTwice(AtomicInteger attempts, RuntimeException failure) {
    if (attempts.incrementAndGet() <= 2) {
      throw failure;
    }
    return RESPONSE;
  }

  private static String alwaysFail(AtomicInteger attempts, RuntimeException failure) {
    attempts.incrementAndGet();
    throw failure;
  }

  private static void assertBackoffsBetween(int attempt, long minMillis, long maxMillis) {
    for (int sample = 0; sample < BACKOFF_SAMPLES; sample++) {
      assertBetween(minMillis, maxMillis, backoffMillis(attempt));
    }
  }

  private static void assertBetween(long minMillis, long maxMillis, long millis) {
    assertTrue(
        String.format("%d is not in [%d, %d].", millis, minMillis, maxMillis),
        millis >= minMillis && millis <= maxMillis);
  }

  /** A ticker that moves only when a test advances it, and a sleeper that records its sleeps. */
  private static final class FakeClock extends Ticker implements Sleeper {

    final List<Long> sleeps = new ArrayList<>();
    private long nanos;

    @Override
    public long read() {
      return nanos;
    }

    @Override
    public void sleep(long millis) {
      sleeps.add(millis);
    }

    void advance(Duration duration) {
      nanos += duration.toNanos();
    }
  }
}
