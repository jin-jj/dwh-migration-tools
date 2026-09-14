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

import com.google.common.base.Preconditions;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;

/**
 * Backoff policy shared by every Databricks retry loop.
 *
 * <p>Jitter matters here more than the curve does. A dump runs several tasks concurrently against
 * one workspace, and throttling tends to hit all of them at once; without jitter they would all
 * sleep for the same interval and collide again on every subsequent attempt.
 *
 * <p>The delay is "equal jitter": half the exponential ceiling plus a random share of the other
 * half. Full jitter decorrelates slightly better but can return a delay of nearly zero, which is
 * the wrong thing to do to an endpoint that has just said it is overloaded.
 */
final class DatabricksBackoff {

  private static final long INITIAL_DELAY_MILLIS = 1_000L;
  private static final long MAX_DELAY_MILLIS = 60_000L;

  /**
   * Ceiling on an honored {@code Retry-After}. The header is under server control and a
   * misconfigured or hostile value would otherwise stall a dump indefinitely; past this point it is
   * better to fail the task and let the retry budget run out.
   */
  private static final long MAX_RETRY_AFTER_MILLIS = 300_000L;

  /** Returned when no usable {@code Retry-After} was present. */
  static final long NO_RETRY_AFTER = -1L;

  private DatabricksBackoff() {}

  /**
   * Returns how long to wait before the given 1-based retry attempt.
   *
   * <p>Callers that have a {@code Retry-After} from the server should prefer it over this.
   */
  static long delayMillis(int attempt) {
    Preconditions.checkArgument(attempt >= 1, "Attempt must be at least 1, but was %s.", attempt);
    long ceiling = INITIAL_DELAY_MILLIS;
    // Doubling in a loop rather than shifting, because a large attempt would overflow the shift.
    for (int i = 1; i < attempt && ceiling < MAX_DELAY_MILLIS; i++) {
      ceiling *= 2;
    }
    ceiling = Math.min(ceiling, MAX_DELAY_MILLIS);
    long half = ceiling / 2;
    return half + ThreadLocalRandom.current().nextLong(half + 1);
  }

  /**
   * Parses a {@code Retry-After} header value, in either of the two forms RFC 7231 permits:
   * delta-seconds, or an HTTP date.
   *
   * @return the delay in milliseconds, or {@link #NO_RETRY_AFTER} if the header was absent,
   *     unparseable, or already in the past.
   */
  static long retryAfterMillis(@Nullable String headerValue) {
    if (headerValue == null) {
      return NO_RETRY_AFTER;
    }
    String trimmed = headerValue.trim();
    if (trimmed.isEmpty()) {
      return NO_RETRY_AFTER;
    }
    long millis;
    try {
      millis = Long.parseLong(trimmed) * 1000L;
    } catch (NumberFormatException e) {
      millis = millisUntilHttpDate(trimmed);
    }
    if (millis < 0) {
      return NO_RETRY_AFTER;
    }
    return Math.min(millis, MAX_RETRY_AFTER_MILLIS);
  }

  /** Returns the milliseconds from now until an HTTP date, or a negative value if unusable. */
  private static long millisUntilHttpDate(String value) {
    try {
      ZonedDateTime deadline = ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME);
      return deadline.toInstant().toEpochMilli() - System.currentTimeMillis();
    } catch (DateTimeParseException e) {
      return NO_RETRY_AFTER;
    }
  }
}
