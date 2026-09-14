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

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksBackoff.NO_RETRY_AFTER;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksBackoff.delayMillis;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksBackoff.retryAfterMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksBackoffTest {

  private static final long INITIAL_DELAY_MILLIS = 1_000L;
  private static final long MAX_DELAY_MILLIS = 60_000L;
  private static final long MAX_RETRY_AFTER_MILLIS = 300_000L;

  /** The exponential ceiling the delay for a given attempt is drawn against. */
  private static long ceiling(int attempt) {
    return Math.min(MAX_DELAY_MILLIS, INITIAL_DELAY_MILLIS * (long) Math.pow(2, attempt - 1));
  }

  @Test
  public void delayMillis_staysWithinTheEqualJitterBand() {
    for (int attempt = 1; attempt <= 10; attempt++) {
      long ceiling = ceiling(attempt);
      for (int draw = 0; draw < 50; draw++) {
        long delay = delayMillis(attempt);
        assertTrue(
            "attempt " + attempt + " produced " + delay, delay >= ceiling / 2 && delay <= ceiling);
      }
    }
  }

  @Test
  public void delayMillis_isCappedRatherThanOverflowing() {
    // A shift-based implementation would overflow long here and could go negative.
    long delay = delayMillis(Integer.MAX_VALUE);
    assertTrue(String.valueOf(delay), delay >= MAX_DELAY_MILLIS / 2 && delay <= MAX_DELAY_MILLIS);
  }

  @Test
  public void delayMillis_variesAcrossCalls() {
    // The whole reason for jitter: concurrently throttled tasks must not wake together.
    Set<Long> delays = new HashSet<>();
    for (int draw = 0; draw < 100; draw++) {
      delays.add(delayMillis(4));
    }
    assertTrue("Expected varied delays but saw " + delays, delays.size() > 1);
  }

  @Test
  public void retryAfterMillis_readsDeltaSeconds() {
    assertEquals(120_000L, retryAfterMillis("120"));
    assertEquals(0L, retryAfterMillis("0"));
    assertEquals(3_000L, retryAfterMillis("  3  "));
  }

  @Test
  public void retryAfterMillis_readsAnHttpDate() {
    long expected = 30_000L;
    String header =
        ZonedDateTime.now(java.time.ZoneOffset.UTC)
            .plusSeconds(expected / 1000)
            .format(DateTimeFormatter.RFC_1123_DATE_TIME);

    long actual = retryAfterMillis(header);

    // The header has one-second resolution, and time passes between formatting and parsing.
    assertTrue(
        "Expected roughly " + expected + "ms but was " + actual,
        Math.abs(actual - expected) <= 2_000L);
  }

  @Test
  public void retryAfterMillis_capsAnExcessiveValue() {
    assertEquals(MAX_RETRY_AFTER_MILLIS, retryAfterMillis("86400"));
  }

  @Test
  public void retryAfterMillis_rejectsUnusableValues() {
    assertEquals(NO_RETRY_AFTER, retryAfterMillis(null));
    assertEquals(NO_RETRY_AFTER, retryAfterMillis(""));
    assertEquals(NO_RETRY_AFTER, retryAfterMillis("   "));
    assertEquals(NO_RETRY_AFTER, retryAfterMillis("soon"));
    assertEquals(NO_RETRY_AFTER, retryAfterMillis("-5"));
    // A date already in the past means the server is not asking us to wait.
    assertEquals(
        NO_RETRY_AFTER,
        retryAfterMillis(
            ZonedDateTime.now(java.time.ZoneOffset.UTC)
                .minusMinutes(5)
                .format(DateTimeFormatter.RFC_1123_DATE_TIME)));
  }
}
