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

import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ObjectStatementRunnerTest {

  private static final int MAX_CONCURRENT_STATEMENTS = 4;
  private static final long TIMEOUT_SECONDS = 10;

  private final ObjectStatementRunner runner = new ObjectStatementRunner(MAX_CONCURRENT_STATEMENTS);

  @After
  public void closeRunner() {
    runner.close();
  }

  @Test
  public void runInOrder_skipsFailedObjectsAndKeepsInputOrder() throws IOException {
    ImmutableList<String> inputs = ImmutableList.of("a", "b", "c", "d");
    ImmutableSet<String> checkedFailures = ImmutableSet.of("b");
    ImmutableSet<String> uncheckedFailures = ImmutableSet.of("c");
    List<String> consumed = new ArrayList<>();

    runner.runInOrder(
        "TEST",
        inputs,
        input -> input,
        input -> {
          if (checkedFailures.contains(input)) {
            throw new IOException("Broken " + input);
          }
          if (uncheckedFailures.contains(input)) {
            throw new UncheckedIOException(new IOException("Broken " + input));
          }
          return input.toUpperCase(Locale.ROOT);
        },
        (input, result) -> consumed.add(input + "=" + result));

    List<String> expected = new ArrayList<>();
    for (String input : inputs) {
      if (!checkedFailures.contains(input) && !uncheckedFailures.contains(input)) {
        expected.add(input + "=" + input.toUpperCase(Locale.ROOT));
      }
    }
    assertEquals(expected, consumed);
  }

  @Test
  public void runInOrder_keepsInputOrderWhenLaterStatementsFinishFirst() throws IOException {
    ImmutableList<Integer> inputs = inputs(2 * MAX_CONCURRENT_STATEMENTS);
    // The first statement finishes only after the statements that run alongside it.
    CountDownLatch othersFinished = new CountDownLatch(MAX_CONCURRENT_STATEMENTS - 1);
    List<Integer> finishOrder = Collections.synchronizedList(new ArrayList<>());
    List<Integer> consumed = new ArrayList<>();

    runner.runInOrder(
        "TEST",
        inputs,
        String::valueOf,
        input -> {
          if (input == 0) {
            awaitUninterruptibly(othersFinished, TIMEOUT_SECONDS, SECONDS);
          }
          finishOrder.add(input);
          if (input > 0 && input < MAX_CONCURRENT_STATEMENTS) {
            othersFinished.countDown();
          }
          return input;
        },
        (input, result) -> consumed.add(result));

    assertNotEquals(inputs.get(0), finishOrder.get(0));
    assertEquals(inputs, consumed);
  }

  @Test
  public void runInOrder_runsUpToMaxConcurrentStatementsAtOnce() throws IOException {
    CountDownLatch allThreadsBusy = new CountDownLatch(MAX_CONCURRENT_STATEMENTS);
    AtomicInteger running = new AtomicInteger();
    AtomicInteger maxRunning = new AtomicInteger();

    runner.runInOrder(
        "TEST",
        inputs(5 * MAX_CONCURRENT_STATEMENTS),
        String::valueOf,
        input -> {
          maxRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
          allThreadsBusy.countDown();
          // Holds the first statements until MAX_CONCURRENT_STATEMENTS of them run at once.
          awaitUninterruptibly(allThreadsBusy, TIMEOUT_SECONDS, SECONDS);
          running.decrementAndGet();
          return input;
        },
        (input, result) -> {});

    assertEquals(MAX_CONCURRENT_STATEMENTS, maxRunning.get());
  }

  @Test
  public void runInOrder_throwsFirstFailureWhenEveryStatementFails() {
    ImmutableMap<String, IOException> failuresByInput =
        ImmutableMap.of("a", new IOException("Broken a"), "b", new IOException("Broken b"));
    ImmutableList<String> inputs = failuresByInput.keySet().asList();

    IOException thrown =
        assertThrows(
            IOException.class,
            () ->
                runner.runInOrder(
                    "TEST",
                    inputs,
                    input -> input,
                    input -> {
                      throw failuresByInput.get(input);
                    },
                    (input, result) -> fail("No statement succeeded.")));

    assertSame(failuresByInput.get(inputs.get(0)), thrown.getCause());
    assertTrue(thrown.getMessage().contains("all " + inputs.size() + " objects"));
  }

  @Test
  public void runInOrder_acceptsNoInputs() throws IOException {
    runner.runInOrder(
        "TEST",
        ImmutableList.<String>of(),
        input -> input,
        input -> input,
        (input, result) -> fail("There are no inputs."));
  }

  @Test
  public void runInOrder_propagatesRuntimeExceptions() {
    IllegalStateException bug = new IllegalStateException("Bug");

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                runner.runInOrder(
                    "TEST",
                    ImmutableList.of("a", "b"),
                    input -> input,
                    input -> {
                      throw bug;
                    },
                    (input, result) -> fail("No statement succeeded.")));

    assertSame(bug, thrown);
  }

  @Test
  public void runInOrder_throwsInterruptedIoExceptionWhenCallerIsInterrupted() {
    Thread.currentThread().interrupt();
    try {
      assertThrows(
          InterruptedIOException.class,
          () ->
              runner.runInOrder(
                  "TEST",
                  ImmutableList.of("a"),
                  input -> input,
                  input -> input,
                  (input, result) -> fail("The caller was interrupted.")));
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      // Clears the interrupt, so it doesn't leak into other tests on this thread.
      Thread.interrupted();
    }
  }

  @Test
  public void runInOrder_cancelsRunningStatementWhenInterrupted() throws InterruptedException {
    CountDownLatch statementStarted = new CountDownLatch(1);
    CountDownLatch statementInterrupted = new CountDownLatch(1);
    AtomicReference<Throwable> thrown = new AtomicReference<>();
    AtomicBoolean interruptFlagKept = new AtomicBoolean();
    Thread caller =
        new Thread(
            () -> {
              try {
                runner.runInOrder(
                    "TEST",
                    ImmutableList.of("a"),
                    input -> input,
                    input -> {
                      statementStarted.countDown();
                      try {
                        Thread.sleep(SECONDS.toMillis(TIMEOUT_SECONDS));
                      } catch (InterruptedException e) {
                        statementInterrupted.countDown();
                        throw new InterruptedIOException("Statement was cancelled.");
                      }
                      return input;
                    },
                    (input, result) -> fail("The statement was cancelled."));
              } catch (Throwable e) {
                thrown.set(e);
              }
              interruptFlagKept.set(Thread.currentThread().isInterrupted());
            });

    caller.start();
    assertTrue(statementStarted.await(TIMEOUT_SECONDS, SECONDS));
    caller.interrupt();
    caller.join(SECONDS.toMillis(TIMEOUT_SECONDS));

    assertTrue(String.valueOf(thrown.get()), thrown.get() instanceof InterruptedIOException);
    assertTrue(interruptFlagKept.get());
    assertTrue(statementInterrupted.await(TIMEOUT_SECONDS, SECONDS));
  }

  private static ImmutableList<Integer> inputs(int count) {
    ImmutableList.Builder<Integer> inputs = ImmutableList.builder();
    for (int i = 0; i < count; i++) {
      inputs.add(i);
    }
    return inputs.build();
  }
}
