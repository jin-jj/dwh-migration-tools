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

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.concurrent.Executors.newFixedThreadPool;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs one SQL statement or Unity Catalog API request per object, such as {@code SHOW TABLE
 * EXTENDED} or a table listing request for each schema, on a bounded number of threads, and passes
 * the results to a consumer in input order.
 *
 * <p>The consumer runs on the calling thread, so it can write to a file without synchronization.
 * Only a few statements per thread are submitted ahead of the consumer, so a slow statement doesn't
 * make the results of the following statements pile up in memory.
 *
 * <p>A statement that fails with an {@link IOException} or {@link UncheckedIOException} is logged
 * and its object is skipped, so one broken object doesn't discard the output of the others. If
 * every statement fails, the cause is likely systemic, such as lost access to the warehouse, so the
 * run fails with the first failure as its cause. Other exceptions propagate immediately.
 */
@ThreadSafe
final class ObjectStatementRunner implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(ObjectStatementRunner.class);

  /**
   * Statements submitted per thread ahead of the consumer. More than one keeps the threads busy
   * while the consumer waits for a slow statement.
   */
  private static final int PENDING_STATEMENTS_PER_THREAD = 2;

  /** Executes the statement for one object. */
  interface Statement<I, O> {

    @Nonnull
    O execute(@Nonnull I input) throws IOException;
  }

  /** Consumes the result of one successful statement. */
  interface ResultConsumer<I, O> {

    void accept(@Nonnull I input, @Nonnull O result) throws IOException;
  }

  private final ExecutorService executor;
  private final int maxPendingStatements;

  ObjectStatementRunner(int maxConcurrentStatements) {
    Preconditions.checkArgument(
        maxConcurrentStatements > 0,
        "Max concurrent statements must be positive, got %s.",
        maxConcurrentStatements);
    this.executor =
        newFixedThreadPool(
            maxConcurrentStatements,
            new ThreadFactoryBuilder()
                .setNameFormat("databricks-statement-%d")
                .setDaemon(true)
                .build());
    this.maxPendingStatements = PENDING_STATEMENTS_PER_THREAD * maxConcurrentStatements;
  }

  /**
   * Runs {@code statement} for each input and passes each successful result to {@code consumer}, in
   * input order.
   *
   * @param statementName the statement, for log messages, e.g. {@code SHOW TABLE EXTENDED}
   * @param describer describes an input for log messages, e.g. as a quoted schema name
   * @throws InterruptedIOException if the calling thread was interrupted
   * @throws IOException if every statement failed, or if the consumer failed
   */
  <I, O> void runInOrder(
      @Nonnull String statementName,
      @Nonnull Iterable<I> inputs,
      @Nonnull Function<? super I, String> describer,
      @Nonnull Statement<? super I, ? extends O> statement,
      @Nonnull ResultConsumer<? super I, ? super O> consumer)
      throws IOException {
    Failures failures = new Failures(statementName);
    Deque<PendingStatement<I, O>> pendingStatements = new ArrayDeque<>();
    try {
      for (I input : inputs) {
        Future<O> result = executor.submit(() -> statement.execute(input));
        pendingStatements.addLast(new PendingStatement<>(input, result));
        if (pendingStatements.size() >= maxPendingStatements) {
          consume(pendingStatements.removeFirst(), describer, consumer, failures);
        }
      }

      while (!pendingStatements.isEmpty()) {
        consume(pendingStatements.removeFirst(), describer, consumer, failures);
      }
    } finally {
      for (PendingStatement<I, O> pendingStatement : pendingStatements) {
        pendingStatement.result.cancel(true);
      }
    }
    failures.throwIfAllFailed();
  }

  private static <I, O> void consume(
      @Nonnull PendingStatement<I, O> pendingStatement,
      @Nonnull Function<? super I, String> describer,
      @Nonnull ResultConsumer<? super I, ? super O> consumer,
      @Nonnull Failures failures)
      throws IOException {
    I input = pendingStatement.input;
    O result;
    try {
      result = pendingStatement.result.get();
    } catch (InterruptedException e) {
      pendingStatement.result.cancel(true);
      Thread.currentThread().interrupt();
      InterruptedIOException interrupted =
          new InterruptedIOException(
              String.format(
                  "Interrupted while waiting for %s for %s.",
                  failures.statementName, describer.apply(input)));
      interrupted.initCause(e);
      throw interrupted;
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        failures.record(describer.apply(input), (IOException) cause);
        return;
      }

      if (cause instanceof UncheckedIOException) {
        failures.record(describer.apply(input), ((UncheckedIOException) cause).getCause());
        return;
      }

      throwIfUnchecked(cause);
      throw new IllegalStateException("Statement threw an undeclared checked exception.", cause);
    }
    failures.recordSuccess();
    consumer.accept(input, result);
  }

  /** Stops the threads, interrupting the statements that are still running. */
  @Override
  public void close() {
    executor.shutdownNow();
  }

  /** A submitted statement and the object it runs for. */
  private static final class PendingStatement<I, O> {

    final I input;
    final Future<O> result;

    PendingStatement(@Nonnull I input, @Nonnull Future<O> result) {
      this.input = input;
      this.result = result;
    }
  }

  /** Counts statement outcomes and logs each failure. Used by the calling thread only. */
  private static final class Failures {

    private final String statementName;
    private int successCount;
    private int failureCount;
    @CheckForNull private IOException firstFailure;

    Failures(@Nonnull String statementName) {
      this.statementName = statementName;
    }

    void recordSuccess() {
      successCount++;
    }

    void record(@Nonnull String objectDescription, @Nonnull IOException failure) {
      failureCount++;
      if (firstFailure == null) {
        firstFailure = failure;
      }
      logger.warn("Skipping {} for {}: {}", statementName, objectDescription, failure.getMessage());
      logger.debug("{} failed for {}.", statementName, objectDescription, failure);
    }

    /** Logs how many objects were skipped, and throws if no statement succeeded. */
    void throwIfAllFailed() throws IOException {
      if (firstFailure == null) {
        return;
      }

      if (successCount == 0) {
        throw new IOException(
            String.format(
                "%s failed for all %d objects. First failure: %s",
                statementName, failureCount, firstFailure.getMessage()),
            firstFailure);
      }

      logger.warn(
          "{} failed for {} of {} objects; they are missing from the output.",
          statementName,
          failureCount,
          failureCount + successCount);
    }
  }
}
