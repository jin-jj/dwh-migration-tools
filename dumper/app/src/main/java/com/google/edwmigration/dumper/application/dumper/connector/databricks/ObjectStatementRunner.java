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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs one SQL statement per object, such as {@code SHOW CREATE TABLE} for each table, and passes
 * the results to a consumer in input order.
 *
 * <p>A statement that fails with an {@link IOException} or {@link UncheckedIOException} is logged
 * and its object is skipped, so one broken object doesn't discard the output of the others. If
 * every statement fails, the cause is likely systemic, such as lost access to the warehouse, so the
 * run fails with the first failure as its cause. Other exceptions propagate immediately.
 */
final class ObjectStatementRunner {

  private static final Logger logger = LoggerFactory.getLogger(ObjectStatementRunner.class);

  /** Executes the statement for one object. */
  interface Statement<I, O> {

    @Nonnull
    O execute(@Nonnull I input) throws IOException;
  }

  /** Consumes the result of one successful statement. */
  interface ResultConsumer<I, O> {

    void accept(@Nonnull I input, @Nonnull O result) throws IOException;
  }

  private ObjectStatementRunner() {}

  /**
   * Runs {@code statement} for each input and passes each successful result to {@code consumer}, in
   * input order.
   *
   * @param statementName the statement, for log messages, e.g. {@code SHOW CREATE TABLE}
   * @param describer describes an input for log messages, e.g. as a quoted table name
   * @throws IOException if every statement failed, or if the consumer failed
   */
  static <I, O> void runInOrder(
      @Nonnull String statementName,
      @Nonnull Iterable<I> inputs,
      @Nonnull Function<? super I, String> describer,
      @Nonnull Statement<? super I, ? extends O> statement,
      @Nonnull ResultConsumer<? super I, ? super O> consumer)
      throws IOException {
    Failures failures = new Failures(statementName);
    for (I input : inputs) {
      O result;
      try {
        result = statement.execute(input);
      } catch (IOException e) {
        failures.record(describer.apply(input), e);
        continue;
      } catch (UncheckedIOException e) {
        failures.record(describer.apply(input), e.getCause());
        continue;
      }
      failures.recordSuccess();
      consumer.accept(input, result);
    }
    failures.throwIfAllFailed();
  }

  /** Counts statement outcomes and logs each failure. */
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
