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

import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for the metastore-wide {@code system.information_schema} extraction tier.
 *
 * <p>This is the first and cheapest tier: one query covers every catalog in the metastore. It is
 * marked optional and swallows its own failure so that the per-catalog tier behind it can run.
 */
abstract class AbstractDatabricksSystemSqlTask extends AbstractDatabricksSqlTask {

  private static final Logger logger =
      LoggerFactory.getLogger(AbstractDatabricksSystemSqlTask.class);

  AbstractDatabricksSystemSqlTask(
      @Nonnull String targetPath,
      @Nonnull Predicate<String> catalogPredicate,
      @Nonnull Predicate<String> schemaPredicate) {
    super(targetPath, catalogPredicate, schemaPredicate);
  }

  AbstractDatabricksSystemSqlTask(
      @Nonnull String targetPath, @Nonnull Predicate<String> catalogPredicate) {
    super(targetPath, catalogPredicate);
  }

  @Nonnull
  @Override
  public TaskCategory getCategory() {
    return TaskCategory.OPTIONAL;
  }

  @Override
  public boolean handleException(Exception e) {
    logger.info(
        "Databricks system table query for '{}' failed ({}), falling back to catalog-level queries.",
        getTargetPath(),
        e.getMessage());
    return true;
  }

  /** Receives one result row at a time. Unlike a {@code Consumer} it may write I/O. */
  interface RowHandler {
    void accept(@Nonnull List<String> row) throws IOException;
  }

  /**
   * Runs {@code sql}, and if it fails runs {@code compatibilitySql} instead.
   *
   * <p>The primary statements use {@code unix_millis()} so that timestamps come back as epoch
   * milliseconds. That function is missing on older Databricks runtimes, hence the plain-column
   * variant. The fallback only runs if the primary failed before emitting a row, because otherwise
   * re-running the query would write the already-emitted rows a second time.
   *
   * @throws SQLException with the original failure if both statements fail.
   */
  protected void executeWithCompatibilityFallback(
      @Nonnull DatabricksHandle handle,
      @Nonnull String sql,
      @Nullable String compatibilitySql,
      @Nonnull RowHandler handler)
      throws SQLException {
    RowCounter counter = new RowCounter(handler);
    try {
      DatabricksSqlHelper.executeBulkQueryOrThrow(handle, sql, counter);
    } catch (SQLException e) {
      if (compatibilitySql == null || counter.rows > 0) {
        throw e;
      }
      logger.info("Retrying '{}' without unix_millis() after: {}", getTargetPath(), e.getMessage());
      try {
        DatabricksSqlHelper.executeBulkQueryOrThrow(handle, compatibilitySql, counter);
      } catch (SQLException retried) {
        e.addSuppressed(retried);
        throw e;
      }
    }
  }

  /**
   * Adapts a {@link RowHandler} to the helper's consumer, counting what it has emitted.
   *
   * <p>The consumer contract cannot declare {@code IOException}, so a write failure travels as an
   * {@link UncheckedIOException} and is unwrapped by the helper's caller.
   */
  private static final class RowCounter implements Consumer<List<String>> {

    private final RowHandler handler;
    private long rows;

    RowCounter(RowHandler handler) {
      this.handler = handler;
    }

    @Override
    public void accept(List<String> row) {
      try {
        handler.accept(row);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to write a record of the dump", e);
      }
      rows++;
    }
  }
}
