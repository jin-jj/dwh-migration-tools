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

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksCatalogNames.HIVE_METASTORE;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for the per-catalog {@code <catalog>.information_schema} extraction tier.
 *
 * <p>This is the second tier of the fallback chain. It is marked optional and swallows its own
 * failure so that the REST tier behind it gets a chance to run.
 */
abstract class AbstractDatabricksSqlTask extends AbstractTask<Void> {

  private static final Logger logger = LoggerFactory.getLogger(AbstractDatabricksSqlTask.class);

  protected static final CSVFormat FORMAT = CSVFormat.DEFAULT;

  /** Placeholder in a statement template, replaced with the escaped name of one catalog. */
  protected static final String CATALOG = "$catalog";

  /** Placeholder in a statement template, replaced with the generated {@code WHERE} clause. */
  protected static final String WHERE = DatabricksFilter.WHERE;

  protected final DatabricksFilter filter;

  AbstractDatabricksSqlTask(@Nonnull String targetPath, @Nonnull DatabricksFilter filter) {
    super(targetPath);
    this.filter = Preconditions.checkNotNull(filter, "Filter cannot be null.");
  }

  /**
   * Substitutes the {@code WHERE} clause for the requested catalogs and schemas into {@code
   * sqlTemplate}, which must contain {@link DatabricksFilter#WHERE} at the point the clause
   * belongs.
   *
   * <p>Pushing the restriction into the statement is what keeps a narrow dump cheap. Without it the
   * warehouse reads and sorts every row in the metastore before the first one is shipped, because
   * the statements are ordered and an ordered result cannot be streamed until it is complete.
   *
   * @param catalogColumn the column holding the catalog name, or null if the statement is already
   *     scoped to one catalog.
   * @param schemaColumn the column holding the schema name, or null if the statement has none.
   */
  @Nonnull
  protected String withFilter(
      @Nonnull String sqlTemplate,
      @CheckForNull String catalogColumn,
      @CheckForNull String schemaColumn) {
    Preconditions.checkNotNull(sqlTemplate, "SQL template was null.");
    Preconditions.checkArgument(
        sqlTemplate.contains(DatabricksFilter.WHERE),
        "SQL template has no '%s' placeholder: %s",
        DatabricksFilter.WHERE,
        sqlTemplate);
    return sqlTemplate.replace(
        DatabricksFilter.WHERE, filter.whereClause(catalogColumn, schemaColumn));
  }

  @Nonnull
  @Override
  public TaskCategory getCategory() {
    return TaskCategory.OPTIONAL;
  }

  @Override
  public boolean handleException(Exception e) {
    logger.info(
        "Databricks per-catalog query for '{}' failed ({}); falling back to the Unity Catalog REST"
            + " API if that tier is enabled.",
        getTargetPath(),
        e.getMessage());
    return true;
  }

  /**
   * Returns column {@code index} of {@code row}, or {@code null} if the row is shorter than that.
   *
   * <p>A result row can be short if the statement fell back to a variant with fewer columns, so
   * reads are guarded rather than assumed.
   */
  @CheckForNull
  protected static String cell(@Nonnull List<String> row, int index) {
    return index < row.size() ? row.get(index) : null;
  }

  /**
   * Returns the catalogs that match the user-supplied filter and are readable.
   *
   * <p>{@code hive_metastore} is excluded because it is a legacy catalog that does not expose an
   * {@code information_schema}. It is dumped by the dedicated Hive Metastore tasks instead.
   */
  @Nonnull
  protected List<String> fetchMatchingCatalogs(@Nonnull DatabricksHandle handle)
      throws SQLException {
    List<String> result = new ArrayList<>();
    List<List<String>> rows = DatabricksSqlHelper.executeQueryOrThrow(handle, "SHOW CATALOGS");
    for (List<String> row : rows) {
      if (!row.isEmpty()) {
        String cat = row.get(0);
        if (cat != null
            && filter.matchesCatalog(cat)
            && !cat.equalsIgnoreCase(HIVE_METASTORE)
            && !handle.isCatalogInaccessible(cat)) {
          result.add(cat);
        }
      }
    }
    return ImmutableList.copyOf(result);
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
      @CheckForNull String compatibilitySql,
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

  /** Reads one catalog, given its name already escaped for interpolation into a statement. */
  interface CatalogAction {
    void accept(@Nonnull String escapedCatalog) throws SQLException, IOException;
  }

  /**
   * Runs {@code action} once per matching catalog.
   *
   * <p>A catalog the caller cannot read must not cost the other catalogs their output, so a failed
   * catalog is recorded and the walk continues. If every catalog failed the first failure is
   * rethrown: without that, an entirely unreadable metastore would look like an empty one and the
   * tier behind this one would never run.
   *
   * @throws SQLException if the catalogs cannot be listed, or if none of them could be read.
   * @throws IOException if writing the output failed, which is never worth continuing past.
   */
  protected void forEachCatalog(@Nonnull DatabricksHandle handle, @Nonnull CatalogAction action)
      throws SQLException, IOException {
    List<String> catalogs = fetchMatchingCatalogs(handle);
    SQLException failure = null;
    int failed = 0;
    for (String catalogName : catalogs) {
      try {
        action.accept(DatabricksSqlHelper.escapeIdentifier(catalogName));
      } catch (SQLException e) {
        failed++;
        if (failure == null) {
          failure = e;
        } else {
          failure.addSuppressed(e);
        }
        logger.warn(
            "Failed to read catalog '{}' for '{}': {}",
            catalogName,
            getTargetPath(),
            e.getMessage());
      }
    }
    if (failure != null && failed == catalogs.size()) {
      throw failure;
    }
  }

  /** Runs a statement template once per matching catalog, substituting {@link #CATALOG}. */
  protected void executePerCatalog(
      @Nonnull DatabricksHandle handle,
      @Nonnull String sqlTemplate,
      @CheckForNull String compatibilitySqlTemplate,
      @Nonnull RowHandler handler)
      throws SQLException, IOException {
    forEachCatalog(
        handle,
        escapedCatalog ->
            executeWithCompatibilityFallback(
                handle,
                sqlTemplate.replace(CATALOG, escapedCatalog),
                compatibilitySqlTemplate == null
                    ? null
                    : compatibilitySqlTemplate.replace(CATALOG, escapedCatalog),
                handler));
  }

  /**
   * Adapts a {@link RowHandler} to the helper's consumer, counting what it has emitted.
   *
   * <p>The consumer contract cannot declare {@code IOException}, so a write failure travels as an
   * {@link UncheckedIOException}.
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
