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
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.escapeIdentifier;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.executeBulkQueryInCatalogOrThrow;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.executeQueryOrThrow;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for the tasks that dump the legacy {@code hive_metastore} catalog.
 *
 * <p>{@code hive_metastore} is not a Unity Catalog securable: it does not appear in the REST API,
 * and it has no {@code information_schema}. Everything about it has to come from {@code SHOW}
 * commands against a SQL warehouse, which is why it gets its own tasks and its own output files
 * rather than participating in the three-tier fallback chain.
 */
abstract class AbstractDatabricksHiveMetastoreTask extends AbstractTask<Void> {

  private static final Logger logger =
      LoggerFactory.getLogger(AbstractDatabricksHiveMetastoreTask.class);

  protected final DatabricksFilter filter;

  AbstractDatabricksHiveMetastoreTask(
      @Nonnull String targetPath, @Nonnull DatabricksFilter filter) {
    super(targetPath);
    this.filter = Preconditions.checkNotNull(filter, "Filter was null.");
  }

  @Nonnull
  @Override
  public TaskCategory getCategory() {
    return TaskCategory.OPTIONAL;
  }

  @Override
  public boolean handleException(Exception e) {
    logger.warn(
        "Databricks hive_metastore extraction for '{}' failed: {}",
        getTargetPath(),
        e.getMessage());
    return true;
  }

  /** Receives one parsed table description at a time. */
  interface TableConsumer {
    void accept(@Nonnull String schemaName, @Nonnull DatabricksHiveMetastoreTable table)
        throws IOException;
  }

  /**
   * Walks every matching schema of {@code hive_metastore} and hands over each of its tables.
   *
   * <p>One {@code SHOW TABLE EXTENDED} covers a whole schema, so the cost is one query per schema
   * rather than the two-per-table that {@code SHOW TABLES} plus {@code DESCRIBE TABLE} would need.
   * Each row carries a description of a kilobyte or more, so the result is streamed through the
   * external-links transport: a large schema would exceed the 25 MiB cap of the inline one, which
   * aborts the statement outright.
   *
   * <p>A schema that cannot be read is logged and skipped, because a single unreadable or corrupt
   * schema should not cost the caller the whole metastore. If every schema fails, the failure is
   * raised so that the task is reported as failed rather than silently producing an empty file.
   */
  protected void forEachTable(@Nonnull DatabricksHandle handle, @Nonnull TableConsumer consumer)
      throws IOException, SQLException {
    ImmutableList<String> schemaNames = fetchMatchingSchemaNames(handle);
    int failures = 0;
    SQLException lastFailure = null;
    for (String schemaName : schemaNames) {
      // Referencing the schema as hive_metastore.<schema> is rejected by this command with
      // CROSS_CATALOG_SCHEMA_REFERENCE_NOT_SUPPORTED, so the catalog travels on the request and the
      // schema is named on its own.
      String sql = "SHOW TABLE EXTENDED IN " + escapeIdentifier(schemaName) + " LIKE '*'";
      try {
        executeBulkQueryInCatalogOrThrow(
            handle, HIVE_METASTORE, sql, row -> describe(schemaName, row, consumer));
      } catch (SQLException e) {
        failures++;
        lastFailure = e;
        logger.warn(
            "Failed to describe the tables of hive_metastore.{}: {}", schemaName, e.getMessage());
      } catch (UncheckedIOException e) {
        throw e.getCause();
      }
    }
    if (lastFailure != null && failures == schemaNames.size()) {
      throw lastFailure;
    }
  }

  /**
   * Parses one {@code SHOW TABLE EXTENDED} row and hands it to {@code consumer}.
   *
   * <p>The consumer contract of the SQL helper cannot declare {@code IOException}, so a write
   * failure travels as an {@link UncheckedIOException} and is unwrapped by the caller.
   */
  private static void describe(String schemaName, List<String> row, TableConsumer consumer) {
    // SHOW TABLE EXTENDED returns (namespace, tableName, isTemporary, information).
    if (row.size() < 4 || row.get(3) == null) {
      return;
    }
    try {
      consumer.accept(schemaName, DatabricksHiveMetastoreTable.parse(row.get(3)));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write a record of the dump", e);
    }
  }

  /** Returns the names of the {@code hive_metastore} schemas that match the schema filter. */
  @Nonnull
  protected ImmutableList<String> fetchMatchingSchemaNames(@Nonnull DatabricksHandle handle)
      throws SQLException {
    List<String> names = new ArrayList<>();
    for (List<String> row : executeQueryOrThrow(handle, "SHOW SCHEMAS IN " + HIVE_METASTORE)) {
      if (row.isEmpty()) {
        continue;
      }
      String name = row.get(0);
      if (name != null && filter.matchesSchema(name)) {
        names.add(name);
      }
    }
    return ImmutableList.copyOf(names);
  }
}
