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
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractDatabricksSqlTask extends AbstractTask<Void> {

  private static final Logger logger = LoggerFactory.getLogger(AbstractDatabricksSqlTask.class);

  public static final String SAMPLES = "samples";
  public static final String SYSTEM = "system";
  public static final String HIVE_METASTORE = "hive_metastore";

  protected static final CSVFormat FORMAT = CSVFormat.DEFAULT;

  protected final Predicate<String> catalogPredicate;
  protected final Predicate<String> schemaPredicate;

  AbstractDatabricksSqlTask(
      @Nonnull String targetPath,
      @Nonnull Predicate<String> catalogPredicate,
      @Nonnull Predicate<String> schemaPredicate) {
    super(targetPath);
    this.catalogPredicate =
        Preconditions.checkNotNull(catalogPredicate, "Catalog predicate cannot be null.");
    this.schemaPredicate =
        Preconditions.checkNotNull(schemaPredicate, "Schema predicate cannot be null.");
  }

  AbstractDatabricksSqlTask(
      @Nonnull String targetPath, @Nonnull Predicate<String> catalogPredicate) {
    this(targetPath, catalogPredicate, s -> true);
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
            && catalogPredicate.test(cat)
            && !cat.equalsIgnoreCase(HIVE_METASTORE)
            && !handle.isCatalogInaccessible(cat)) {
          result.add(cat);
        }
      }
    }
    return ImmutableList.copyOf(result);
  }
}
