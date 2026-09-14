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
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base class for Databricks system table SQL extraction tasks. */
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
}
