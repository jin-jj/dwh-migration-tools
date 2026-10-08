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
import com.google.edwmigration.dumper.application.dumper.handle.AbstractHandle;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread-safe shared {@code Handle} for the Databricks connector.
 *
 * <p>Holds the {@link DatabricksSqlHelper} bound to the configured SQL Warehouse, the {@link
 * DatabricksRestHelper} that reads Unity Catalog metadata, the catalog and schema {@link
 * DatabricksFilter}, and the {@link ObjectStatementRunner} that bounds the number of statements and
 * requests that run concurrently. It also deletes the temporary files of the tasks when it is
 * closed.
 */
@ThreadSafe
final class DatabricksHandle extends AbstractHandle {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksHandle.class);

  private final DatabricksSqlHelper sqlHelper;
  private final DatabricksRestHelper restHelper;
  private final DatabricksFilter filter;
  private final ObjectStatementRunner statementRunner;

  @GuardedBy("temporaryFiles")
  private final List<Path> temporaryFiles = new ArrayList<>();

  DatabricksHandle(
      @Nonnull DatabricksSqlHelper sqlHelper,
      @Nonnull DatabricksRestHelper restHelper,
      @Nonnull DatabricksFilter filter,
      int maxConcurrentStatements) {
    this.sqlHelper = Preconditions.checkNotNull(sqlHelper, "DatabricksSqlHelper was null.");
    this.restHelper = Preconditions.checkNotNull(restHelper, "DatabricksRestHelper was null.");
    this.filter = Preconditions.checkNotNull(filter, "DatabricksFilter was null.");
    this.statementRunner = new ObjectStatementRunner(maxConcurrentStatements);
  }

  @Nonnull
  DatabricksSqlHelper sqlHelper() {
    return sqlHelper;
  }

  @Nonnull
  DatabricksRestHelper restHelper() {
    return restHelper;
  }

  @Nonnull
  DatabricksFilter filter() {
    return filter;
  }

  @Nonnull
  ObjectStatementRunner statementRunner() {
    return statementRunner;
  }

  /**
   * Creates an empty temporary file, such as a listing that is too large for memory, that is
   * deleted when this handle is closed.
   */
  @Nonnull
  Path createTemporaryFile(@Nonnull String prefix, @Nonnull String suffix) throws IOException {
    Path file = Files.createTempFile(prefix, suffix);
    synchronized (temporaryFiles) {
      temporaryFiles.add(file);
    }
    return file;
  }

  @Override
  public void close() {
    statementRunner.close();
    synchronized (temporaryFiles) {
      for (Path file : temporaryFiles) {
        try {
          Files.deleteIfExists(file);
        } catch (IOException e) {
          logger.warn("Failed to delete the temporary file {}: {}", file, e.getMessage());
        }
      }
      temporaryFiles.clear();
    }
  }
}
