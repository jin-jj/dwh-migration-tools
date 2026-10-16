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
package com.google.edwmigration.dumper.application.dumper.connector.databricks.metadata;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.databricks.sdk.core.DatabricksException;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksRestHelper;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.output.DatabricksMetadataDumpFormat.DatabricksFilter;
import com.google.edwmigration.dumper.application.dumper.handle.AbstractHandle;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.Task;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.function.Function;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread-safe shared {@code Handle} and CSV {@link MetadataTask} implementation for the Databricks
 * metadata connector.
 */
@ThreadSafe
public final class DatabricksHandle extends AbstractHandle {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksHandle.class);

  /** Runs one statement or API call for an object {@code I} and returns {@code O}. */
  @FunctionalInterface
  public interface Statement<I, O> {
    O execute(I input) throws IOException;
  }

  /** Consumes the result {@code O} of one object {@code I}. */
  @FunctionalInterface
  public interface ResultConsumer<I, O> {
    void accept(I input, O result) throws IOException;
  }

  /** A metadata task that writes rows for a header enum {@code H} to a CSV zip entry. */
  public static final class MetadataTask<H extends Enum<H>> extends AbstractTask<Void> {

    public static final String CHECK_TASK_TARGET_PATH = "databricks-metadata-check.txt";

    /** Writes the rows of one metadata CSV file, in the column order of its header. */
    @FunctionalInterface
    public interface RowSource {
      void writeRows(DatabricksHandle handle, CSVPrinter printer) throws IOException;
    }

    private final Class<H> header;
    private final RowSource rowSource;

    MetadataTask(String targetPath, Class<H> header, RowSource rowSource) {
      super(targetPath);
      this.header = header;
      this.rowSource = rowSource;
    }

    public static Task<?> createAllFailedCheckTask(List<Task<?>> networkTasks) {
      return new AbstractTask<Void>(
          CHECK_TASK_TARGET_PATH,
          TaskOptions.DEFAULT.withTargetInitialization(TargetInitialization.DO_NOT_CREATE)) {
        @Override
        protected String describeSourceData() {
          return "Check whether all Databricks metadata tasks failed";
        }

        @Override
        protected Void doRun(TaskRunContext context, ByteSink sink, Handle handle)
            throws IOException {
          throw new IOException(
              "All Databricks metadata tasks failed. Check the task exceptions above for"
                  + " details.");
        }
      }.onlyIfAllFailed(networkTasks.toArray(new Task<?>[0]));
    }

    @Override
    public TaskCategory getCategory() {
      return TaskCategory.OPTIONAL;
    }

    @Override
    protected String describeSourceData() {
      return "from Databricks";
    }

    @Override
    protected Void doRun(TaskRunContext context, ByteSink sink, Handle handle) throws Exception {
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter printer = FORMAT.withHeader(this.header).print(writer)) {
        this.rowSource.writeRows((DatabricksHandle) handle, printer);
      }
      return null;
    }
  }

  private final DatabricksSqlHelper sqlHelper;
  private final DatabricksRestHelper restHelper;
  private final DatabricksFilter filter;

  /**
   * Creates the handle of a run. {@code sqlHelper} is null when no SQL warehouse was given; then
   * only the Unity Catalog tasks run, because they read the REST API only.
   */
  public DatabricksHandle(
      DatabricksSqlHelper sqlHelper, DatabricksRestHelper restHelper, DatabricksFilter filter) {
    this.sqlHelper = sqlHelper;
    this.restHelper = Preconditions.checkNotNull(restHelper, "DatabricksRestHelper was null.");
    this.filter = Preconditions.checkNotNull(filter, "DatabricksFilter was null.");
  }

  /**
   * Returns the SQL helper of the hive_metastore tasks, or null when no SQL warehouse was given.
   */
  public DatabricksSqlHelper sqlHelper() {
    return this.sqlHelper;
  }

  public DatabricksRestHelper restHelper() {
    return this.restHelper;
  }

  public DatabricksFilter filter() {
    return this.filter;
  }

  /**
   * Executes {@code statement} sequentially for each element of {@code inputs} and forwards each
   * successful result to {@code consumer} in input order. Logs and skips per-object {@link
   * IOException} and {@link DatabricksException} failures, and throws an {@link IOException} if
   * {@code statement} fails for every object in a non-empty {@code inputs}.
   */
  public <I, O> void runForEach(
      String statementName,
      Iterable<I> inputs,
      Function<I, String> describeInput,
      Statement<I, O> statement,
      ResultConsumer<I, O> consumer)
      throws IOException {
    int successCount = 0;
    int failureCount = 0;
    Exception firstFailure = null;
    for (I input : inputs) {
      O result;
      try {
        result = statement.execute(input);
      } catch (IOException | DatabricksException e) {
        failureCount++;
        if (firstFailure == null) {
          firstFailure = e;
        }
        logger.warn(
            "Skipping {} for {}: {}", statementName, describeInput.apply(input), e.getMessage());
        continue;
      }
      consumer.accept(input, result);
      successCount++;
    }
    if (failureCount > 0) {
      if (successCount == 0) {
        throw new IOException(
            String.format(
                "%s failed for all %d objects. First failure: %s",
                statementName, failureCount, firstFailure.getMessage()),
            firstFailure);
      }
      logger.warn(
          "{} failed for {} of {} objects.",
          statementName,
          failureCount,
          failureCount + successCount);
    }
  }
}
