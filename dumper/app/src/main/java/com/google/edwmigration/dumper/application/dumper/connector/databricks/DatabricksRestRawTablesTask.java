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

import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.RawTablesFormat;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes the Unity Catalog table listing verbatim, one JSON document per line.
 *
 * <p>The other REST tasks project the listing into the CSV columns the assessment consumes, which
 * discards everything else the API returned. This task copies the listing the tier has already
 * built, so a consumer that wants the full fidelity of the API objects can read it instead of
 * walking the metastore again.
 *
 * <p>It costs nothing beyond the write: the listing is shared through {@link DatabricksHandle}, so
 * this task either reuses the walk another task performed or performs the walk the others then
 * reuse.
 */
class DatabricksRestRawTablesTask extends AbstractDatabricksRestTask implements RawTablesFormat {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksRestRawTablesTask.class);

  DatabricksRestRawTablesTask(@Nonnull DatabricksFilter filter) {
    super(ZIP_ENTRY_NAME, filter);
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    Path listing = tableListing((DatabricksHandle) handle);
    logger.info("Writing the raw table listing to '{}'", getTargetPath());
    try (OutputStream output = sink.openBufferedStream()) {
      Files.copy(listing, output);
    }
    return null;
  }
}
