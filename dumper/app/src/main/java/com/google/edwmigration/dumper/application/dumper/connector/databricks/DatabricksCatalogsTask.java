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

import com.databricks.sdk.service.catalog.CatalogInfo;
import com.databricks.sdk.service.catalog.ListCatalogsRequest;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.RecordProgressMonitor;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.CatalogsFormat;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dumps catalog definitions from Databricks Unity Catalog. */
class DatabricksCatalogsTask extends AbstractDatabricksTask implements CatalogsFormat {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksCatalogsTask.class);

  DatabricksCatalogsTask(@Nonnull Predicate<String> catalogPredicate) {
    super(ZIP_ENTRY_NAME, catalogPredicate);
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    DatabricksHandle databricksHandle = (DatabricksHandle) handle;
    logger.info("Writing catalogs to '{}'", getTargetPath());
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
        CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer);
        RecordProgressMonitor monitor =
            new RecordProgressMonitor("Writing catalogs to " + getTargetPath())) {
      for (CatalogInfo catalogInfo :
          databricksHandle.getClient().catalogs().list(new ListCatalogsRequest())) {
        String name = catalogInfo.getName();
        if (name != null && catalogPredicate.test(name)) {
          monitor.count();
          printer.printRecord(
              catalogInfo.getName(),
              catalogInfo.getComment(),
              catalogInfo.getOwner(),
              catalogInfo.getCreatedAt(),
              catalogInfo.getUpdatedAt());
        }
      }
    }
    return null;
  }
}
