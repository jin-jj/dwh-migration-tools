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

import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.CatalogsFormat;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Records the legacy {@code hive_metastore} catalog itself.
 *
 * <p>Without this the catalog is invisible to anything reading the dump: it is absent from {@code
 * information_schema.catalogs} and from the Unity Catalog REST API, so none of the three tiers that
 * write {@code catalogs.csv} can see it, even though its schemas and tables are dumped.
 */
class DatabricksHiveMetastoreCatalogsTask extends AbstractDatabricksHiveMetastoreTask
    implements CatalogsFormat {

  private static final Logger logger =
      LoggerFactory.getLogger(DatabricksHiveMetastoreCatalogsTask.class);

  DatabricksHiveMetastoreCatalogsTask() {
    super(HMS_ZIP_ENTRY_NAME, DatabricksFilter.all());
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    logger.info("Writing the hive_metastore catalog to '{}'", getTargetPath());
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
        CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer)) {
      printer.printRecord(
          HIVE_METASTORE,
          "Legacy Databricks Hive metastore",
          /* owner= */ null,
          /* createdAt= */ null,
          /* updatedAt= */ null);
    }
    return null;
  }
}
