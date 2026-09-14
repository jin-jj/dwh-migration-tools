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

import com.databricks.sdk.service.catalog.FunctionInfo;
import com.databricks.sdk.service.catalog.ListFunctionsRequest;
import com.databricks.sdk.service.catalog.ListFunctionsResponse;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestHelper.Page;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import com.google.edwmigration.dumper.plugin.ext.jdk.progress.RecordProgressMonitor;
import com.google.edwmigration.dumper.plugin.lib.dumper.spi.DatabricksMetadataDumpFormat.FunctionsFormat;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dumps function and UDF definitions from the Unity Catalog REST API. */
class DatabricksRestFunctionsTask extends AbstractDatabricksRestTask implements FunctionsFormat {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksRestFunctionsTask.class);

  DatabricksRestFunctionsTask(
      @Nonnull Predicate<String> catalogPredicate, @Nonnull Predicate<String> schemaPredicate) {
    super(ZIP_ENTRY_NAME, catalogPredicate, schemaPredicate);
  }

  @Override
  protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
      throws Exception {
    DatabricksHandle databricksHandle = (DatabricksHandle) handle;
    logger.info("Writing functions from the REST API to '{}'", getTargetPath());
    try (Writer writer = sink.asCharSink(StandardCharsets.UTF_8).openBufferedStream();
        CSVPrinter printer = FORMAT.withHeader(Header.class).print(writer);
        RecordProgressMonitor monitor =
            new RecordProgressMonitor("Writing functions to " + getTargetPath())) {
      for (String catalogName : fetchMatchingCatalogNames(databricksHandle)) {
        for (String schemaName : fetchMatchingSchemaNames(databricksHandle, catalogName)) {
          DatabricksRestHelper.forEachItem(
              databricksHandle,
              "listing functions of schema '" + catalogName + "." + schemaName + "'",
              pageToken -> {
                ListFunctionsResponse response =
                    databricksHandle
                        .getClient()
                        .functions()
                        .impl()
                        .list(
                            new ListFunctionsRequest()
                                .setCatalogName(catalogName)
                                .setSchemaName(schemaName)
                                .setMaxResults(DatabricksRestHelper.PAGE_SIZE)
                                .setPageToken(pageToken));
                return new Page<>(response.getFunctions(), response.getNextPageToken());
              },
              function -> {
                monitor.count();
                printer.printRecord(
                    catalogName,
                    schemaName,
                    function.getName(),
                    dataTypeOf(function),
                    function.getInputParams() == null ? null : function.getInputParams().toString(),
                    function.getRoutineDefinition(),
                    function.getExternalLanguage(),
                    function.getComment(),
                    function.getOwner());
              });
        }
      }
    }
    return null;
  }

  private static String dataTypeOf(@Nonnull FunctionInfo function) {
    String fullDataType = function.getFullDataType();
    if (fullDataType != null) {
      return fullDataType;
    }
    return function.getDataType() == null ? null : function.getDataType().name();
  }
}
