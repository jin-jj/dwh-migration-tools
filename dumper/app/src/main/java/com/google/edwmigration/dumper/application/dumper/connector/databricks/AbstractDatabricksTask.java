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
import com.databricks.sdk.service.catalog.SchemaInfo;
import com.google.common.base.Preconditions;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base task for extracting Databricks Unity Catalog metadata. */
abstract class AbstractDatabricksTask extends AbstractTask<Void> {

  private static final Logger logger = LoggerFactory.getLogger(AbstractDatabricksTask.class);

  protected static final String HIVE_METASTORE = "hive_metastore";

  protected final Predicate<String> catalogPredicate;
  protected final Predicate<String> schemaPredicate;

  AbstractDatabricksTask(
      @Nonnull String targetPath,
      @Nonnull Predicate<String> catalogPredicate,
      @Nonnull Predicate<String> schemaPredicate) {
    super(targetPath);
    this.catalogPredicate =
        Preconditions.checkNotNull(catalogPredicate, "Catalog predicate was null.");
    this.schemaPredicate =
        Preconditions.checkNotNull(schemaPredicate, "Schema predicate was null.");
  }

  AbstractDatabricksTask(@Nonnull String targetPath, @Nonnull Predicate<String> catalogPredicate) {
    this(targetPath, catalogPredicate, schema -> true);
  }

  protected List<String> getMatchingCatalogs(DatabricksHandle handle) {
    List<String> catalogs = new ArrayList<>();
    try {
      for (CatalogInfo catalogInfo :
          handle.getClient().catalogs().list(new ListCatalogsRequest())) {
        String name = catalogInfo.getName();
        if (name != null && catalogPredicate.test(name) && !name.equalsIgnoreCase(HIVE_METASTORE)) {
          catalogs.add(name);
        }
      }
    } catch (Exception e) {
      logger.warn("Failed to list catalogs from Unity Catalog: {}", e.getMessage());
    }
    return catalogs;
  }

  protected List<String> getMatchingSchemas(DatabricksHandle handle, String catalogName) {
    List<String> schemas = new ArrayList<>();
    try {
      for (SchemaInfo schemaInfo : handle.getClient().schemas().list(catalogName)) {
        String name = schemaInfo.getName();
        if (name != null && schemaPredicate.test(name)) {
          schemas.add(name);
        }
      }
    } catch (Exception e) {
      logger.warn("Failed to list schemas for catalog '{}': {}", catalogName, e.getMessage());
    }
    return schemas;
  }
}
