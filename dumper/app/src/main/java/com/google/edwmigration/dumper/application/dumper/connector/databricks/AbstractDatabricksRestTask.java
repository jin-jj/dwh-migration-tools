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
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestHelper.PAGE_SIZE;

import com.databricks.sdk.service.catalog.CatalogInfo;
import com.databricks.sdk.service.catalog.ListCatalogsRequest;
import com.databricks.sdk.service.catalog.ListCatalogsResponse;
import com.databricks.sdk.service.catalog.ListSchemasRequest;
import com.databricks.sdk.service.catalog.ListSchemasResponse;
import com.databricks.sdk.service.catalog.ListTablesRequest;
import com.databricks.sdk.service.catalog.ListTablesResponse;
import com.databricks.sdk.service.catalog.SchemaInfo;
import com.databricks.sdk.service.catalog.TableInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksRestHelper.Page;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for the Unity Catalog REST extraction tier.
 *
 * <p>This tier is the last resort: it runs only when both the {@code system.information_schema} and
 * the per-catalog {@code information_schema} tiers have failed, typically because no SQL warehouse
 * is reachable or the caller lacks {@code SELECT} on the metastore's information schema. It is
 * markedly slower and less complete than SQL, so it is never preferred.
 */
abstract class AbstractDatabricksRestTask extends AbstractTask<Void> {

  private static final Logger logger = LoggerFactory.getLogger(AbstractDatabricksRestTask.class);

  protected static final CSVFormat FORMAT = CSVFormat.DEFAULT;

  protected final Predicate<String> catalogPredicate;
  protected final Predicate<String> schemaPredicate;

  AbstractDatabricksRestTask(
      @Nonnull String targetPath,
      @Nonnull Predicate<String> catalogPredicate,
      @Nonnull Predicate<String> schemaPredicate) {
    super(targetPath);
    this.catalogPredicate =
        Preconditions.checkNotNull(catalogPredicate, "Catalog predicate was null.");
    this.schemaPredicate =
        Preconditions.checkNotNull(schemaPredicate, "Schema predicate was null.");
  }

  AbstractDatabricksRestTask(
      @Nonnull String targetPath, @Nonnull Predicate<String> catalogPredicate) {
    this(targetPath, catalogPredicate, schema -> true);
  }

  @Nonnull
  @Override
  public TaskCategory getCategory() {
    return TaskCategory.OPTIONAL;
  }

  @Override
  public boolean handleException(Exception e) {
    logger.warn(
        "Databricks REST extraction for '{}' failed: {}. No further fallback is available, so this"
            + " output will be missing from the dump.",
        getTargetPath(),
        e.getMessage());
    return true;
  }

  /** Streams every Unity Catalog catalog, unfiltered, so callers can read the full metadata. */
  protected void forEachCatalog(
      @Nonnull DatabricksHandle handle,
      @Nonnull DatabricksRestHelper.ItemConsumer<CatalogInfo> consumer)
      throws IOException {
    DatabricksRestHelper.forEachItem(
        handle,
        "listing catalogs",
        pageToken -> {
          ListCatalogsResponse response =
              handle
                  .getClient()
                  .catalogs()
                  .impl()
                  .list(new ListCatalogsRequest().setMaxResults(PAGE_SIZE).setPageToken(pageToken));
          return new Page<>(response.getCatalogs(), response.getNextPageToken());
        },
        consumer);
  }

  /** Streams every schema of {@code catalogName}, unfiltered. */
  protected void forEachSchema(
      @Nonnull DatabricksHandle handle,
      @Nonnull String catalogName,
      @Nonnull DatabricksRestHelper.ItemConsumer<SchemaInfo> consumer)
      throws IOException {
    DatabricksRestHelper.forEachItem(
        handle,
        "listing schemas of catalog '" + catalogName + "'",
        pageToken -> {
          ListSchemasResponse response =
              handle
                  .getClient()
                  .schemas()
                  .impl()
                  .list(
                      new ListSchemasRequest()
                          .setCatalogName(catalogName)
                          .setMaxResults(PAGE_SIZE)
                          .setPageToken(pageToken));
          return new Page<>(response.getSchemas(), response.getNextPageToken());
        },
        consumer);
  }

  /**
   * Streams every table of {@code catalogName.schemaName}, unfiltered.
   *
   * <p>{@code /tables/list} returns the full column array inline, so callers that need columns do
   * not have to issue a per-table get. Callers that do not need columns should pass {@code false}
   * for {@code includeColumns} to keep the responses small.
   */
  protected void forEachTable(
      @Nonnull DatabricksHandle handle,
      @Nonnull String catalogName,
      @Nonnull String schemaName,
      boolean includeColumns,
      @Nonnull DatabricksRestHelper.ItemConsumer<TableInfo> consumer)
      throws IOException {
    DatabricksRestHelper.forEachItem(
        handle,
        "listing tables of schema '" + catalogName + "." + schemaName + "'",
        pageToken -> {
          ListTablesResponse response =
              handle
                  .getClient()
                  .tables()
                  .impl()
                  .list(
                      new ListTablesRequest()
                          .setCatalogName(catalogName)
                          .setSchemaName(schemaName)
                          .setOmitColumns(!includeColumns)
                          .setOmitProperties(true)
                          .setMaxResults(PAGE_SIZE)
                          .setPageToken(pageToken));
          return new Page<>(response.getTables(), response.getNextPageToken());
        },
        consumer);
  }

  /**
   * Returns the names of the catalogs to walk.
   *
   * <p>{@code hive_metastore} is not a Unity Catalog securable and is absent from this listing
   * entirely; it is dumped by the dedicated Hive Metastore tasks, which require a SQL warehouse.
   */
  @Nonnull
  protected ImmutableList<String> fetchMatchingCatalogNames(@Nonnull DatabricksHandle handle)
      throws IOException {
    List<String> names = new ArrayList<>();
    forEachCatalog(
        handle,
        catalog -> {
          String name = catalog.getName();
          if (name != null
              && catalogPredicate.test(name)
              && !HIVE_METASTORE.equalsIgnoreCase(name)) {
            names.add(name);
          }
        });
    return ImmutableList.copyOf(names);
  }

  /** Returns the names of the schemas of {@code catalogName} that match the schema filter. */
  @Nonnull
  protected ImmutableList<String> fetchMatchingSchemaNames(
      @Nonnull DatabricksHandle handle, @Nonnull String catalogName) throws IOException {
    List<String> names = new ArrayList<>();
    forEachSchema(
        handle,
        catalogName,
        schema -> {
          String name = schema.getName();
          if (name != null && schemaPredicate.test(name)) {
            names.add(name);
          }
        });
    return ImmutableList.copyOf(names);
  }
}
