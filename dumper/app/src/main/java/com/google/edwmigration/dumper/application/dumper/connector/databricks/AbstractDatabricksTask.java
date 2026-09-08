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

import com.databricks.sdk.core.DatabricksError;
import com.databricks.sdk.service.catalog.CatalogInfo;
import com.databricks.sdk.service.catalog.ListCatalogsRequest;
import com.databricks.sdk.service.catalog.SchemaInfo;
import com.google.common.base.Preconditions;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base task for extracting Databricks Unity Catalog metadata. */
abstract class AbstractDatabricksTask extends AbstractTask<Void> {

  private static final Logger logger = LoggerFactory.getLogger(AbstractDatabricksTask.class);

  static final String HIVE_METASTORE = "hive_metastore";
  static final String SAMPLES = "samples";
  static final String SYSTEM = "system";

  static final int MAX_RATE_LIMIT_RETRIES = 3;
  static final long DEFAULT_INITIAL_RETRY_BACKOFF_MS = 2000L;
  static final String RETRY_BACKOFF_PROPERTY = "databricks.retry.backoff.ms";

  static long getInitialRetryBackoffMs() {
    String prop = System.getProperty(RETRY_BACKOFF_PROPERTY);
    if (prop != null) {
      try {
        long val = Long.parseLong(prop.trim());
        if (val >= 0) {
          return val;
        }
      } catch (NumberFormatException ignored) {
        // Fall back to default
      }
    }
    return DEFAULT_INITIAL_RETRY_BACKOFF_MS;
  }

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

  protected static boolean isRateLimited(@Nullable Throwable t) {
    Throwable current = t;
    while (current != null) {
      if (current instanceof DatabricksError) {
        DatabricksError de = (DatabricksError) current;
        if (de.getStatusCode() == 429 || "TOO_MANY_REQUESTS".equalsIgnoreCase(de.getErrorCode())) {
          return true;
        }
      }
      String msg = current.getMessage();
      if (msg != null
          && (msg.contains("429")
              || msg.contains("TOO_MANY_REQUESTS")
              || msg.contains("Current request has to be retried")
              || msg.contains("Rate limit")
              || msg.contains("rate limit"))) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  protected List<String> fetchMatchingCatalogs(DatabricksHandle handle) {
    List<String> catalogs = new ArrayList<>();
    long backoffMs = getInitialRetryBackoffMs();
    for (int attempt = 1; attempt <= MAX_RATE_LIMIT_RETRIES; attempt++) {
      try {
        handle.acquirePermit();
        for (CatalogInfo catalogInfo :
            handle.getClient().catalogs().list(new ListCatalogsRequest())) {
          String name = catalogInfo.getName();
          if (name != null
              && catalogPredicate.test(name)
              && !name.equalsIgnoreCase(HIVE_METASTORE)
              && !name.equalsIgnoreCase(SAMPLES)
              && !name.equalsIgnoreCase(SYSTEM)) {
            catalogs.add(name);
          }
        }
        return catalogs;
      } catch (Exception e) {
        if (isRateLimited(e) && attempt < MAX_RATE_LIMIT_RETRIES) {
          logger.warn(
              "Rate limited while listing catalogs. Retrying in {}ms (attempt {}/{})",
              backoffMs,
              attempt,
              MAX_RATE_LIMIT_RETRIES);
          try {
            Thread.sleep(backoffMs);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during rate limit backoff", ie);
          }
          backoffMs *= 2;
          catalogs.clear();
          continue;
        }
        logger.warn("Failed to list catalogs from Unity Catalog: {}", e.getMessage());
        break;
      }
    }
    return catalogs;
  }

  protected List<String> fetchMatchingSchemas(DatabricksHandle handle, String catalogName) {
    List<String> schemas = new ArrayList<>();
    long backoffMs = getInitialRetryBackoffMs();
    for (int attempt = 1; attempt <= MAX_RATE_LIMIT_RETRIES; attempt++) {
      try {
        handle.acquirePermit();
        for (SchemaInfo schemaInfo : handle.getClient().schemas().list(catalogName)) {
          String name = schemaInfo.getName();
          if (name != null && schemaPredicate.test(name)) {
            schemas.add(name);
          }
        }
        return schemas;
      } catch (Exception e) {
        if (isRateLimited(e) && attempt < MAX_RATE_LIMIT_RETRIES) {
          logger.warn(
              "Rate limited while listing schemas for catalog '{}'. Retrying in {}ms (attempt {}/{})",
              catalogName,
              backoffMs,
              attempt,
              MAX_RATE_LIMIT_RETRIES);
          try {
            Thread.sleep(backoffMs);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during rate limit backoff", ie);
          }
          backoffMs *= 2;
          schemas.clear();
          continue;
        }
        logger.warn("Failed to list schemas for catalog '{}': {}", catalogName, e.getMessage());
        break;
      }
    }
    return schemas;
  }
}
