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

import java.io.IOException;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.io.function.IOSupplier;

/**
 * Loads a value at most once, for the tasks of a connector that share a listing. A failed load is
 * remembered and reported to later callers instead of being retried, so the other tasks fail fast
 * instead of repeating a scan that already failed.
 */
@ThreadSafe
final class Memoized<T> {

  private final String description;

  @GuardedBy("this")
  @CheckForNull
  private T value;

  @GuardedBy("this")
  @CheckForNull
  private Exception failure;

  /**
   * @param description describes the load for error messages, e.g. {@code Listing hive_metastore
   *     schemas}
   */
  Memoized(@Nonnull String description) {
    this.description = description;
  }

  @Nonnull
  synchronized T load(@Nonnull IOSupplier<T> loader) throws IOException {
    if (value != null) {
      return value;
    }

    if (failure != null) {
      throw new IOException(
          description + " failed earlier and is not retried: " + failure.getMessage(), failure);
    }

    try {
      value = loader.get();
      return value;
    } catch (IOException | RuntimeException e) {
      failure = e;
      throw e;
    }
  }
}
