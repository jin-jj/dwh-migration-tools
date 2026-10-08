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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Map;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * Writes the JSON values of CSV columns, such as table properties.
 *
 * <p>Map entries are written in key order, so that a dump doesn't depend on the order in which
 * Databricks returns them. An absent or empty value is written as an empty string.
 */
final class DatabricksJson {

  private static final ObjectWriter WRITER =
      new ObjectMapper().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true).writer();

  private DatabricksJson() {}

  /** Returns the map as a JSON object, or an empty string if it is null or empty. */
  @Nonnull
  static String toJsonOrEmpty(@CheckForNull Map<String, ?> values) {
    if (values == null || values.isEmpty()) {
      return "";
    }
    return write(values);
  }

  /** Returns the collection as a JSON array, or an empty string if it is null or empty. */
  @Nonnull
  static String toJsonOrEmpty(@CheckForNull Collection<?> values) {
    if (values == null || values.isEmpty()) {
      return "";
    }
    return write(values);
  }

  @Nonnull
  private static String write(@Nonnull Object value) {
    try {
      return WRITER.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException("Failed to write a CSV value as JSON.", e);
    }
  }
}
