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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
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

  /**
   * Reads a JSON object, keeping the digits of its decimal numbers. Text that can't be written back
   * without a loss, such as an object with a duplicate key or with trailing text, is rejected.
   */
  private static final ObjectReader OBJECT_READER =
      new ObjectMapper()
          .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
          .configure(DeserializationFeature.FAIL_ON_TRAILING_TOKENS, true)
          .configure(JsonParser.Feature.STRICT_DUPLICATE_DETECTION, true)
          .readerFor(new TypeReference<Map<String, Object>>() {});

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

  /**
   * Returns the JSON text of an object with its keys sorted, such as the properties that Databricks
   * returns as text, or the text unchanged if it isn't a JSON object. Returns an empty string if
   * the text is null, blank or an empty object.
   */
  @Nonnull
  static String sortJsonObjectKeys(@CheckForNull String text) {
    if (text == null || text.trim().isEmpty()) {
      return "";
    }

    Map<String, Object> values;
    try {
      values = OBJECT_READER.readValue(text);
    } catch (IOException e) {
      // The text is kept as Databricks returned it, so no information is lost.
      return text;
    }
    return toJsonOrEmpty(values);
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
