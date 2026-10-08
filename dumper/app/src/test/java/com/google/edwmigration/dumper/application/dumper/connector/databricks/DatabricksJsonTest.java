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

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksJson.toJsonOrEmpty;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksJsonTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  @Test
  public void toJsonOrEmpty_writesMapWithSortedKeys() throws IOException {
    ImmutableMap<String, String> values =
        ImmutableMap.of("zone", "a \"quoted\"\nvalue", "b.key", "2", "a.key", "");

    LinkedHashMap<String, String> written =
        JSON.readValue(
            toJsonOrEmpty(values), new TypeReference<LinkedHashMap<String, String>>() {});

    assertEquals(values, written);
    assertEquals(
        Ordering.natural().sortedCopy(values.keySet()), ImmutableList.copyOf(written.keySet()));
  }

  @Test
  public void toJsonOrEmpty_writesCollectionAsArray() throws IOException {
    ImmutableList<String> values = ImmutableList.of("b", "a`b", "c, d");

    List<String> written =
        JSON.readValue(toJsonOrEmpty(values), new TypeReference<List<String>>() {});

    assertEquals(values, written);
  }

  @Test
  public void toJsonOrEmpty_writesEmptyStringForNullOrEmptyValues() {
    assertEquals("", toJsonOrEmpty((Map<String, ?>) null));
    assertEquals("", toJsonOrEmpty(ImmutableMap.of()));
    assertEquals("", toJsonOrEmpty((Collection<?>) null));
    assertEquals("", toJsonOrEmpty(ImmutableList.of()));
  }
}
