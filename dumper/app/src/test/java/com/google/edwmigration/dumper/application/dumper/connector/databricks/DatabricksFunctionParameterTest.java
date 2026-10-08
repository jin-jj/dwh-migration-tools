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
import java.util.LinkedHashMap;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksFunctionParameterTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  @Test
  public void toSqlDefinition_quotesNameAndEscapesComment() {
    DatabricksFunctionParameter parameter =
        DatabricksFunctionParameter.newBuilder("p `q`", "INT")
            .setDefaultExpression("1")
            .setComment("it's")
            .build();

    assertEquals("`p ``q``` INT DEFAULT 1 COMMENT 'it\\'s'", parameter.toSqlDefinition());
  }

  @Test
  public void toSqlDefinition_omitsAbsentDefaultAndComment() {
    DatabricksFunctionParameter parameter =
        DatabricksFunctionParameter.newBuilder("x", "MAP<STRING, INT>")
            .setMode("IN")
            .setDefaultExpression(null)
            .setComment(null)
            .build();

    assertEquals("`x` MAP<STRING, INT>", parameter.toSqlDefinition());
  }

  @Test
  public void toSqlDefinition_returnsTypeOfParameterWithoutName() {
    String unsplittableText = "x INT 'open comment";

    DatabricksFunctionParameter parameter =
        DatabricksFunctionParameter.newBuilder("", unsplittableText).build();

    assertEquals(unsplittableText, parameter.toSqlDefinition());
  }

  @Test
  public void json_writesPartsInOrderAndOmitsAbsentParts() throws Exception {
    ImmutableMap<String, String> fullParts =
        ImmutableMap.of(
            "name", "x", "type", "INT", "mode", "IN", "default", "1", "comment", "the input");
    DatabricksFunctionParameter full =
        DatabricksFunctionParameter.newBuilder(fullParts.get("name"), fullParts.get("type"))
            .setMode(fullParts.get("mode"))
            .setDefaultExpression(fullParts.get("default"))
            .setComment(fullParts.get("comment"))
            .build();
    DatabricksFunctionParameter minimal =
        DatabricksFunctionParameter.newBuilder("y", "STRING").setComment(null).build();

    List<LinkedHashMap<String, String>> written =
        JSON.readValue(
            toJsonOrEmpty(ImmutableList.of(full, minimal)),
            new TypeReference<List<LinkedHashMap<String, String>>>() {});

    assertEquals(
        ImmutableList.of(fullParts, ImmutableMap.of("name", "y", "type", "STRING")), written);
    assertEquals(
        ImmutableList.copyOf(fullParts.keySet()), ImmutableList.copyOf(written.get(0).keySet()));
  }
}
