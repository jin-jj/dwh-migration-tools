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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.databricks.sdk.service.catalog.FunctionParameterMode;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreFunctionParser.ParsedFunction;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksHiveMetastoreFunctionParserTest {

  @Test
  public void parseDescribeFunctionExtended_parsesSqlFunction() {
    ParsedFunction parsed =
        parseFunction(
            "double_it",
            "Function: hive_metastore.legacy_db.double_it",
            "Input: x INT",
            "Returns: INT",
            "Comment: Doubles input",
            "Owner: alice",
            "Body: x * 2");

    assertEquals("hive_metastore", parsed.functionCatalog);
    assertEquals("legacy_db", parsed.functionSchema);
    assertEquals("double_it", parsed.functionName);
    assertFalse(parsed.isTableFunction);
    assertEquals("INT", parsed.dataType);
    assertEquals(ImmutableList.of("x|INT|null|null"), describe(parsed.inputParams));
    assertTrue(parsed.returnParams.isEmpty());
    assertEquals("x * 2", parsed.routineDefinition);
    assertEquals("SQL", parsed.routineLanguage);
    assertEquals("Doubles input", parsed.comment);
    assertEquals("alice", parsed.owner);
    assertEquals("", parsed.className);
  }

  @Test
  public void parseDescribeFunctionExtended_parsesHiveFunctionClass() {
    String className = "com.acme.udf.CustomHashUdf";

    ParsedFunction parsed =
        parseFunction(
            "custom_hash", "Function: default.custom_hash", "Class: " + className, "Owner: bob");

    assertEquals(className, parsed.className);
    assertEquals("", parsed.routineDefinition);
    assertEquals("", parsed.routineLanguage);
    assertEquals("bob", parsed.owner);
  }

  @Test
  public void parseDescribeFunctionExtended_keepsEmptyValuesWithoutBodyOrClass() {
    ParsedFunction parsed =
        parseFunction("f_unknown", "Function: hive_metastore.legacy_db.f_unknown", "Owner: bob");

    assertEquals("bob", parsed.owner);
    assertEquals("", parsed.dataType);
    assertEquals("", parsed.routineDefinition);
    assertEquals("", parsed.routineLanguage);
    assertEquals("", parsed.className);
    assertEquals("", parsed.isDeterministic);
    assertEquals("", parsed.sqlDataAccess);
    assertEquals("", parsed.createdAt);
    assertTrue(parsed.inputParams.isEmpty());
  }

  @Test
  public void parseDescribeFunctionExtended_keepsMultiLineBody() {
    String body = "CASE\n  WHEN x > 0 THEN 'pos'\n  ELSE 'non-pos'\nEND";

    ParsedFunction parsed =
        parseFunction(
            "sign_label",
            "Function: hive_metastore.legacy_db.sign_label",
            "Type: SCALAR",
            "Input: x INT",
            "Returns: STRING",
            "Body:          " + body);

    assertEquals(body, parsed.routineDefinition);
    assertEquals("SQL", parsed.routineLanguage);
    assertEquals("STRING", parsed.dataType);
  }

  @Test
  public void parseDescribeFunctionExtended_parsesPaddedParametersWithDefaultsAndComments() {
    ParsedFunction parsed =
        parseFunction(
            "f_params",
            "Function:      hive_metastore.legacy_db.f_params",
            "Type:          SCALAR",
            "Input:         c DECIMAL(10,2)             'dec  two'",
            "               a INT                       DEFAULT 5 'has default'",
            "               b STRING                    DEFAULT 'x  y' 'it's a  test'",
            "               d MAP<STRING, INT>          DEFAULT NULL",
            "               e STRUCT<p: INT, q: STRING> DEFAULT NULL 'nested'",
            "Returns:       STRING",
            "Comment:       params test",
            "Deterministic: true",
            "Data Access:   CONTAINS SQL",
            "Configs:       spark.sql.ansi.enabled=true",
            "               spark.sql.session.timeZone=UTC",
            "Owner:         root",
            "Create Time:   Sun Sep 27 21:03:11 UTC 2026",
            "Body:          concat(a, b)");

    assertEquals(
        ImmutableList.of(
            "c|DECIMAL(10,2)|null|dec  two",
            "a|INT|5|has default",
            "b|STRING|'x  y'|it's a  test",
            "d|MAP<STRING, INT>|NULL|null",
            "e|STRUCT<p: INT, q: STRING>|NULL|nested"),
        describe(parsed.inputParams));
    assertEquals("STRING", parsed.dataType);
    assertEquals("params test", parsed.comment);
    assertEquals("true", parsed.isDeterministic);
    assertEquals("CONTAINS SQL", parsed.sqlDataAccess);
    assertEquals("root", parsed.owner);
    assertEquals(
        ZonedDateTime.of(2026, 9, 27, 21, 3, 11, 0, ZoneOffset.UTC).toInstant().toString(),
        parsed.createdAt);
    assertEquals("concat(a, b)", parsed.routineDefinition);
  }

  @Test
  public void parseDescribeFunctionExtended_writesEmptyCreatedTimeInOtherFormat() {
    ParsedFunction parsed =
        parseFunction(
            "f_time",
            "Function: hive_metastore.legacy_db.f_time",
            "Create Time: 2026-09-27 21:03:11",
            "Body: 1");

    assertEquals("", parsed.createdAt);
    assertEquals("1", parsed.routineDefinition);
  }

  @Test
  public void parseDescribeFunctionExtended_separatesDefaultExpressionsFromComments() {
    ParsedFunction parsed =
        parseFunction(
            "f_defaults",
            "Type:          SCALAR",
            "Input:         i INTERVAL DAY TO SECOND     'ivl'",
            "               s STRUCT<a: INT COMMENT 'x'> 'st'",
            "               g STRING                     DEFAULT 'it\\'s' 'user's'",
            "               h STRING                     DEFAULT 'a' || 'b'",
            "               k STRING                     DEFAULT 'a' || 'b' 'c'",
            "               m STRING                     DEFAULT CASE WHEN true THEN 'a' END 'd'",
            "Returns:       STRING",
            "Body:          g");

    assertEquals(
        ImmutableList.of(
            "i|INTERVAL DAY TO SECOND|null|ivl",
            "s|STRUCT<a: INT COMMENT 'x'>|null|st",
            "g|STRING|'it\\'s'|user's",
            "h|STRING|'a' || 'b'|null",
            "k|STRING|'a' || 'b'|c",
            "m|STRING|CASE WHEN true THEN 'a' END|d"),
        describe(parsed.inputParams));
  }

  @Test
  public void parseDescribeFunctionExtended_parsesTableColumnsAndNamesWithSpaces() {
    ParsedFunction parsed =
        parseFunction(
            "f_rows",
            "Type:          TABLE",
            "Input:         p q    INT   ",
            "               select STRING",
            "Returns:       id    INT    'the id'",
            "               label STRING 'it's a label'",
            "Comment:       first line\nsecond line",
            "Deterministic: true",
            "Data Access:   CONTAINS SQL",
            "Body:          SELECT n AS id, 'x' AS label");

    assertTrue(parsed.isTableFunction);
    assertEquals(
        ImmutableList.of("p q|INT|null|null", "select|STRING|null|null"),
        describe(parsed.inputParams));
    assertEquals(
        ImmutableList.of("id|INT|null|the id", "label|STRING|null|it's a label"),
        describe(parsed.returnParams));
    assertEquals(
        "TABLE(`id` INT COMMENT 'the id', `label` STRING COMMENT 'it\\'s a label')",
        parsed.dataType);
    assertEquals("first line\nsecond line", parsed.comment);
  }

  @Test
  public void parseDescribeFunctionExtended_parsesNonDeterminismAndDataAccess() {
    ParsedFunction parsed =
        parseFunction(
            "f_nd",
            "Type:          SCALAR",
            "Input:         x INT    'the input'",
            "               y DOUBLE DEFAULT 1.0",
            "Returns:       DOUBLE",
            "Comment:       it's at C:\\tmp",
            "Deterministic: false",
            "Data Access:   READS  SQL\tDATA",
            "Body:          x * y * rand()");

    assertEquals(
        ImmutableList.of("x|INT|null|the input", "y|DOUBLE|1.0|null"),
        describe(parsed.inputParams));
    assertEquals("it's at C:\\tmp", parsed.comment);
    assertEquals("false", parsed.isDeterministic);
    assertEquals("READS SQL DATA", parsed.sqlDataAccess);
  }

  @Test
  public void parseDescribeFunctionExtended_parsesEmptyParameterList() {
    ParsedFunction parsed =
        parseFunction(
            "f_zero",
            "Type:          SCALAR",
            "Input:         ()",
            "Returns:       INT",
            "Body:          42");

    assertTrue(parsed.inputParams.isEmpty());
    assertEquals("INT", parsed.dataType);
  }

  @Test
  public void parseDescribeFunctionExtended_keepsUnsplittableParameterTextAsType() {
    ParsedFunction parsed =
        parseFunction("f_odd", "Input:         x", "Returns:       INT", "Body:          1");

    assertEquals(ImmutableList.of("|x|null|null"), describe(parsed.inputParams));
  }

  @Test
  public void parseDescribeFunctionExtended_keepsUnterminatedCommentInType() {
    ParsedFunction parsed =
        parseFunction(
            "f_open",
            "Input:         x INT 'open comment",
            "Returns:       INT",
            "Body:          1");

    assertEquals(ImmutableList.of("x|INT 'open comment|null|null"), describe(parsed.inputParams));
  }

  @Test
  public void parseDescribeFunctionExtended_writesInModeForParametersButNotTableColumns() {
    String inputMode = FunctionParameterMode.IN.name();
    ParsedFunction tableFunction =
        parseFunction(
            "f_rows",
            "Type:          TABLE",
            "Input:         n INT    'rows'",
            "               x STRING 'open comment",
            "Returns:       id INT",
            "Body:          SELECT n AS id");
    ParsedFunction unsplittable =
        parseFunction("f_odd", "Input:         x", "Returns:       INT", "Body:          1");

    assertEquals(ImmutableList.of(inputMode, inputMode), modes(tableFunction.inputParams));
    assertEquals(ImmutableList.of("null"), modes(tableFunction.returnParams));
    assertEquals(ImmutableList.of(inputMode), modes(unsplittable.inputParams));
  }

  @Test
  public void parseDescribeFunctionExtended_parsesPythonScalarFunctionAndKeepsIndentation() {
    String body =
        "            if not input_text:\n"
            + "                return None\n"
            + "            return input_text.strip()";

    ParsedFunction parsed =
        parseFunction(
            "py_slug",
            "Function:      hive_metastore.legacy_db.py_slug",
            "Input:         input_text STRING 'text input'",
            "Returns:       STRING",
            "Comment:       Python scalar UDF",
            "Deterministic: true",
            "Owner:         alice",
            "Body:          \n" + body + "\n        ",
            "Language:      Python");

    assertFalse(parsed.isTableFunction);
    assertEquals(
        ImmutableList.of("input_text|STRING|null|text input"), describe(parsed.inputParams));
    assertEquals("STRING", parsed.dataType);
    assertEquals(body, parsed.routineDefinition);
    assertEquals("PYTHON", parsed.routineLanguage);
  }

  @Test
  public void parseDescribeFunctionExtended_parsesPythonTableFunctionWithHandler() {
    String body =
        "class RowEmitter:\n"
            + "    def eval(self, n: int):\n"
            + "        for idx in range(n):\n"
            + "            yield (idx, 'x')";

    ParsedFunction parsed =
        parseFunction(
            "py_udtf",
            "Function:    hive_metastore.legacy_db.py_udtf",
            "Input:       n INT 'rows to emit'",
            "Returns:     row_num INT    'index'",
            "             label   STRING 'formatted label'",
            "Comment:     Python UDTF",
            "Owner:       alice",
            "Body:        \n" + body + "\n",
            "Language:    Python",
            "Handler:     RowEmitter");

    assertTrue(parsed.isTableFunction);
    assertEquals(ImmutableList.of("n|INT|null|rows to emit"), describe(parsed.inputParams));
    assertEquals(
        ImmutableList.of("row_num|INT|null|index", "label|STRING|null|formatted label"),
        describe(parsed.returnParams));
    assertEquals(
        "TABLE(`row_num` INT COMMENT 'index', `label` STRING COMMENT 'formatted label')",
        parsed.dataType);
    assertEquals(body, parsed.routineDefinition);
    assertEquals("PYTHON", parsed.routineLanguage);
    assertEquals("RowEmitter", parsed.handler);
  }

  private static ParsedFunction parseFunction(String functionName, String... lines) {
    return DatabricksHiveMetastoreFunctionParser.parseDescribeFunctionExtended(
        "hive_metastore", "legacy_db", functionName, Arrays.asList(lines));
  }

  /** Returns each parameter as {@code name|type|default|comment}, with absent parts as null. */
  private static ImmutableList<String> describe(List<DatabricksFunctionParameter> parameters) {
    ImmutableList.Builder<String> described = ImmutableList.builder();
    for (DatabricksFunctionParameter parameter : parameters) {
      described.add(
          String.join(
              "|",
              parameter.name,
              parameter.type,
              String.valueOf(parameter.defaultExpression),
              String.valueOf(parameter.comment)));
    }
    return described.build();
  }

  /** Returns the mode of each parameter, with an absent mode as null. */
  private static ImmutableList<String> modes(List<DatabricksFunctionParameter> parameters) {
    ImmutableList.Builder<String> modes = ImmutableList.builder();
    for (DatabricksFunctionParameter parameter : parameters) {
      modes.add(String.valueOf(parameter.mode));
    }
    return modes.build();
  }
}
