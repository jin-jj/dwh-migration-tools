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

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreFunctionDdlFormatter.formatCreateFunction;
import static org.junit.Assert.assertEquals;

import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreFunctionParser.ParsedFunction;
import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksHiveMetastoreFunctionDdlFormatterTest {

  @Test
  public void formatCreateFunction_formatsSqlFunction() {
    ParsedFunction function =
        parseFunction(
            "double_it",
            "Function: hive_metastore.legacy_db.double_it",
            "Input: x INT",
            "Returns: INT",
            "Comment: Doubles input",
            "Owner: alice",
            "Body: x * 2");

    assertEquals(
        "CREATE FUNCTION `hive_metastore`.`legacy_db`.`double_it`(`x` INT)\n"
            + "RETURNS INT\n"
            + "COMMENT 'Doubles input'\n"
            + "RETURN x * 2",
        formatCreateFunction(function));
  }

  @Test
  public void formatCreateFunction_formatsHiveFunctionClass() {
    ParsedFunction function =
        parseFunction(
            "custom_hash",
            "Function: default.custom_hash",
            "Class: com.acme.udf.CustomHashUdf",
            "Owner: bob");

    assertEquals(
        "CREATE FUNCTION `hive_metastore`.`legacy_db`.`custom_hash`"
            + " AS 'com.acme.udf.CustomHashUdf'",
        formatCreateFunction(function));
  }

  @Test
  public void formatCreateFunction_returnsEmptyWithoutBodyOrClass() {
    ParsedFunction function =
        parseFunction("f_unknown", "Function: hive_metastore.legacy_db.f_unknown", "Owner: bob");

    assertEquals("", formatCreateFunction(function));
  }

  @Test
  public void formatCreateFunction_keepsMultiLineBody() {
    String body = "CASE\n  WHEN x > 0 THEN 'pos'\n  ELSE 'non-pos'\nEND";

    ParsedFunction function =
        parseFunction(
            "sign_label",
            "Type: SCALAR",
            "Input: x INT",
            "Returns: STRING",
            "Body:          " + body);

    assertEquals(
        "CREATE FUNCTION `hive_metastore`.`legacy_db`.`sign_label`(`x` INT)\n"
            + "RETURNS STRING\n"
            + "RETURN "
            + body,
        formatCreateFunction(function));
  }

  @Test
  public void formatCreateFunction_formatsParametersWithDefaultsAndComments() {
    ParsedFunction function =
        parseFunction(
            "f_params",
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
            "Body:          concat(a, b)");

    assertEquals(
        "CREATE FUNCTION `hive_metastore`.`legacy_db`.`f_params`("
            + "`c` DECIMAL(10,2) COMMENT 'dec  two', "
            + "`a` INT DEFAULT 5 COMMENT 'has default', "
            + "`b` STRING DEFAULT 'x  y' COMMENT 'it\\'s a  test', "
            + "`d` MAP<STRING, INT> DEFAULT NULL, "
            + "`e` STRUCT<p: INT, q: STRING> DEFAULT NULL COMMENT 'nested')\n"
            + "RETURNS STRING\n"
            + "DETERMINISTIC\n"
            + "CONTAINS SQL\n"
            + "COMMENT 'params test'\n"
            + "RETURN concat(a, b)",
        formatCreateFunction(function));
  }

  @Test
  public void formatCreateFunction_formatsSqlTableFunction() {
    ParsedFunction function =
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

    assertEquals(
        "CREATE FUNCTION `hive_metastore`.`legacy_db`.`f_rows`(`p q` INT, `select` STRING)\n"
            + "RETURNS TABLE(`id` INT COMMENT 'the id', `label` STRING COMMENT 'it\\'s a label')\n"
            + "DETERMINISTIC\n"
            + "CONTAINS SQL\n"
            + "COMMENT 'first line\nsecond line'\n"
            + "RETURN SELECT n AS id, 'x' AS label",
        formatCreateFunction(function));
  }

  @Test
  public void formatCreateFunction_statesNonDeterminismAndEscapesComment() {
    ParsedFunction function =
        parseFunction(
            "f_nd",
            "Type:          SCALAR",
            "Input:         x INT    'the input'",
            "               y DOUBLE DEFAULT 1.0",
            "Returns:       DOUBLE",
            "Comment:       it's at C:\\tmp",
            "Deterministic: false",
            "Data Access:   READS SQL DATA",
            "Body:          x * y * rand()");

    assertEquals(
        "CREATE FUNCTION `hive_metastore`.`legacy_db`.`f_nd`("
            + "`x` INT COMMENT 'the input', `y` DOUBLE DEFAULT 1.0)\n"
            + "RETURNS DOUBLE\n"
            + "NOT DETERMINISTIC\n"
            + "READS SQL DATA\n"
            + "COMMENT 'it\\'s at C:\\\\tmp'\n"
            + "RETURN x * y * rand()",
        formatCreateFunction(function));
  }

  @Test
  public void formatCreateFunction_formatsEmptyParameterList() {
    ParsedFunction function =
        parseFunction(
            "f_zero",
            "Type:          SCALAR",
            "Input:         ()",
            "Returns:       INT",
            "Body:          42");

    assertEquals(
        "CREATE FUNCTION `hive_metastore`.`legacy_db`.`f_zero`()\nRETURNS INT\nRETURN 42",
        formatCreateFunction(function));
  }

  @Test
  public void formatCreateFunction_keepsUnsplittableParameterText() {
    ParsedFunction function =
        parseFunction("f_odd", "Input:         x", "Returns:       INT", "Body:          1");

    assertEquals(
        "CREATE FUNCTION `hive_metastore`.`legacy_db`.`f_odd`(x)\nRETURNS INT\nRETURN 1",
        formatCreateFunction(function));
  }

  @Test
  public void formatCreateFunction_formatsPythonScalarFunctionAndKeepsIndentation() {
    String body =
        "            if not input_text:\n"
            + "                return None\n"
            + "            return input_text.strip()";

    ParsedFunction function =
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

    assertEquals(
        "CREATE FUNCTION `hive_metastore`.`legacy_db`.`py_slug`("
            + "`input_text` STRING COMMENT 'text input')\n"
            + "RETURNS STRING\n"
            + "LANGUAGE PYTHON\n"
            + "DETERMINISTIC\n"
            + "COMMENT 'Python scalar UDF'\n"
            + "AS $$\n"
            + body
            + "\n$$",
        formatCreateFunction(function));
  }

  @Test
  public void formatCreateFunction_formatsPythonTableFunctionWithHandler() {
    String body =
        "class RowEmitter:\n"
            + "    def eval(self, n: int):\n"
            + "        for idx in range(n):\n"
            + "            yield (idx, 'x')";

    ParsedFunction function =
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

    assertEquals(
        "CREATE FUNCTION `hive_metastore`.`legacy_db`.`py_udtf`(`n` INT COMMENT 'rows to emit')\n"
            + "RETURNS TABLE(`row_num` INT COMMENT 'index',"
            + " `label` STRING COMMENT 'formatted label')\n"
            + "LANGUAGE PYTHON\n"
            + "HANDLER 'RowEmitter'\n"
            + "COMMENT 'Python UDTF'\n"
            + "AS $$\n"
            + body
            + "\n$$",
        formatCreateFunction(function));
  }

  @Test
  public void formatCreateFunction_quotesBodyThatContainsDollarQuotes() {
    ParsedFunction function =
        parseFunction(
            "py_dollars",
            "Returns:       STRING",
            "Body:          return '$$'",
            "Language:      Python");

    assertEquals(
        "CREATE FUNCTION `hive_metastore`.`legacy_db`.`py_dollars`()\n"
            + "RETURNS STRING\n"
            + "LANGUAGE PYTHON\n"
            + "AS 'return \\'$$\\''",
        formatCreateFunction(function));
  }

  private static ParsedFunction parseFunction(String functionName, String... lines) {
    return DatabricksHiveMetastoreFunctionParser.parseDescribeFunctionExtended(
        "hive_metastore", "legacy_db", functionName, Arrays.asList(lines));
  }
}
