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

import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreFunctionParser.ParsedFunctionExtended;
import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksHiveMetastoreFunctionParserTest {

  @Test
  public void parseDescribeFunctionExtended_buildsSqlFunctionDdl() {
    ParsedFunctionExtended parsed =
        DatabricksHiveMetastoreFunctionParser.parseDescribeFunctionExtended(
            "hive_metastore",
            "legacy_db",
            "double_it",
            Arrays.asList(
                "Function: hive_metastore.legacy_db.double_it",
                "Input: x INT",
                "Returns: INT",
                "Comment: Doubles input",
                "Owner: alice",
                "Body: x * 2"));

    assertEquals("INT", parsed.dataType);
    assertEquals("`x` INT", parsed.inputParams);
    assertEquals("x * 2", parsed.routineDefinition);
    assertEquals("SQL", parsed.routineLanguage);
    assertEquals("Doubles input", parsed.comment);
    assertEquals("alice", parsed.owner);
    assertEquals(
        "CREATE FUNCTION `hive_metastore`.`legacy_db`.`double_it`(`x` INT)\n"
            + "RETURNS INT\n"
            + "COMMENT 'Doubles input'\n"
            + "RETURN x * 2",
        parsed.ddl);
  }

  @Test
  public void parseDescribeFunctionExtended_buildsJavaFunctionDdl() {
    ParsedFunctionExtended parsed =
        DatabricksHiveMetastoreFunctionParser.parseDescribeFunctionExtended(
            "hive_metastore",
            "legacy_db",
            "custom_hash",
            Arrays.asList(
                "Function: default.custom_hash",
                "Class: com.acme.udf.CustomHashUdf",
                "Owner: bob"));

    assertEquals("com.acme.udf.CustomHashUdf", parsed.routineDefinition);
    assertEquals("JAVA", parsed.routineLanguage);
    assertEquals(
        "CREATE FUNCTION `hive_metastore`.`legacy_db`.`custom_hash`"
            + " AS 'com.acme.udf.CustomHashUdf'",
        parsed.ddl);
  }

  @Test
  public void parseDescribeFunctionExtended_writesNoDdlWithoutBodyOrClass() {
    ParsedFunctionExtended parsed =
        parseFunction("f_unknown", "Function: hive_metastore.legacy_db.f_unknown", "Owner: bob");

    assertEquals("bob", parsed.owner);
    assertEquals("", parsed.routineDefinition);
    assertEquals("", parsed.routineLanguage);
    assertEquals("", parsed.ddl);
  }

  @Test
  public void parseDescribeFunctionExtended_keepsMultiLineBody() {
    String body = "CASE\n  WHEN x > 0 THEN 'pos'\n  ELSE 'non-pos'\nEND";

    ParsedFunctionExtended parsed =
        DatabricksHiveMetastoreFunctionParser.parseDescribeFunctionExtended(
            "hive_metastore",
            "legacy_db",
            "sign_label",
            Arrays.asList(
                "Function: hive_metastore.legacy_db.sign_label",
                "Type: SCALAR",
                "Input: x INT",
                "Returns: STRING",
                "Body:          " + body));

    assertEquals(body, parsed.routineDefinition);
    assertEquals("SQL", parsed.routineLanguage);
    assertEquals(
        "CREATE FUNCTION `hive_metastore`.`legacy_db`.`sign_label`(`x` INT)\n"
            + "RETURNS STRING\n"
            + "RETURN "
            + body,
        parsed.ddl);
  }

  @Test
  public void parseDescribeFunctionExtended_parsesPaddedParametersWithDefaultsAndComments() {
    ParsedFunctionExtended parsed =
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
        "`c` DECIMAL(10,2) COMMENT 'dec  two', "
            + "`a` INT DEFAULT 5 COMMENT 'has default', "
            + "`b` STRING DEFAULT 'x  y' COMMENT 'it\\'s a  test', "
            + "`d` MAP<STRING, INT> DEFAULT NULL, "
            + "`e` STRUCT<p: INT, q: STRING> DEFAULT NULL COMMENT 'nested'",
        parsed.inputParams);
    assertEquals("STRING", parsed.dataType);
    assertEquals(
        "CREATE FUNCTION `hive_metastore`.`legacy_db`.`f_params`("
            + parsed.inputParams
            + ")\n"
            + "RETURNS STRING\n"
            + "DETERMINISTIC\n"
            + "CONTAINS SQL\n"
            + "COMMENT 'params test'\n"
            + "RETURN concat(a, b)",
        parsed.ddl);
  }

  @Test
  public void parseDescribeFunctionExtended_separatesDefaultExpressionsFromComments() {
    ParsedFunctionExtended parsed =
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
        "`i` INTERVAL DAY TO SECOND COMMENT 'ivl', "
            + "`s` STRUCT<a: INT COMMENT 'x'> COMMENT 'st', "
            + "`g` STRING DEFAULT 'it\\'s' COMMENT 'user\\'s', "
            + "`h` STRING DEFAULT 'a' || 'b', "
            + "`k` STRING DEFAULT 'a' || 'b' COMMENT 'c', "
            + "`m` STRING DEFAULT CASE WHEN true THEN 'a' END COMMENT 'd'",
        parsed.inputParams);
  }

  @Test
  public void parseDescribeFunctionExtended_parsesTableColumnsAndNamesWithSpaces() {
    ParsedFunctionExtended parsed =
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

    assertEquals("`p q` INT, `select` STRING", parsed.inputParams);
    assertEquals(
        "TABLE(`id` INT COMMENT 'the id', `label` STRING COMMENT 'it\\'s a label')",
        parsed.dataType);
    assertEquals("first line\nsecond line", parsed.comment);
    assertEquals(
        "CREATE FUNCTION `hive_metastore`.`legacy_db`.`f_rows`(`p q` INT, `select` STRING)\n"
            + "RETURNS "
            + parsed.dataType
            + "\n"
            + "DETERMINISTIC\n"
            + "CONTAINS SQL\n"
            + "COMMENT 'first line\nsecond line'\n"
            + "RETURN SELECT n AS id, 'x' AS label",
        parsed.ddl);
  }

  @Test
  public void parseDescribeFunctionExtended_statesNonDeterminismAndEscapesComment() {
    ParsedFunctionExtended parsed =
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

    assertEquals("`x` INT COMMENT 'the input', `y` DOUBLE DEFAULT 1.0", parsed.inputParams);
    assertEquals("it's at C:\\tmp", parsed.comment);
    assertEquals(
        "CREATE FUNCTION `hive_metastore`.`legacy_db`.`f_nd`("
            + parsed.inputParams
            + ")\n"
            + "RETURNS DOUBLE\n"
            + "NOT DETERMINISTIC\n"
            + "READS SQL DATA\n"
            + "COMMENT 'it\\'s at C:\\\\tmp'\n"
            + "RETURN x * y * rand()",
        parsed.ddl);
  }

  @Test
  public void parseDescribeFunctionExtended_writesEmptyParameterListWithoutArguments() {
    ParsedFunctionExtended parsed =
        parseFunction(
            "f_zero",
            "Type:          SCALAR",
            "Input:         ()",
            "Returns:       INT",
            "Body:          42");

    assertEquals("", parsed.inputParams);
    assertEquals(
        "CREATE FUNCTION `hive_metastore`.`legacy_db`.`f_zero`()\nRETURNS INT\nRETURN 42",
        parsed.ddl);
  }

  private static ParsedFunctionExtended parseFunction(String functionName, String... lines) {
    return DatabricksHiveMetastoreFunctionParser.parseDescribeFunctionExtended(
        "hive_metastore", "legacy_db", functionName, Arrays.asList(lines));
  }
}
