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
}
