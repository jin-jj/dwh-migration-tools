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

import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreParser.ParsedFunctionExtended;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreParser.ParsedTableExtended;
import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksHiveMetastoreParserTest {

  @Test
  public void parseShowTableExtended_parsesAttributesColumnsAndConstraints() {
    String blob =
        "Catalog: hive_metastore\n"
            + "Database: legacy_db\n"
            + "Table: events\n"
            + "Owner: data_eng\n"
            + "Created Time: Wed Sep 18 12:00:00 UTC 2024\n"
            + "Type: EXTERNAL\n"
            + "Provider: parquet\n"
            + "Comment: Legacy events\n"
            + "Table Properties: [delta.checkpoint.writeStatsAsJson=false, "
            + "delta.constraints.valid_event_type=event_type IN ('click', 'view', 'purchase'), "
            + "delta.enableDeletionVectors=true]\n"
            + "Location: s3://legacy-bucket/events\n"
            + "Partition Columns: [`dt`, `country`]\n"
            + "Schema: root\n"
            + " |-- event_id: string (nullable = false)\n"
            + " |-- payload: string (nullable = true)\n"
            + " |-- dt: date (nullable = true)\n"
            + " |-- country: string (nullable = true)\n";

    ParsedTableExtended parsed =
        DatabricksHiveMetastoreParser.parseShowTableExtended("legacy_db", "events", blob);
    assertEquals("hive_metastore", parsed.catalogName);
    assertEquals("legacy_db", parsed.schemaName);
    assertEquals("events", parsed.tableName);
    assertEquals("EXTERNAL", parsed.tableType);
    assertEquals("PARQUET", parsed.dataSourceFormat);
    assertEquals("s3://legacy-bucket/events", parsed.storageLocation);
    assertEquals(4, parsed.columns.size());
    assertEquals("NO", parsed.columns.get(0).isNullable);
    assertEquals("", parsed.columns.get(0).partitionIndex);
    assertEquals("0", parsed.columns.get(2).partitionIndex);
    assertEquals("1", parsed.columns.get(3).partitionIndex);
    assertEquals(1, parsed.constraints.size());
    assertEquals("valid_event_type", parsed.constraints.get(0).constraintName);
    assertEquals("CHECK", parsed.constraints.get(0).constraintType);
    assertEquals(
        "event_type IN ('click', 'view', 'purchase')", parsed.constraints.get(0).constraintDetails);
  }

  @Test
  public void parseDescribeFunctionExtended_buildsSqlFunctionDdl() {
    ParsedFunctionExtended parsed =
        DatabricksHiveMetastoreParser.parseDescribeFunctionExtended(
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
    assertEquals("x INT", parsed.inputParams);
    assertEquals("x * 2", parsed.routineDefinition);
    assertEquals("SQL", parsed.routineLanguage);
    assertEquals(
        "CREATE FUNCTION hive_metastore.legacy_db.double_it(x INT)\n"
            + "RETURNS INT\n"
            + "COMMENT 'Doubles input'\n"
            + "RETURN x * 2",
        parsed.ddl);
  }

  @Test
  public void parseDescribeFunctionExtended_keepsMultiLineBody() {
    String body = "CASE\n  WHEN x > 0 THEN 'pos'\n  ELSE 'non-pos'\nEND";

    ParsedFunctionExtended parsed =
        DatabricksHiveMetastoreParser.parseDescribeFunctionExtended(
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
        "CREATE FUNCTION hive_metastore.legacy_db.sign_label(x INT)\n"
            + "RETURNS STRING\n"
            + "RETURN "
            + body,
        parsed.ddl);
  }

  @Test
  public void parseDescribeFunctionExtended_buildsJavaFunctionDdl() {
    ParsedFunctionExtended parsed =
        DatabricksHiveMetastoreParser.parseDescribeFunctionExtended(
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
        "CREATE FUNCTION hive_metastore.legacy_db.custom_hash AS 'com.acme.udf.CustomHashUdf'",
        parsed.ddl);
  }
}
