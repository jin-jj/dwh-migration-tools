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

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreParser.hasStringColumn;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreParser.parseDescribeTableTypes;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreParser.withCharVarcharLengths;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreParser.ParsedColumn;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreParser.ParsedFunctionExtended;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreParser.ParsedTableExtended;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksHiveMetastoreParserTest {

  private static final String OWNER = "data_eng";
  private static final String DELTA_LOCATION = "dbfs:/user/hive/warehouse/legacy_db.db/events";
  private static final String DELTA_TABLE_PROPERTIES =
      "[delta.checkpoint.writeStatsAsJson=false, delta.checkpoint.writeStatsAsStruct=true,"
          + " delta.enableDeletionVectors=true]";
  private static final String VIEW_TEXT =
      "SELECT id\nFROM legacy_db.events\nWHERE name:a IS NOT NULL";
  private static final String VIEW_TABLE_PROPERTIES = "[transient_lastDdlTime=1790543963]";

  @Test
  public void parseShowTableExtended_parsesAttributesColumnsAndConstraints() {
    String tableProperties =
        "[delta.checkpoint.writeStatsAsJson=false, "
            + "delta.constraints.valid_event_type=event_type IN ('click', 'view', 'purchase'), "
            + "delta.enableDeletionVectors=true]";
    String blob =
        "Catalog: hive_metastore\n"
            + "Database: legacy_db\n"
            + "Table: events\n"
            + "Owner: data_eng\n"
            + "Created Time: Wed Sep 18 12:00:00 UTC 2024\n"
            + "Type: EXTERNAL\n"
            + "Provider: parquet\n"
            + "Comment: Legacy events\n"
            + "Table Properties: "
            + tableProperties
            + "\n"
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
    assertEquals(tableProperties, parsed.tableProperties);
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
  public void parseShowTableExtended_keepsHiveStorageDescriptor() {
    String serdeLibrary = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
    String inputFormat = "org.apache.hadoop.mapred.TextInputFormat";
    String outputFormat = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
    String storageProperties = "[field.delim=,, serialization.format=,]";
    String tableProperties = "[transient_lastDdlTime=1790541380]";
    String blob =
        "Catalog: hive_metastore\n"
            + "Database: legacy_db\n"
            + "Table: csv_events\n"
            + "Type: EXTERNAL\n"
            + "Provider: hive\n"
            + "Table Properties: "
            + tableProperties
            + "\n"
            + "Location: dbfs:/mnt/legacy/csv_events\n"
            + "Serde Library: "
            + serdeLibrary
            + "\n"
            + "InputFormat: "
            + inputFormat
            + "\n"
            + "OutputFormat: "
            + outputFormat
            + "\n"
            + "Storage Properties: "
            + storageProperties
            + "\n"
            + "Partition Provider: Catalog\n"
            + "Schema: root\n"
            + " |-- id: integer (nullable = true)\n"
            + " |-- name: string (nullable = true)\n";

    ParsedTableExtended parsed =
        DatabricksHiveMetastoreParser.parseShowTableExtended("legacy_db", "csv_events", blob);

    assertEquals("HIVE", parsed.dataSourceFormat);
    assertEquals(serdeLibrary, parsed.serdeLibrary);
    assertEquals(inputFormat, parsed.inputFormat);
    assertEquals(outputFormat, parsed.outputFormat);
    assertEquals(storageProperties, parsed.storageProperties);
    assertEquals(tableProperties, parsed.tableProperties);
    assertEquals(2, parsed.columns.size());
    assertEquals(0, parsed.constraints.size());
  }

  @Test
  public void parseShowTableExtended_keepsMultiLineViewText() {
    ParsedTableExtended parsed =
        DatabricksHiveMetastoreParser.parseShowTableExtended(
            "legacy_db", "active_ids", viewBlob("view line one", VIEW_TEXT));

    assertEquals(VIEW_TEXT, parsed.viewDefinition);
    assertEquals("VIEW", parsed.tableType);
    assertEquals(VIEW_TABLE_PROPERTIES, parsed.tableProperties);
    assertEquals(1, parsed.columns.size());
  }

  @Test
  public void parseShowTableExtended_keepsViewCommentLinesThatLookLikeKeys() {
    for (String comment :
        Arrays.asList("view line one\nOwner: not the owner", "Sales view\nLocation: building 5")) {
      ParsedTableExtended parsed =
          DatabricksHiveMetastoreParser.parseShowTableExtended(
              "legacy_db", "active_ids", viewBlob(comment, VIEW_TEXT));

      assertEquals(comment, parsed.comment);
      assertEquals(comment, OWNER, parsed.owner);
      assertEquals(comment, "", parsed.storageLocation);
      assertEquals(comment, VIEW_TEXT, parsed.viewDefinition);
    }
  }

  @Test
  public void parseShowTableExtended_keepsTableCommentLinesThatLookLikeKeys() {
    for (String comment :
        Arrays.asList(
            "first line\nType: VIEW\nLocation: fake\nlast line",
            "Nightly load\nStatistics: refreshed daily",
            "first line\nTable Properties: [fake=1]",
            "first line\nComment: second line")) {
      ParsedTableExtended parsed =
          DatabricksHiveMetastoreParser.parseShowTableExtended(
              "legacy_db", "events", deltaTableBlob(comment, DELTA_TABLE_PROPERTIES));

      assertEquals(comment, parsed.comment);
      assertEquals(comment, "MANAGED", parsed.tableType);
      assertEquals(comment, DELTA_TABLE_PROPERTIES, parsed.tableProperties);
      assertEquals(comment, DELTA_LOCATION, parsed.storageLocation);
    }
  }

  @Test
  public void parseShowTableExtended_keepsCommentWhitespace() {
    String comment = "  indented\ntrailing newline\n";

    ParsedTableExtended parsed =
        DatabricksHiveMetastoreParser.parseShowTableExtended(
            "legacy_db", "events", deltaTableBlob(comment, DELTA_TABLE_PROPERTIES));

    assertEquals(comment, parsed.comment);
    assertEquals(DELTA_TABLE_PROPERTIES, parsed.tableProperties);
  }

  @Test
  public void parseShowTableExtended_keepsMultiLineCheckConstraint() {
    String checkConstraint = "a > 0\nAND b > 0";
    String tableProperties =
        "[delta.checkpoint.writeStatsAsJson=false, delta.constraints.positive="
            + checkConstraint
            + ", delta.enableDeletionVectors=true]";

    ParsedTableExtended parsed =
        DatabricksHiveMetastoreParser.parseShowTableExtended(
            "legacy_db", "events", deltaTableBlob("checked", tableProperties));

    assertEquals(tableProperties, parsed.tableProperties);
    assertEquals(DELTA_LOCATION, parsed.storageLocation);
    assertEquals(1, parsed.constraints.size());
    assertEquals(checkConstraint, parsed.constraints.get(0).constraintDetails);
  }

  @Test
  public void parseShowTableExtended_keepsMultiLineStorageProperties() {
    String storageProperties = "[line.delim=\n, field.delim=,, serialization.format=,]";
    String blob =
        String.join(
            "\n",
            "Catalog: hive_metastore",
            "Database: legacy_db",
            "Table: csv_events",
            "Type: EXTERNAL",
            "Provider: hive",
            "Table Properties: [transient_lastDdlTime=1790544831]",
            "Location: dbfs:/mnt/legacy/csv_events",
            "Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            "InputFormat: org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat: org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "Storage Properties: " + storageProperties,
            "Partition Provider: Catalog",
            "Schema: root",
            " |-- id: integer (nullable = true)",
            " |-- name: string (nullable = true)",
            "",
            "");

    ParsedTableExtended parsed =
        DatabricksHiveMetastoreParser.parseShowTableExtended("legacy_db", "csv_events", blob);

    assertEquals(storageProperties, parsed.storageProperties);
    assertEquals(2, parsed.columns.size());
  }

  @Test
  public void parseShowTableExtended_skipsKeyLineOutOfSparkOrder() {
    String comment = "first line\nLocation: fake";
    String blob = deltaTableBlob(comment, DELTA_TABLE_PROPERTIES) + "Owner: late";

    ParsedTableExtended parsed =
        DatabricksHiveMetastoreParser.parseShowTableExtended("legacy_db", "events", blob);

    assertEquals(comment, parsed.comment);
    assertEquals(OWNER, parsed.owner);
    assertEquals(DELTA_TABLE_PROPERTIES, parsed.tableProperties);
    assertEquals(DELTA_LOCATION, parsed.storageLocation);
    assertEquals(1, parsed.columns.size());
  }

  @Test
  public void parseShowTableExtended_writesColumnTypesInHiveSyntax() {
    String blob =
        String.join(
            "\n",
            "Catalog: hive_metastore",
            "Database: legacy_db",
            "Table: typed",
            "Type: MANAGED",
            "Provider: delta",
            "Schema: root",
            " |-- a:b: short (nullable = false)",
            " |-- t: byte (nullable = true)",
            " |-- amount: decimal(10,2) (nullable = true)",
            " |-- ts: timestamp_ntz (nullable = true)",
            " |-- arr: array (nullable = true)",
            " |    |-- element: struct (containsNull = true)",
            " |    |    |-- x: long (nullable = true)",
            " |    |    |-- y: array (nullable = true)",
            " |    |    |    |-- element: string (containsNull = true)",
            " |-- m: map (nullable = true)",
            " |    |-- key: string",
            " |    |-- value: map (valueContainsNull = true)",
            " |    |    |-- key: integer",
            " |    |    |-- value: timestamp (valueContainsNull = true)",
            " |-- st: struct (nullable = true)",
            " |    |-- inner: struct (nullable = true)",
            " |    |    |-- z: double (nullable = true)",
            " |    |-- k: v: integer (nullable = true)",
            "",
            "");

    ParsedTableExtended parsed =
        DatabricksHiveMetastoreParser.parseShowTableExtended("legacy_db", "typed", blob);

    ImmutableList.Builder<String> columns = ImmutableList.builder();
    for (ParsedColumn column : parsed.columns) {
      columns.add(
          String.join(
              "|",
              String.valueOf(column.ordinalPosition),
              column.columnName,
              column.dataType,
              column.isNullable));
    }
    assertEquals(
        ImmutableList.of(
            "0|a:b|smallint|NO",
            "1|t|tinyint|YES",
            "2|amount|decimal(10,2)|YES",
            "3|ts|timestamp_ntz|YES",
            "4|arr|array<struct<x:bigint,y:array<string>>>|YES",
            "5|m|map<string,map<int,timestamp>>|YES",
            "6|st|struct<inner:struct<z:double>,k: v:int>|YES"),
        columns.build());
  }

  @Test
  public void hasStringColumn_findsStringTypeButNotStringNamePrefix() {
    assertTrue(
        hasStringColumn(
            tableWithSchema(
                " |-- tags: array (nullable = true)",
                " |    |-- element: string (containsNull = true)")));
    assertFalse(
        hasStringColumn(
            tableWithSchema(
                " |-- s: struct (nullable = true)",
                " |    |-- string_id: long (nullable = true)")));
  }

  @Test
  public void parseDescribeTableTypes_stopsAtPartitionInformation() {
    ImmutableList<List<String>> columnRows =
        ImmutableList.of(
            Arrays.asList("id", "bigint", "primary id"),
            Arrays.asList("name", "varchar(20)", "customer's name"),
            Arrays.asList("code", "char(3)", null),
            Arrays.asList("addr", "struct<city:string,zip:int>", null),
            Arrays.asList("dt", "date", null));
    List<List<String>> rows = new ArrayList<>(columnRows);
    rows.add(Arrays.asList("# Partition Information", "", ""));
    rows.add(Arrays.asList("# col_name", "data_type", "comment"));
    rows.add(Arrays.asList("dt", "date", null));

    ImmutableMap.Builder<String, String> expected = ImmutableMap.builder();
    for (List<String> row : columnRows) {
      expected.put(row.get(0), row.get(1));
    }
    assertEquals(expected.build(), parseDescribeTableTypes(rows));
  }

  @Test
  public void withCharVarcharLengths_replacesTypesThatDifferOnlyInLengths() {
    // DESCRIBE TABLE shows the first 25 fields of a struct, as for this 30-field struct.
    int wideFieldCount = 30;
    int shownFieldCount = 25;
    List<String> wideFields = new ArrayList<>();
    for (int field = 0; field < wideFieldCount - 1; field++) {
      wideFields.add(String.format("f%02d:int", field));
    }
    wideFields.add("last:string");
    String wideTreeType = "struct<" + String.join(",", wideFields) + ">";
    String wideDescribedType =
        String.format(
            "struct<%s,... %d more fields>",
            String.join(",", wideFields.subList(0, shownFieldCount)),
            wideFieldCount - shownFieldCount);
    ImmutableList<ParsedColumn> columns =
        ImmutableList.of(
            new ParsedColumn(0, "name", "string", "YES", ""),
            new ParsedColumn(1, "code", "string", "NO", "0"),
            new ParsedColumn(2, "s", "struct<a:string,b:int>", "YES", ""),
            new ParsedColumn(3, "arr", "array<string>", "YES", ""),
            new ParsedColumn(4, "m", "map<string,string>", "YES", ""),
            new ParsedColumn(5, "plain", "string", "YES", ""),
            new ParsedColumn(6, "w", wideTreeType, "YES", ""),
            new ParsedColumn(7, "n", "int", "YES", ""),
            new ParsedColumn(8, "added", "string", "YES", ""));
    ImmutableMap<String, String> describedTypes =
        ImmutableMap.<String, String>builder()
            .put("name", "varchar(20)")
            .put("code", "char(3)")
            .put("s", "struct<a:varchar(10),b:int>")
            .put("arr", "array<char(2)>")
            .put("m", "map<varchar(5),char(1)>")
            .put("plain", "string")
            .put("w", wideDescribedType)
            .put("n", "varchar(3)")
            .build();
    ImmutableSet<String> columnsWithDescribedTypes =
        ImmutableSet.of("name", "code", "s", "arr", "m", "plain");

    ImmutableList<ParsedColumn> result = withCharVarcharLengths(columns, describedTypes);

    ImmutableList.Builder<String> expected = ImmutableList.builder();
    for (ParsedColumn column : columns) {
      String dataType =
          columnsWithDescribedTypes.contains(column.columnName)
              ? describedTypes.get(column.columnName)
              : column.dataType;
      expected.add(describeColumn(column.withDataType(dataType)));
    }
    ImmutableList.Builder<String> actual = ImmutableList.builder();
    for (ParsedColumn column : result) {
      actual.add(describeColumn(column));
    }
    assertEquals(expected.build(), actual.build());
  }

  @Test
  public void parseShowTableExtended_writesCreatedTimeAsInstant() {
    ParsedTableExtended parsed =
        DatabricksHiveMetastoreParser.parseShowTableExtended(
            "legacy_db", "events", deltaTableBlob("created", DELTA_TABLE_PROPERTIES));

    assertEquals(
        ZonedDateTime.of(2026, 9, 27, 21, 19, 25, 0, ZoneOffset.UTC).toInstant().toString(),
        parsed.createdAt);
  }

  @Test
  public void parseShowTableExtended_keepsCreatedTimeInOtherFormat() {
    String createdTime = "2026-09-27 21:19:25";
    String blob =
        String.join(
            "\n",
            "Catalog: hive_metastore",
            "Database: legacy_db",
            "Table: events",
            "Created Time: " + createdTime,
            "Type: MANAGED",
            "Schema: root",
            " |-- id: integer (nullable = true)",
            "");

    ParsedTableExtended parsed =
        DatabricksHiveMetastoreParser.parseShowTableExtended("legacy_db", "events", blob);

    assertEquals(createdTime, parsed.createdAt);
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
    assertEquals("`x` INT", parsed.inputParams);
    assertEquals("x * 2", parsed.routineDefinition);
    assertEquals("SQL", parsed.routineLanguage);
    assertEquals(
        "CREATE FUNCTION `hive_metastore`.`legacy_db`.`double_it`(`x` INT)\n"
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
        "CREATE FUNCTION `hive_metastore`.`legacy_db`.`sign_label`(`x` INT)\n"
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
        "CREATE FUNCTION `hive_metastore`.`legacy_db`.`custom_hash`"
            + " AS 'com.acme.udf.CustomHashUdf'",
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

  /**
   * Returns a {@code SHOW TABLE EXTENDED} blob as Databricks prints it for a managed Delta table.
   */
  private static String deltaTableBlob(String comment, String tableProperties) {
    return String.join(
        "\n",
        "Catalog: hive_metastore",
        "Database: legacy_db",
        "Table: events",
        "Owner: " + OWNER,
        "Created Time: Sun Sep 27 21:19:25 UTC 2026",
        "Last Access: UNKNOWN",
        "Created By: Spark 4.2.0",
        "Type: MANAGED",
        "Provider: delta",
        "Comment: " + comment,
        "Table Properties: " + tableProperties,
        "Location: " + DELTA_LOCATION,
        "Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        "InputFormat: org.apache.hadoop.mapred.SequenceFileInputFormat",
        "OutputFormat: org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
        "Partition Provider: Catalog",
        "Schema: root",
        " |-- id: integer (nullable = true)",
        "",
        "");
  }

  /** Returns a {@code SHOW TABLE EXTENDED} blob as Databricks prints it for a view. */
  private static String viewBlob(String comment, String viewText) {
    return String.join(
        "\n",
        "Catalog: hive_metastore",
        "Database: legacy_db",
        "Table: active_ids",
        "Owner: " + OWNER,
        "Created Time: Sun Sep 27 21:19:23 UTC 2026",
        "Last Access: UNKNOWN",
        "Created By: Spark 4.2.0",
        "Type: VIEW",
        "Comment: " + comment,
        "View Text: " + viewText,
        "View Original Text: " + viewText,
        "View Schema Mode: COMPENSATION",
        "View Catalog and Namespace: spark_catalog.default",
        "View Query Output Columns: [`id`]",
        "Table Properties: " + VIEW_TABLE_PROPERTIES,
        "Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        "InputFormat: org.apache.hadoop.mapred.SequenceFileInputFormat",
        "OutputFormat: org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
        "Storage Properties: [serialization.format=1]",
        "Schema: root",
        " |-- id: integer (nullable = true)",
        "",
        "");
  }

  private static ParsedFunctionExtended parseFunction(String functionName, String... lines) {
    return DatabricksHiveMetastoreParser.parseDescribeFunctionExtended(
        "hive_metastore", "legacy_db", functionName, Arrays.asList(lines));
  }

  /** Returns a managed Delta table parsed from the given {@code Schema} tree lines. */
  private static ParsedTableExtended tableWithSchema(String... schemaTreeLines) {
    List<String> lines = new ArrayList<>();
    lines.add("Catalog: hive_metastore");
    lines.add("Database: legacy_db");
    lines.add("Table: typed");
    lines.add("Type: MANAGED");
    lines.add("Provider: delta");
    lines.add("Schema: root");
    lines.addAll(Arrays.asList(schemaTreeLines));
    return DatabricksHiveMetastoreParser.parseShowTableExtended(
        "legacy_db", "typed", String.join("\n", lines) + "\n");
  }

  /** Returns all fields of a column, such as {@code 0|name|varchar(20)|YES|}. */
  private static String describeColumn(ParsedColumn column) {
    return String.join(
        "|",
        String.valueOf(column.ordinalPosition),
        column.columnName,
        column.dataType,
        column.isNullable,
        column.partitionIndex);
  }
}
