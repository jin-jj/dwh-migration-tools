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

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.formatDataSourceFormat;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.parseBracketedProperties;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.parseDescribeTableColumns;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.parseIdentifierList;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.withDescribedColumns;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.databricks.sdk.service.catalog.DataSourceFormat;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.DescribedColumn;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.ParsedColumn;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.ParsedTableExtended;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksHiveMetastoreTableParserTest {

  private static final String OWNER = "data_eng";
  private static final String DELTA_LOCATION = "dbfs:/user/hive/warehouse/legacy_db.db/events";
  private static final ImmutableMap<String, String> DELTA_TABLE_PROPERTIES =
      ImmutableMap.of(
          "delta.checkpoint.writeStatsAsJson", "false",
          "delta.checkpoint.writeStatsAsStruct", "true",
          "delta.enableDeletionVectors", "true");
  private static final String VIEW_TEXT =
      "SELECT id\nFROM legacy_db.events\nWHERE name:a IS NOT NULL";
  private static final ImmutableMap<String, String> VIEW_TABLE_PROPERTIES =
      ImmutableMap.of("transient_lastDdlTime", "1790543963");
  private static final String VIEW_CATALOG_AND_NAMESPACE = "spark_catalog.default";
  private static final String VIEW_SCHEMA_MODE = "COMPENSATION";

  @Test
  public void parseShowTableExtended_parsesAttributesAndColumns() {
    ImmutableMap<String, String> tableProperties =
        ImmutableMap.of(
            "delta.checkpoint.writeStatsAsJson", "false",
            "delta.constraints.valid_event_type", "event_type IN ('click', 'view', 'purchase')",
            "delta.enableDeletionVectors", "true");
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
            + bracketed(tableProperties)
            + "\n"
            + "Location: s3://legacy-bucket/events\n"
            + "Partition Columns: [`dt`, `country`]\n"
            + "Schema: root\n"
            + " |-- event_id: string (nullable = false)\n"
            + " |-- payload: string (nullable = true)\n"
            + " |-- dt: date (nullable = true)\n"
            + " |-- country: string (nullable = true)\n";

    ParsedTableExtended parsed =
        DatabricksHiveMetastoreTableParser.parseShowTableExtended("legacy_db", "events", blob);
    assertEquals("legacy_db", parsed.schemaName);
    assertEquals("events", parsed.tableName);
    assertEquals("EXTERNAL", parsed.tableType);
    assertEquals("PARQUET", parsed.dataSourceFormat);
    assertEquals("s3://legacy-bucket/events", parsed.storageLocation);
    assertEquals("Legacy events", parsed.comment);
    assertEquals(OWNER, parsed.owner);
    assertEntriesInOrder(tableProperties, parsed.tableProperties);
    assertEquals(4, parsed.columns.size());
    assertEquals("NO", parsed.columns.get(0).isNullable);
    assertEquals("", parsed.columns.get(0).partitionIndex);
    assertEquals("", parsed.columns.get(0).comment);
    assertEquals("0", parsed.columns.get(2).partitionIndex);
    assertEquals("1", parsed.columns.get(3).partitionIndex);
  }

  @Test
  public void parseShowTableExtended_parsesDeltaCheckConstraints() {
    String constraintName = "valid_event_type";
    String expression = "event_type IN ('click', 'view', 'purchase')";
    ImmutableMap<String, String> tableProperties =
        ImmutableMap.of(
            "delta.checkpoint.writeStatsAsJson",
            "false",
            "delta.constraints." + constraintName,
            expression,
            "delta.enableDeletionVectors",
            "true");

    ParsedTableExtended parsed =
        DatabricksHiveMetastoreTableParser.parseShowTableExtended(
            "legacy_db", "events", deltaTableBlob("checked", bracketed(tableProperties)));

    assertEquals(1, parsed.constraints.size());
    assertEquals(constraintName, parsed.constraints.get(0).constraintName);
    assertEquals("CHECK", parsed.constraints.get(0).constraintType);
    assertEquals(expression, parsed.constraints.get(0).expression);
  }

  @Test
  public void parseShowTableExtended_usesDefaultsWithoutInformation() {
    ParsedTableExtended parsed =
        DatabricksHiveMetastoreTableParser.parseShowTableExtended("legacy_db", "events", null);

    assertEquals("legacy_db", parsed.schemaName);
    assertEquals("events", parsed.tableName);
    assertEquals("", parsed.tableType);
    assertEquals("", parsed.createdAt);
    assertEquals("", parsed.numBuckets);
    assertNull(parsed.viewDefinition);
    assertTrue(parsed.tableProperties.isEmpty());
    assertTrue(parsed.storageProperties.isEmpty());
    assertTrue(parsed.bucketColumns.isEmpty());
    assertTrue(parsed.viewQueryOutputColumns.isEmpty());
    assertTrue(parsed.columns.isEmpty());
    assertTrue(parsed.constraints.isEmpty());
  }

  @Test
  public void parseShowTableExtended_keepsHiveStorageDescriptor() {
    String serdeLibrary = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
    String inputFormat = "org.apache.hadoop.mapred.TextInputFormat";
    String outputFormat = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
    ImmutableMap<String, String> storageProperties =
        ImmutableMap.of("field.delim", ",", "serialization.format", ",");
    ImmutableMap<String, String> tableProperties =
        ImmutableMap.of("transient_lastDdlTime", "1790541380");
    String blob =
        "Catalog: hive_metastore\n"
            + "Database: legacy_db\n"
            + "Table: csv_events\n"
            + "Type: EXTERNAL\n"
            + "Provider: hive\n"
            + "Table Properties: "
            + bracketed(tableProperties)
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
            + bracketed(storageProperties)
            + "\n"
            + "Partition Provider: Catalog\n"
            + "Schema: root\n"
            + " |-- id: integer (nullable = true)\n"
            + " |-- name: string (nullable = true)\n";

    ParsedTableExtended parsed =
        DatabricksHiveMetastoreTableParser.parseShowTableExtended("legacy_db", "csv_events", blob);

    assertEquals("HIVE", parsed.dataSourceFormat);
    assertEquals(serdeLibrary, parsed.serdeLibrary);
    assertEquals(inputFormat, parsed.inputFormat);
    assertEquals(outputFormat, parsed.outputFormat);
    assertEntriesInOrder(storageProperties, parsed.storageProperties);
    assertEntriesInOrder(tableProperties, parsed.tableProperties);
    assertEquals(2, parsed.columns.size());
    assertEquals(0, parsed.constraints.size());
  }

  @Test
  public void parseShowTableExtended_parsesBucketing() {
    String numBuckets = "4";
    ImmutableList<String> bucketColumns = ImmutableList.of("id", "region code");
    ImmutableList<String> sortColumns = ImmutableList.of("id");
    String blob =
        String.join(
            "\n",
            "Catalog: hive_metastore",
            "Database: legacy_db",
            "Table: bucketed",
            "Type: MANAGED",
            "Provider: parquet",
            "Num Buckets: " + numBuckets,
            "Bucket Columns: " + quotedList(bucketColumns),
            "Sort Columns: " + quotedList(sortColumns),
            "Schema: root",
            " |-- id: long (nullable = true)",
            " |-- region code: string (nullable = true)",
            "");

    ParsedTableExtended parsed =
        DatabricksHiveMetastoreTableParser.parseShowTableExtended("legacy_db", "bucketed", blob);

    assertEquals(numBuckets, parsed.numBuckets);
    assertEquals(bucketColumns, parsed.bucketColumns);
    assertEquals(sortColumns, parsed.sortColumns);
    assertEquals(2, parsed.columns.size());
  }

  @Test
  public void parseShowTableExtended_keepsMultiLineViewText() {
    ParsedTableExtended parsed =
        DatabricksHiveMetastoreTableParser.parseShowTableExtended(
            "legacy_db", "active_ids", viewBlob("view line one", VIEW_TEXT));

    assertEquals(VIEW_TEXT, parsed.viewDefinition);
    assertEquals("VIEW", parsed.tableType);
    assertEntriesInOrder(VIEW_TABLE_PROPERTIES, parsed.tableProperties);
    assertEquals(1, parsed.columns.size());
  }

  @Test
  public void parseShowTableExtended_parsesViewResolutionFields() {
    ParsedTableExtended parsed =
        DatabricksHiveMetastoreTableParser.parseShowTableExtended(
            "legacy_db", "active_ids", viewBlob("view line one", VIEW_TEXT));

    assertEquals(VIEW_CATALOG_AND_NAMESPACE, parsed.viewCatalogAndNamespace);
    assertEquals(VIEW_SCHEMA_MODE, parsed.viewSchemaMode);
    assertEquals(ImmutableList.of("id"), parsed.viewQueryOutputColumns);
    assertEquals("", parsed.sqlPath);
  }

  @Test
  public void parseShowTableExtended_keepsViewCommentLinesThatLookLikeKeys() {
    for (String comment :
        Arrays.asList("view line one\nOwner: not the owner", "Sales view\nLocation: building 5")) {
      ParsedTableExtended parsed =
          DatabricksHiveMetastoreTableParser.parseShowTableExtended(
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
          DatabricksHiveMetastoreTableParser.parseShowTableExtended(
              "legacy_db", "events", deltaTableBlob(comment, bracketed(DELTA_TABLE_PROPERTIES)));

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
        DatabricksHiveMetastoreTableParser.parseShowTableExtended(
            "legacy_db", "events", deltaTableBlob(comment, bracketed(DELTA_TABLE_PROPERTIES)));

    assertEquals(comment, parsed.comment);
    assertEntriesInOrder(DELTA_TABLE_PROPERTIES, parsed.tableProperties);
  }

  @Test
  public void parseShowTableExtended_keepsMultiLineCheckConstraint() {
    String checkConstraint = "a > 0\nAND b > 0";
    ImmutableMap<String, String> tableProperties =
        ImmutableMap.of(
            "delta.checkpoint.writeStatsAsJson", "false",
            "delta.constraints.positive", checkConstraint,
            "delta.enableDeletionVectors", "true");

    ParsedTableExtended parsed =
        DatabricksHiveMetastoreTableParser.parseShowTableExtended(
            "legacy_db", "events", deltaTableBlob("checked", bracketed(tableProperties)));

    assertEntriesInOrder(tableProperties, parsed.tableProperties);
    assertEquals(DELTA_LOCATION, parsed.storageLocation);
    assertEquals(1, parsed.constraints.size());
    assertEquals(checkConstraint, parsed.constraints.get(0).expression);
  }

  @Test
  public void parseShowTableExtended_keepsMultiLineStorageProperties() {
    ImmutableMap<String, String> storageProperties =
        ImmutableMap.of("line.delim", "\n", "field.delim", ",", "serialization.format", ",");
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
            "Storage Properties: " + bracketed(storageProperties),
            "Partition Provider: Catalog",
            "Schema: root",
            " |-- id: integer (nullable = true)",
            " |-- name: string (nullable = true)",
            "",
            "");

    ParsedTableExtended parsed =
        DatabricksHiveMetastoreTableParser.parseShowTableExtended("legacy_db", "csv_events", blob);

    assertEntriesInOrder(storageProperties, parsed.storageProperties);
    assertEquals(2, parsed.columns.size());
  }

  @Test
  public void parseShowTableExtended_skipsKeyLineOutOfSparkOrder() {
    String comment = "first line\nLocation: fake";
    String blob = deltaTableBlob(comment, bracketed(DELTA_TABLE_PROPERTIES)) + "Owner: late";

    ParsedTableExtended parsed =
        DatabricksHiveMetastoreTableParser.parseShowTableExtended("legacy_db", "events", blob);

    assertEquals(comment, parsed.comment);
    assertEquals(OWNER, parsed.owner);
    assertEntriesInOrder(DELTA_TABLE_PROPERTIES, parsed.tableProperties);
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
        DatabricksHiveMetastoreTableParser.parseShowTableExtended("legacy_db", "typed", blob);

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
  public void parseShowTableExtended_writesCreatedTimeAsInstant() {
    ParsedTableExtended parsed =
        DatabricksHiveMetastoreTableParser.parseShowTableExtended(
            "legacy_db", "events", deltaTableBlob("created", bracketed(DELTA_TABLE_PROPERTIES)));

    assertEquals(
        ZonedDateTime.of(2026, 9, 27, 21, 19, 25, 0, ZoneOffset.UTC).toInstant().toString(),
        parsed.createdAt);
  }

  @Test
  public void parseShowTableExtended_writesEmptyCreatedTimeInOtherFormat() {
    String blob =
        String.join(
            "\n",
            "Catalog: hive_metastore",
            "Database: legacy_db",
            "Table: events",
            "Created Time: 2026-09-27 21:19:25",
            "Type: MANAGED",
            "Schema: root",
            " |-- id: integer (nullable = true)",
            "");

    ParsedTableExtended parsed =
        DatabricksHiveMetastoreTableParser.parseShowTableExtended("legacy_db", "events", blob);

    assertEquals("", parsed.createdAt);
  }

  @Test
  public void formatDataSourceFormat_writesUnityCatalogFormatNames() {
    String customSource = "com.acme.spark.CustomSource";
    ImmutableMap<String, String> formatsByProvider =
        ImmutableMap.<String, String>builder()
            .put("delta", DataSourceFormat.DELTA.name())
            .put("hive", DataSourceFormat.HIVE.name())
            .put("Parquet", DataSourceFormat.PARQUET.name())
            .put("org.apache.spark.sql.parquet", DataSourceFormat.PARQUET.name())
            .put(
                "org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat",
                DataSourceFormat.PARQUET.name())
            .put("org.apache.spark.sql.json", DataSourceFormat.JSON.name())
            .put("org.apache.spark.sql.hive.orc", DataSourceFormat.ORC.name())
            .put("com.databricks.spark.csv", DataSourceFormat.CSV.name())
            .put("com.databricks.spark.avro", DataSourceFormat.AVRO.name())
            .put(
                "org.apache.spark.sql.execution.datasources.text.TextFileFormat",
                DataSourceFormat.TEXT.name())
            .put("org.apache.spark.sql.jdbc", "JDBC")
            .put("binaryFile", "BINARYFILE")
            .put(customSource, customSource)
            .put("", "")
            .build();

    for (Map.Entry<String, String> entry : formatsByProvider.entrySet()) {
      assertEquals(entry.getKey(), entry.getValue(), formatDataSourceFormat(entry.getKey()));
    }
  }

  @Test
  public void parseBracketedProperties_splitsEntriesOutsideParenthesesAndBrackets() {
    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            "a", "1",
            "check", "status IN ('new', 'done')",
            "list", "[x, y=z]",
            "empty", "",
            "equals", "k=v");

    assertEntriesInOrder(properties, parseBracketedProperties(bracketed(properties)));
  }

  @Test
  public void parseBracketedProperties_keepsEntryWithoutKey() {
    String entry = "no key";

    assertEntriesInOrder(ImmutableMap.of(entry, ""), parseBracketedProperties("[" + entry + "]"));
  }

  @Test
  public void parseBracketedProperties_returnsEmptyMapWithoutEntries() {
    for (String text : Arrays.asList("", " ", "[]")) {
      assertEquals(text, ImmutableMap.of(), parseBracketedProperties(text));
    }
  }

  @Test
  public void parseIdentifierList_unquotesIdentifiers() {
    ImmutableList<String> identifiers = ImmutableList.of("a", "b`c", "d, e");

    assertEquals(identifiers, parseIdentifierList(quotedList(identifiers)));
  }

  @Test
  public void parseIdentifierList_splitsUnquotedIdentifiers() {
    ImmutableList<String> identifiers = ImmutableList.of("a", "b c");

    assertEquals(identifiers, parseIdentifierList("[" + String.join(", ", identifiers) + "]"));
  }

  @Test
  public void parseIdentifierList_returnsEmptyListWithoutIdentifiers() {
    for (String text : Arrays.asList("", "[]")) {
      assertEquals(text, ImmutableList.of(), parseIdentifierList(text));
    }
  }

  @Test
  public void parseDescribeTableColumns_stopsAtPartitionInformation() {
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
    rows.add(Arrays.asList("dt", "date", "partition date"));

    ImmutableList.Builder<String> expected = ImmutableList.builder();
    for (List<String> row : columnRows) {
      expected.add(String.join("|", row.get(0), row.get(1), Strings.nullToEmpty(row.get(2))));
    }
    ImmutableList.Builder<String> actual = ImmutableList.builder();
    for (Map.Entry<String, DescribedColumn> column : parseDescribeTableColumns(rows).entrySet()) {
      actual.add(
          String.join("|", column.getKey(), column.getValue().dataType, column.getValue().comment));
    }
    assertEquals(expected.build(), actual.build());
  }

  @Test
  public void withDescribedColumns_replacesTypesThatDifferOnlyInLengthsAndAddsComments() {
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
            new ParsedColumn(0, "name", "string", "YES", "", ""),
            new ParsedColumn(1, "code", "string", "NO", "0", ""),
            new ParsedColumn(2, "s", "struct<a:string,b:int>", "YES", "", ""),
            new ParsedColumn(3, "arr", "array<string>", "YES", "", ""),
            new ParsedColumn(4, "m", "map<string,string>", "YES", "", ""),
            new ParsedColumn(5, "plain", "string", "YES", "", ""),
            new ParsedColumn(6, "w", wideTreeType, "YES", "", ""),
            new ParsedColumn(7, "n", "int", "YES", "", ""),
            new ParsedColumn(8, "added", "string", "YES", "", ""));
    ImmutableMap<String, DescribedColumn> describedColumns =
        ImmutableMap.<String, DescribedColumn>builder()
            .put("name", new DescribedColumn("varchar(20)", "customer's name"))
            .put("code", new DescribedColumn("char(3)", ""))
            .put("s", new DescribedColumn("struct<a:varchar(10),b:int>", "nested"))
            .put("arr", new DescribedColumn("array<char(2)>", ""))
            .put("m", new DescribedColumn("map<varchar(5),char(1)>", ""))
            .put("plain", new DescribedColumn("string", "plain text"))
            .put("w", new DescribedColumn(wideDescribedType, "wide"))
            .put("n", new DescribedColumn("varchar(3)", "changed type"))
            .build();
    ImmutableSet<String> columnsWithDescribedTypes =
        ImmutableSet.of("name", "code", "s", "arr", "m", "plain");

    ImmutableList<ParsedColumn> result = withDescribedColumns(columns, describedColumns);

    ImmutableList.Builder<String> expected = ImmutableList.builder();
    for (ParsedColumn column : columns) {
      DescribedColumn described = describedColumns.get(column.columnName);
      if (described == null) {
        expected.add(describeColumn(column));
        continue;
      }

      String dataType =
          columnsWithDescribedTypes.contains(column.columnName)
              ? described.dataType
              : column.dataType;
      expected.add(describeColumn(column.withDataTypeAndComment(dataType, described.comment)));
    }
    ImmutableList.Builder<String> actual = ImmutableList.builder();
    for (ParsedColumn column : result) {
      actual.add(describeColumn(column));
    }
    assertEquals(expected.build(), actual.build());
  }

  /**
   * Returns properties as {@code SHOW TABLE EXTENDED} prints them, such as {@code [k=v, k2=v2]}.
   */
  private static String bracketed(Map<String, String> properties) {
    List<String> entries = new ArrayList<>();
    for (Map.Entry<String, String> property : properties.entrySet()) {
      entries.add(property.getKey() + "=" + property.getValue());
    }
    return "[" + String.join(", ", entries) + "]";
  }

  /** Returns identifiers as {@code SHOW TABLE EXTENDED} prints them, such as {@code [`a`, `b`]}. */
  private static String quotedList(List<String> identifiers) {
    List<String> quoted = new ArrayList<>();
    for (String identifier : identifiers) {
      quoted.add("`" + identifier.replace("`", "``") + "`");
    }
    return "[" + String.join(", ", quoted) + "]";
  }

  /** Asserts that a map has the expected entries in the expected order. */
  private static void assertEntriesInOrder(
      Map<String, String> expected, Map<String, String> actual) {
    assertEquals(
        ImmutableList.copyOf(expected.entrySet()), ImmutableList.copyOf(actual.entrySet()));
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
        "View Schema Mode: " + VIEW_SCHEMA_MODE,
        "View Catalog and Namespace: " + VIEW_CATALOG_AND_NAMESPACE,
        "View Query Output Columns: [`id`]",
        "Table Properties: " + bracketed(VIEW_TABLE_PROPERTIES),
        "Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        "InputFormat: org.apache.hadoop.mapred.SequenceFileInputFormat",
        "OutputFormat: org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
        "Storage Properties: [serialization.format=1]",
        "Schema: root",
        " |-- id: integer (nullable = true)",
        "",
        "");
  }

  /** Returns all fields of a column, such as {@code 0|name|varchar(20)|YES||comment}. */
  private static String describeColumn(ParsedColumn column) {
    return String.join(
        "|",
        String.valueOf(column.ordinalPosition),
        column.columnName,
        column.dataType,
        column.isNullable,
        column.partitionIndex,
        column.comment);
  }
}
