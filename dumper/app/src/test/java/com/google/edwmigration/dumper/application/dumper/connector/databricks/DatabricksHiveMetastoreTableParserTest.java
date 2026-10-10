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
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.formatDataType;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.parseDeltaPartitionColumns;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.parseDescribeTableJson;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.undescribedTable;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.addColumn;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.arrayType;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.decimalType;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.deltaTable;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.lengthType;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.mapType;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.putList;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.putObject;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.structType;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.table;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.type;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.view;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.databricks.sdk.service.catalog.DataSourceFormat;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.ParsedColumn;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.ParsedTable;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksHiveMetastoreTableParserTest {

  private static final String SCHEMA = "legacy_db";

  @Test
  public void parseDescribeTableJson_parsesTableAttributes() throws IOException {
    String serdeLibrary = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
    String inputFormat = "org.apache.hadoop.mapred.TextInputFormat";
    String outputFormat = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
    ImmutableMap<String, String> storageProperties =
        ImmutableMap.of("serialization.format", ",", "field.delim", ",");
    ImmutableMap<String, String> tableProperties =
        ImmutableMap.of("transient_lastDdlTime", "1790541380", "owner_team", "data");
    ObjectNode output = table(SCHEMA, "events", "EXTERNAL", "hive");
    output.put("location", "s3://legacy-bucket/events");
    output.put("comment", "Legacy events");
    output.put("owner", "data_eng");
    output.put("serde_library", serdeLibrary);
    output.put("input_format", inputFormat);
    output.put("output_format", outputFormat);
    putObject(output, "storage_properties", storageProperties);
    putObject(output, "table_properties", tableProperties);

    ParsedTable parsed = parseDescribeTableJson(SCHEMA, "events", output.toString());

    assertTrue(parsed.described);
    assertEquals(SCHEMA, parsed.schemaName);
    assertEquals("events", parsed.tableName);
    assertEquals("EXTERNAL", parsed.tableType);
    assertEquals(DataSourceFormat.HIVE.name(), parsed.dataSourceFormat);
    assertEquals("s3://legacy-bucket/events", parsed.storageLocation);
    assertEquals("Legacy events", parsed.comment);
    assertEquals("data_eng", parsed.owner);
    assertEquals(serdeLibrary, parsed.serdeLibrary);
    assertEquals(inputFormat, parsed.inputFormat);
    assertEquals(outputFormat, parsed.outputFormat);
    assertEntriesInOrder(storageProperties, parsed.storageProperties);
    assertEntriesInOrder(tableProperties, parsed.tableProperties);
    assertNull(parsed.viewDefinition);
  }

  @Test
  public void parseDescribeTableJson_parsesBucketing() throws IOException {
    int numBuckets = 8;
    ImmutableList<String> bucketColumns = ImmutableList.of("id", "region");
    ImmutableList<String> sortColumns = ImmutableList.of("id");
    ObjectNode output = table(SCHEMA, "bucketed", "MANAGED", "parquet");
    output.put("num_buckets", numBuckets);
    putList(output, "bucket_columns", bucketColumns);
    putList(output, "sort_columns", sortColumns);

    ParsedTable parsed = parseDescribeTableJson(SCHEMA, "bucketed", output.toString());

    assertEquals(String.valueOf(numBuckets), parsed.numBuckets);
    assertEquals(bucketColumns, parsed.bucketColumns);
    assertEquals(sortColumns, parsed.sortColumns);
  }

  @Test
  public void parseDescribeTableJson_parsesColumnsWithPartitionIndexes() throws IOException {
    ObjectNode output = table(SCHEMA, "events", "MANAGED", "delta");
    addColumn(output, "event_id", type("string"), false).put("comment", "it's the id");
    addColumn(output, "status", type("string"), true).put("default", "'new'");
    addColumn(output, "dt", type("date"), true);
    addColumn(output, "Country", type("string"), true);
    // The partitioning order differs from the column order and the case of a column name.
    putList(output, "partition_columns", ImmutableList.of("country", "dt"));

    ParsedTable parsed = parseDescribeTableJson(SCHEMA, "events", output.toString());

    assertEquals(
        ImmutableList.of(
            "0|event_id|string|NO||it's the id|",
            "1|status|string|YES|||'new'",
            "2|dt|date|YES|1||",
            "3|Country|string|YES|0||"),
        describe(parsed.columns));
  }

  @Test
  public void parseDescribeTableJson_leavesOnlyThePartitioningOfDeltaTablesUnknown()
      throws IOException {
    ImmutableMap<ObjectNode, Boolean> partitioningKnownByOutput =
        ImmutableMap.of(
            deltaTable(SCHEMA, "events", 2), false,
            table(SCHEMA, "files", "EXTERNAL", "parquet"), true,
            view(SCHEMA, "event_ids", "SELECT 1"), true);

    for (Map.Entry<ObjectNode, Boolean> output : partitioningKnownByOutput.entrySet()) {
      ParsedTable parsed =
          parseDescribeTableJson(
              SCHEMA, output.getKey().get("table_name").asText(), output.getKey().toString());

      assertEquals(parsed.tableName, output.getValue(), parsed.partitioningKnown);
    }
  }

  @Test
  public void withPartitionColumns_setsPartitionIndexesAndKeepsOtherValues() throws IOException {
    ObjectNode output = deltaTable(SCHEMA, "events", 2);
    output.put("owner", "data_eng");
    ((ObjectNode) output.get("table_properties")).put("delta.constraints.positive", "id > 0");
    addColumn(output, "id", type("long"), false).put("comment", "event id");
    addColumn(output, "Region", type("string"), true).put("default", "'eu'");
    addColumn(output, "dt", type("date"), true);
    ParsedTable described = parseDescribeTableJson(SCHEMA, "events", output.toString());

    ParsedTable partitioned = described.withPartitionColumns(ImmutableList.of("dt", "region"));

    assertTrue(partitioned.partitioningKnown);
    assertEquals(
        ImmutableList.of(
            "0|id|bigint|NO||event id|", "1|Region|string|YES|1||'eu'", "2|dt|date|YES|0||"),
        describe(partitioned.columns));
    assertEquals(described.owner, partitioned.owner);
    assertEquals(described.tableProperties, partitioned.tableProperties);
    assertEquals(1, partitioned.constraints.size());
    assertTrue(partitioned.isDelta());
  }

  @Test
  public void parseDeltaPartitionColumns_parsesJsonArrayOfNames() throws IOException {
    assertEquals(
        ImmutableList.of("region", "dt"), parseDeltaPartitionColumns("[\"region\",\"dt\"]"));
    assertEquals(ImmutableList.of(), parseDeltaPartitionColumns("[]"));
  }

  @Test
  public void parseDeltaPartitionColumns_throwsForOtherValues() {
    for (String partitionColumns : Arrays.asList(null, "", "region", "{}", "[1]", "[null]")) {
      assertThrows(
          String.valueOf(partitionColumns),
          IOException.class,
          () -> parseDeltaPartitionColumns(partitionColumns));
    }
  }

  @Test
  public void parseDescribeTableJson_parsesDeltaCheckConstraintsWithoutSplittingThem()
      throws IOException {
    String constraintName = "valid_status";
    String expression = "status IN ('new', 'done') OR note = 'x, y=z'";
    ObjectNode output = table(SCHEMA, "checked", "MANAGED", "delta");
    putObject(
        output,
        "table_properties",
        ImmutableMap.of(
            "delta.constraints." + constraintName, expression, "delta.minWriterVersion", "3"));

    ParsedTable parsed = parseDescribeTableJson(SCHEMA, "checked", output.toString());

    assertEquals(1, parsed.constraints.size());
    assertEquals(constraintName, parsed.constraints.get(0).constraintName);
    assertEquals("CHECK", parsed.constraints.get(0).constraintType);
    assertEquals(expression, parsed.constraints.get(0).expression);
  }

  @Test
  public void parseDescribeTableJson_parsesViewFields() throws IOException {
    String viewText = "SELECT id\nFROM legacy_db.events\nWHERE name:a IS NOT NULL";
    String viewCatalogAndNamespace = "spark_catalog.default";
    ImmutableList<String> viewQueryOutputColumns = ImmutableList.of("id");
    String viewSchemaMode = "COMPENSATION";
    ObjectNode output = view(SCHEMA, "event_ids", viewText);
    output.put("view_catalog_and_namespace", viewCatalogAndNamespace);
    putList(output, "view_query_output_columns", viewQueryOutputColumns);
    output.put("view_schema_mode", viewSchemaMode);

    ParsedTable parsed = parseDescribeTableJson(SCHEMA, "event_ids", output.toString());

    assertEquals("VIEW", parsed.tableType);
    assertEquals("", parsed.dataSourceFormat);
    assertEquals(viewText, parsed.viewDefinition);
    assertEquals(viewCatalogAndNamespace, parsed.viewCatalogAndNamespace);
    assertEquals(viewQueryOutputColumns, parsed.viewQueryOutputColumns);
    assertEquals(viewSchemaMode, parsed.viewSchemaMode);
  }

  @Test
  public void parseDescribeTableJson_writesOtherValuesAsTextAndNestedValuesAsJson()
      throws IOException {
    ObjectNode output = view(SCHEMA, "event_ids", "SELECT 1");
    ArrayNode sqlPath = output.putArray("sql_path");
    sqlPath.addObject().put("catalog", "spark_catalog").put("schema", "default");
    output.putObject("table_properties").put("retention_days", 7).putNull("dropped");

    ParsedTable parsed = parseDescribeTableJson(SCHEMA, "event_ids", output.toString());

    assertEquals(sqlPath.toString(), parsed.sqlPath);
    assertEntriesInOrder(
        ImmutableMap.of("retention_days", "7", "dropped", ""), parsed.tableProperties);
  }

  @Test
  public void parseDescribeTableJson_writesCreatedTimeAsInstant() throws IOException {
    OffsetDateTime createdTime =
        OffsetDateTime.of(2026, 9, 14, 18, 19, 4, 0, ZoneOffset.ofHours(2));
    String expected = createdTime.toInstant().toString();

    for (String printedTime : Arrays.asList(expected, createdTime.toString())) {
      ObjectNode output = table(SCHEMA, "events", "MANAGED", "delta");
      output.put("created_time", printedTime);

      assertEquals(
          printedTime,
          expected,
          parseDescribeTableJson(SCHEMA, "events", output.toString()).createdAt);
    }
  }

  @Test
  public void parseDescribeTableJson_writesEmptyCreatedTimeInOtherFormat() throws IOException {
    ObjectNode output = table(SCHEMA, "events", "MANAGED", "delta");
    output.put("created_time", "Sun Sep 27 21:19:25 UTC 2026");

    assertEquals("", parseDescribeTableJson(SCHEMA, "events", output.toString()).createdAt);
  }

  @Test
  public void parseDescribeTableJson_throwsForOutputThatIsNotJsonObject() {
    for (String json : Arrays.asList(null, "", "null", "[]", "\"events\"", "{\"columns\": [")) {
      assertThrows(
          String.valueOf(json),
          IOException.class,
          () -> parseDescribeTableJson(SCHEMA, "events", json));
    }
  }

  @Test
  public void undescribedTable_hasOnlyNames() {
    ParsedTable table = undescribedTable(SCHEMA, "locked");

    assertFalse(table.described);
    assertEquals(SCHEMA, table.schemaName);
    assertEquals("locked", table.tableName);
    assertEquals("", table.tableType);
    assertEquals("", table.dataSourceFormat);
    assertEquals("", table.createdAt);
    assertTrue(table.tableProperties.isEmpty());
    assertTrue(table.columns.isEmpty());
    assertTrue(table.constraints.isEmpty());
    assertNull(table.viewDefinition);
    assertFalse(table.partitioningKnown);
  }

  @Test
  public void formatDataType_writesTypesAsUnityCatalogTypeText() {
    ImmutableMap<String, JsonNode> structFields =
        ImmutableMap.of(
            "amount", decimalType(4, 2), "tags", mapType(type("string"), arrayType(type("int"))));
    List<Map.Entry<JsonNode, String>> typeTexts =
        ImmutableList.of(
            Maps.immutableEntry(type("int"), "int"),
            Maps.immutableEntry(type("integer"), "int"),
            Maps.immutableEntry(type("long"), "bigint"),
            Maps.immutableEntry(type("short"), "smallint"),
            Maps.immutableEntry(type("byte"), "tinyint"),
            Maps.immutableEntry(type("BIGINT"), "bigint"),
            Maps.immutableEntry(type("timestamp_ltz"), "timestamp"),
            Maps.immutableEntry(type("timestamp_ntz"), "timestamp_ntz"),
            Maps.immutableEntry(decimalType(10, 2), "decimal(10,2)"),
            Maps.immutableEntry(lengthType("varchar", 20), "varchar(20)"),
            Maps.immutableEntry(lengthType("char", 3), "char(3)"),
            Maps.immutableEntry(type("string"), "string"),
            Maps.immutableEntry(type("string").put("collation", "UTF8_BINARY"), "string"),
            Maps.immutableEntry(
                type("string").put("collation", "UTF8_LCASE"), "string collate UTF8_LCASE"),
            Maps.immutableEntry(
                type("interval").put("start_unit", "day").put("end_unit", "second"),
                "interval day to second"),
            Maps.immutableEntry(
                type("interval").put("start_unit", "year").put("end_unit", "year"),
                "interval year"),
            Maps.immutableEntry(arrayType(type("integer")), "array<int>"),
            Maps.immutableEntry(mapType(type("string"), type("long")), "map<string,bigint>"),
            Maps.immutableEntry(
                arrayType(structType(structFields)),
                "array<struct<amount:decimal(4,2),tags:map<string,array<int>>>>"));

    for (Map.Entry<JsonNode, String> typeText : typeTexts) {
      assertEquals(
          typeText.getKey().toString(), typeText.getValue(), formatDataType(typeText.getKey()));
    }
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

  /** Returns each column as {@code position|name|type|nullable|partitionIndex|comment|default}. */
  private static ImmutableList<String> describe(List<ParsedColumn> columns) {
    ImmutableList.Builder<String> described = ImmutableList.builder();
    for (ParsedColumn column : columns) {
      described.add(
          String.join(
              "|",
              String.valueOf(column.ordinalPosition),
              column.columnName,
              column.dataType,
              column.isNullable,
              column.partitionIndex,
              column.comment,
              column.defaultExpression));
    }
    return described.build();
  }

  private static void assertEntriesInOrder(
      Map<String, String> expected, Map<String, String> actual) {
    assertEquals(
        ImmutableList.copyOf(expected.entrySet()), ImmutableList.copyOf(actual.entrySet()));
  }
}
