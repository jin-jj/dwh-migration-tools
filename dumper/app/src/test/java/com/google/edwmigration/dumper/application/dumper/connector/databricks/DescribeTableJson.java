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

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksFilter.HIVE_METASTORE_CATALOG;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.succeeded;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.succeededRows;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.succeededWithColumns;

import com.databricks.sdk.service.sql.StatementResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Builds the {@code DESCRIBE TABLE EXTENDED ... AS JSON} output of {@code hive_metastore} tables
 * and views as Databricks returns it, and the responses of the statements that list and describe
 * them.
 */
final class DescribeTableJson {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** The first columns of {@code DESCRIBE DETAIL}, in the order that Databricks returns them. */
  private static final ImmutableList<String> DESCRIBE_DETAIL_COLUMNS =
      ImmutableList.of(
          "format",
          "id",
          "name",
          "description",
          "location",
          "createdAt",
          "lastModified",
          "partitionColumns",
          "clusteringColumns",
          "numFiles");

  private DescribeTableJson() {}

  /** Returns the output of a table with its type, such as {@code MANAGED}, and no columns. */
  static ObjectNode table(String schemaName, String tableName, String type, String provider) {
    ObjectNode table = relation(schemaName, tableName, type);
    table.put("provider", provider);
    return table;
  }

  /** Returns the output of a managed Delta table with a protocol and no columns. */
  static ObjectNode deltaTable(String schemaName, String tableName, int minWriterVersion) {
    ObjectNode table = table(schemaName, tableName, "MANAGED", "delta");
    table
        .putObject("table_properties")
        .put("delta.minReaderVersion", "1")
        .put("delta.minWriterVersion", String.valueOf(minWriterVersion));
    return table;
  }

  /** Returns the output of a view with its text and no columns. */
  static ObjectNode view(String schemaName, String viewName, String viewText) {
    ObjectNode view = relation(schemaName, viewName, "VIEW");
    view.put("view_text", viewText);
    return view;
  }

  /** Adds a column to a table or view, and returns it so that a comment or default can be set. */
  static ObjectNode addColumn(ObjectNode table, String name, JsonNode type, boolean nullable) {
    ObjectNode column = ((ArrayNode) table.get("columns")).addObject();
    column.put("name", name);
    column.set("type", type);
    column.put("nullable", nullable);
    return column;
  }

  /** Returns a type without parameters, such as {@code {"name": "int"}}. */
  static ObjectNode type(String name) {
    return MAPPER.createObjectNode().put("name", name);
  }

  static ObjectNode decimalType(int precision, int scale) {
    return type("decimal").put("precision", precision).put("scale", scale);
  }

  /** Returns a {@code char} or {@code varchar} type with its length. */
  static ObjectNode lengthType(String name, int length) {
    return type(name).put("length", length);
  }

  static ObjectNode arrayType(JsonNode elementType) {
    ObjectNode type = type("array");
    type.set("element_type", elementType);
    type.put("element_nullable", true);
    return type;
  }

  static ObjectNode mapType(JsonNode keyType, JsonNode valueType) {
    ObjectNode type = type("map");
    type.set("key_type", keyType);
    type.set("value_type", valueType);
    type.put("value_nullable", true);
    return type;
  }

  /** Returns a {@code struct} type with fields in the order of {@code fieldTypes}. */
  static ObjectNode structType(Map<String, JsonNode> fieldTypes) {
    ObjectNode type = type("struct");
    ArrayNode fields = type.putArray("fields");
    for (Map.Entry<String, JsonNode> fieldType : fieldTypes.entrySet()) {
      ObjectNode field = fields.addObject();
      field.put("name", fieldType.getKey());
      field.set("type", fieldType.getValue());
      field.put("nullable", true);
    }
    return type;
  }

  /** Puts an object of string values, such as {@code table_properties}, into a table. */
  static void putObject(ObjectNode table, String key, Map<String, String> values) {
    ObjectNode object = table.putObject(key);
    for (Map.Entry<String, String> value : values.entrySet()) {
      object.put(value.getKey(), value.getValue());
    }
  }

  /** Puts a list of strings, such as {@code partition_columns}, into a table. */
  static void putList(ObjectNode table, String key, List<String> values) {
    ArrayNode list = table.putArray(key);
    for (String value : values) {
      list.add(value);
    }
  }

  static String showTablesStatement(String schemaName) {
    return "SHOW TABLES IN `" + schemaName + "`";
  }

  /** Returns the {@code SHOW TABLES} response that lists tables and views of a schema. */
  static StatementResponse showTablesResponse(String schemaName, String... tableNames) {
    List<List<String>> rows = new ArrayList<>();
    for (String tableName : tableNames) {
      rows.add(ImmutableList.of(schemaName, tableName, "false"));
    }
    return succeededRows(rows);
  }

  static String describeStatement(String schemaName, String tableName) {
    return "DESCRIBE TABLE EXTENDED `" + schemaName + "`.`" + tableName + "` AS JSON";
  }

  /** Returns the response of {@code DESCRIBE TABLE ... AS JSON}: one cell with the output. */
  static StatementResponse describeResponse(JsonNode output) {
    return succeeded(output.toString());
  }

  static String describeDetailStatement(String schemaName, String tableName) {
    return "DESCRIBE DETAIL `" + schemaName + "`.`" + tableName + "`";
  }

  /** Returns the {@code DESCRIBE DETAIL} response of a Delta table with its partition columns. */
  static StatementResponse describeDetailResponse(
      String schemaName, String tableName, List<String> partitionColumns) {
    ArrayNode partitionColumnNames = MAPPER.createArrayNode();
    for (String partitionColumn : partitionColumns) {
      partitionColumnNames.add(partitionColumn);
    }
    List<String> row =
        Arrays.asList(
            "delta",
            "f18abf40-0000-0000-0000-000000000000",
            HIVE_METASTORE_CATALOG + "." + schemaName + "." + tableName,
            null,
            "dbfs:/user/hive/warehouse/" + schemaName + ".db/" + tableName,
            "2026-09-30T14:37:02.249Z",
            "2026-09-30T14:37:02.000Z",
            partitionColumnNames.toString(),
            "[]",
            "0");
    return succeededWithColumns(DESCRIBE_DETAIL_COLUMNS, ImmutableList.of(row));
  }

  /**
   * Returns the responses that list a schema and its tables and views, and describe each of them
   * with its {@code DESCRIBE TABLE ... AS JSON} output, in order. Each Delta table gets a {@code
   * DESCRIBE DETAIL} response without partition columns.
   */
  static ImmutableMap.Builder<String, StatementResponse> responsesFor(
      String schemaName, List<? extends JsonNode> outputs) {
    ImmutableMap.Builder<String, StatementResponse> responses = ImmutableMap.builder();
    List<String> tableNames = new ArrayList<>();
    for (JsonNode output : outputs) {
      String tableName = output.get("table_name").asText();
      tableNames.add(tableName);
      responses.put(describeStatement(schemaName, tableName), describeResponse(output));
      if ("delta".equals(output.path("provider").asText())) {
        responses.put(
            describeDetailStatement(schemaName, tableName),
            describeDetailResponse(schemaName, tableName, ImmutableList.of()));
      }
    }
    responses.put("SHOW SCHEMAS", succeeded(schemaName));
    responses.put(
        showTablesStatement(schemaName),
        showTablesResponse(schemaName, tableNames.toArray(new String[0])));
    return responses;
  }

  private static ObjectNode relation(String schemaName, String tableName, String type) {
    ObjectNode relation = MAPPER.createObjectNode();
    relation.put("table_name", tableName);
    relation.put("catalog_name", HIVE_METASTORE_CATALOG);
    relation.put("schema_name", schemaName);
    relation.putArray("namespace").add(schemaName);
    relation.put("type", type);
    relation.putArray("columns");
    return relation;
  }
}
