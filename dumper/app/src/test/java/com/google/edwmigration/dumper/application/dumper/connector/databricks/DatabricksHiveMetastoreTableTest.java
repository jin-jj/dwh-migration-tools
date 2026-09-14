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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTable.Column;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksHiveMetastoreTableTest {

  private static String blob(String... lines) {
    return String.join("\n", lines);
  }

  private static List<String> renderedTypes(DatabricksHiveMetastoreTable table) {
    List<String> types = new ArrayList<>();
    for (Column column : table.columns()) {
      types.add(column.name() + " " + column.dataType());
    }
    return types;
  }

  @Test
  public void parse_readsScalarFields() {
    DatabricksHiveMetastoreTable table =
        DatabricksHiveMetastoreTable.parse(
            blob(
                "Catalog: hive_metastore",
                "Database: sales",
                "Table: orders",
                "Owner: alice@example.com",
                "Type: EXTERNAL",
                "Provider: delta",
                "Comment: the orders table",
                "Location: s3://bucket/orders"));

    assertEquals("sales", table.database());
    assertEquals("orders", table.name());
    assertEquals("alice@example.com", table.owner());
    assertEquals("EXTERNAL", table.type());
    assertEquals("delta", table.provider());
    assertEquals("the orders table", table.comment());
    assertEquals("s3://bucket/orders", table.location());
    assertFalse(table.isView());
  }

  @Test
  public void parse_readsCreatedTimeAsEpochMillis() throws Exception {
    String createdTime = "Mon Mar 03 14:05:06 UTC 2025";
    DatabricksHiveMetastoreTable table =
        DatabricksHiveMetastoreTable.parse(blob("Table: t", "Created Time: " + createdTime));

    long expected =
        new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ROOT)
            .parse(createdTime)
            .getTime();
    assertEquals(Long.valueOf(expected), table.createdAtMillis());
  }

  @Test
  public void parse_unparseableCreatedTime_yieldsNull() {
    DatabricksHiveMetastoreTable table =
        DatabricksHiveMetastoreTable.parse(blob("Table: t", "Created Time: UNKNOWN"));

    assertNull(table.createdAtMillis());
  }

  @Test
  public void parse_readsNullabilityFromTheSchemaTree() {
    DatabricksHiveMetastoreTable table =
        DatabricksHiveMetastoreTable.parse(
            blob(
                "Table: t",
                "Schema: root",
                " |-- required: long (nullable = false)",
                " |-- optional: string (nullable = true)"));

    assertEquals(2, table.columns().size());
    assertFalse(table.columns().get(0).nullable());
    assertTrue(table.columns().get(1).nullable());
  }

  @Test
  public void parse_rendersNestedTypes() {
    DatabricksHiveMetastoreTable table =
        DatabricksHiveMetastoreTable.parse(
            blob(
                "Table: t",
                "Schema: root",
                " |-- id: long (nullable = true)",
                " |-- price: decimal(10,2) (nullable = true)",
                " |-- tags: array (nullable = true)",
                " |    |-- element: string (containsNull = true)",
                " |-- attributes: map (nullable = true)",
                " |    |-- key: string",
                " |    |-- value: integer (valueContainsNull = true)",
                " |-- address: struct (nullable = true)",
                " |    |-- city: string (nullable = true)",
                " |    |-- geo: struct (nullable = true)",
                " |    |    |-- lat: double (nullable = true)",
                " |    |    |-- lon: double (nullable = true)"));

    assertEquals(
        ImmutableList.of(
            "id BIGINT",
            "price DECIMAL(10,2)",
            "tags ARRAY<STRING>",
            "attributes MAP<STRING, INT>",
            "address STRUCT<city: STRING, geo: STRUCT<lat: DOUBLE, lon: DOUBLE>>"),
        renderedTypes(table));
  }

  @Test
  public void parse_readsPartitionColumnPositions() {
    DatabricksHiveMetastoreTable table =
        DatabricksHiveMetastoreTable.parse(
            blob(
                "Table: t",
                "Partition Columns: [`year`, `month`]",
                "Schema: root",
                " |-- id: long (nullable = true)",
                " |-- year: integer (nullable = true)",
                " |-- month: integer (nullable = true)"));

    assertNull(table.partitionIndexOf("id"));
    assertEquals(Integer.valueOf(1), table.partitionIndexOf("year"));
    assertEquals(Integer.valueOf(2), table.partitionIndexOf("month"));
  }

  @Test
  public void parse_readsMultiLineViewText() {
    DatabricksHiveMetastoreTable table =
        DatabricksHiveMetastoreTable.parse(
            blob(
                "Table: v",
                "Type: VIEW",
                "View Text: SELECT id,",
                "       name",
                "FROM sales.orders",
                "Schema: root",
                " |-- id: long (nullable = true)"));

    assertTrue(table.isView());
    assertEquals("SELECT id,\n       name\nFROM sales.orders", table.viewText());
    // The Schema field must still be recognised after the multi-line value.
    assertEquals(1, table.columns().size());
  }

  @Test
  public void parse_emptyBlob_yieldsNoColumns() {
    DatabricksHiveMetastoreTable table = DatabricksHiveMetastoreTable.parse("");

    assertTrue(table.columns().isEmpty());
    assertNull(table.name());
  }

  /**
   * Captured verbatim from {@code SHOW TABLE EXTENDED IN jinjj_test LIKE '*'} against a real Azure
   * Databricks {@code hive_metastore}, so that the parser is pinned to what Spark actually emits
   * rather than to what this test would otherwise have invented.
   */
  private static final String PRODUCTION_BLOB =
      blob(
          "Catalog: hive_metastore",
          "Database: jinjj_test",
          "Table: tbl_advanced_076",
          "Owner: jinjj@google.com",
          "Created Time: Mon Sep 14 16:19:01 UTC 2026",
          "Last Access: UNKNOWN",
          "Created By: Spark 4.2.0",
          "Type: MANAGED",
          "Provider: delta",
          "Comment: Table #76 with Identity columns, Generated columns, and Change Data Feed",
          "Table Properties: [delta.deletedFileRetentionDuration=interval 14 days,"
              + " delta.enableChangeDataFeed=true, delta.enableDeletionVectors=true]",
          "Location: dbfs:/user/hive/warehouse/jinjj_test.db/tbl_advanced_076",
          "Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
          "InputFormat: org.apache.hadoop.mapred.SequenceFileInputFormat",
          "OutputFormat: org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
          "Partition Provider: Catalog",
          "Schema: root",
          " |-- row_id: long (nullable = true)",
          " |-- base_price: decimal(10,2) (nullable = false)",
          " |-- tax_pct: decimal(4,2) (nullable = false)",
          " |-- total_amount: decimal(10,2) (nullable = true)",
          " |-- created_at: timestamp (nullable = true)",
          " |-- created_date: date (nullable = true)",
          " |-- tags: array (nullable = true)",
          " |    |-- element: integer (containsNull = true)",
          " |-- audit_info: struct (nullable = true)",
          " |    |-- name: string (nullable = true)",
          " |    |-- phone: decimal(10,0) (nullable = true)",
          "");

  @Test
  public void parse_productionBlob_readsEveryScalarField() {
    DatabricksHiveMetastoreTable table = DatabricksHiveMetastoreTable.parse(PRODUCTION_BLOB);

    assertEquals("jinjj_test", table.database());
    assertEquals("tbl_advanced_076", table.name());
    assertEquals("jinjj@google.com", table.owner());
    assertEquals("MANAGED", table.type());
    assertEquals("delta", table.provider());
    assertEquals(
        "Table #76 with Identity columns, Generated columns, and Change Data Feed",
        table.comment());
    assertEquals("dbfs:/user/hive/warehouse/jinjj_test.db/tbl_advanced_076", table.location());
    assertFalse(table.isView());
    assertNull(table.viewText());
  }

  @Test
  public void parse_productionBlob_readsTheCreationTime() throws Exception {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z", Locale.US);
    long expected = format.parse("2026-09-14 16:19:01 UTC").getTime();

    assertEquals(
        Long.valueOf(expected),
        DatabricksHiveMetastoreTable.parse(PRODUCTION_BLOB).createdAtMillis());
  }

  /**
   * The nested {@code array} and {@code struct} columns and the parameterised {@code decimal} types
   * are the parts of the tree that a naive line-by-line reader gets wrong.
   */
  @Test
  public void parse_productionBlob_rendersNestedAndParameterisedTypes() {
    DatabricksHiveMetastoreTable table = DatabricksHiveMetastoreTable.parse(PRODUCTION_BLOB);

    assertEquals(
        ImmutableList.of(
            "row_id BIGINT",
            "base_price DECIMAL(10,2)",
            "tax_pct DECIMAL(4,2)",
            "total_amount DECIMAL(10,2)",
            "created_at TIMESTAMP",
            "created_date DATE",
            "tags ARRAY<INT>",
            "audit_info STRUCT<name: STRING, phone: DECIMAL(10,0)>"),
        renderedTypes(table));
  }

  @Test
  public void parse_productionBlob_readsColumnNullability() {
    DatabricksHiveMetastoreTable table = DatabricksHiveMetastoreTable.parse(PRODUCTION_BLOB);
    List<String> nullability = new ArrayList<>();
    for (Column column : table.columns()) {
      nullability.add(column.name() + "=" + column.nullable());
    }

    assertEquals(
        ImmutableList.of(
            "row_id=true",
            "base_price=false",
            "tax_pct=false",
            "total_amount=true",
            "created_at=true",
            "created_date=true",
            "tags=true",
            "audit_info=true"),
        nullability);
  }

  /** An unpartitioned table has no {@code Partition Columns} line at all. */
  @Test
  public void parse_productionBlob_reportsNoPartitionColumns() {
    DatabricksHiveMetastoreTable table = DatabricksHiveMetastoreTable.parse(PRODUCTION_BLOB);

    assertNull(table.partitionIndexOf("row_id"));
    assertNull(table.partitionIndexOf("created_date"));
  }
}
