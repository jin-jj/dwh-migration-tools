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

import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.ParsedTableExtended;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksHiveMetastoreTableParserTest {

  private static final String OWNER = "data_eng";
  private static final String DELTA_LOCATION = "dbfs:/user/hive/warehouse/legacy_db.db/events";
  private static final String VIEW_TEXT =
      "SELECT id\nFROM legacy_db.events\nWHERE name:a IS NOT NULL";
  private static final String VIEW_TABLE_PROPERTIES = "[transient_lastDdlTime=1790543963]";

  @Test
  public void parseShowTableExtended_parsesAttributesAndColumns() {
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
        DatabricksHiveMetastoreTableParser.parseShowTableExtended("legacy_db", "events", blob);
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
  }

  @Test
  public void parseShowTableExtended_parsesDeltaCheckConstraints() {
    String tableProperties =
        "[delta.checkpoint.writeStatsAsJson=false, "
            + "delta.constraints.valid_event_type=event_type IN ('click', 'view', 'purchase'), "
            + "delta.enableDeletionVectors=true]";

    ParsedTableExtended parsed =
        DatabricksHiveMetastoreTableParser.parseShowTableExtended(
            "legacy_db", "events", deltaTableBlob("checked", tableProperties));

    assertEquals(1, parsed.constraints.size());
    assertEquals("valid_event_type", parsed.constraints.get(0).constraintName);
    assertEquals("CHECK", parsed.constraints.get(0).constraintType);
    assertEquals(
        "event_type IN ('click', 'view', 'purchase')", parsed.constraints.get(0).constraintDetails);
  }

  @Test
  public void parseShowTableExtended_keepsMultiLineViewText() {
    ParsedTableExtended parsed =
        DatabricksHiveMetastoreTableParser.parseShowTableExtended(
            "legacy_db", "active_ids", viewBlob("view line one", VIEW_TEXT));

    assertEquals(VIEW_TEXT, parsed.viewDefinition);
    assertEquals("VIEW", parsed.tableType);
    assertEquals(VIEW_TABLE_PROPERTIES, parsed.tableProperties);
    assertEquals(1, parsed.columns.size());
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
}
