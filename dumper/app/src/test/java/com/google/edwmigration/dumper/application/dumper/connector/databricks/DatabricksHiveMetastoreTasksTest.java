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
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.parseSchemaProperties;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.acceptAllFilter;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.failed;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.mockStatements;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.newHandle;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.succeeded;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.succeededRows;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.addColumn;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.arrayType;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.deltaTable;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.describeDetailResponse;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.describeDetailStatement;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.describeResponse;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.describeStatement;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.lengthType;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.putList;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.putObject;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.showTablesResponse;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.showTablesStatement;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.structType;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.table;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.type;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.view;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.StatementExecutionAPI;
import com.databricks.sdk.service.sql.StatementResponse;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDdlTasks.DatabricksHiveMetastoreDdlsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreCatalogsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreColumnsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreSchemataTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreTableConstraintsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreTablesTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreViewsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.HiveMetastoreSessionCache;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.CatalogsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.ColumnsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.DdlsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.SchemataHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.TableConstraintsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.TablesHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.ViewsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.TaskBody;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksHiveMetastoreTasksTest {

  private static final String SCHEMA = "legacy";
  private static final String SHOW_SCHEMAS = "SHOW SCHEMAS";
  private static final String SHOW_TABLES = showTablesStatement(SCHEMA);
  private static final String INSUFFICIENT_PERMISSIONS =
      "[INSUFFICIENT_PERMISSIONS] Insufficient privileges.";
  private static final ObjectMapper JSON = new ObjectMapper();

  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void catalogsTask_writesHiveMetastoreWithoutRunningStatements() throws Exception {
    StatementExecutionAPI statements = mockStatements(ImmutableMap.of());
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreCatalogsTask(cache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    CSVRecord record = records.get(0);
    for (CatalogsHeader column : CatalogsHeader.values()) {
      if (column == CatalogsHeader.CatalogName) {
        assertEquals(HIVE_METASTORE_CATALOG, record.get(column));
      } else {
        assertEquals(column.name(), "", record.get(column));
      }
    }
    verify(statements, never()).executeStatement(any(ExecuteStatementRequest.class));
  }

  @Test
  public void schemataTask_writesDescription() throws Exception {
    String location = "dbfs:/mnt/lake/legacy";
    ImmutableMap<String, String> properties = ImmutableMap.of("purpose", "old", "team", "lake");
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                "DESCRIBE SCHEMA EXTENDED `legacy`",
                succeededRows(
                    ImmutableList.of(
                        ImmutableList.of("Catalog Name", HIVE_METASTORE_CATALOG),
                        ImmutableList.of("Namespace Name", SCHEMA),
                        ImmutableList.of("Comment", "Old data"),
                        ImmutableList.of("Location", location),
                        ImmutableList.of("Owner", "alice"),
                        ImmutableList.of("Properties", schemaPropertiesText(properties))))));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreSchemataTask(cache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    CSVRecord record = records.get(0);
    assertEquals(HIVE_METASTORE_CATALOG, record.get(SchemataHeader.CatalogName));
    assertEquals(SCHEMA, record.get(SchemataHeader.SchemaName));
    assertEquals("Old data", record.get(SchemataHeader.Comment));
    assertEquals("alice", record.get(SchemataHeader.Owner));
    assertEquals("", record.get(SchemataHeader.StorageRoot));
    assertEquals(location, record.get(SchemataHeader.StorageLocation));
    assertEquals(properties, parseJsonObject(record.get(SchemataHeader.Properties)));
  }

  @Test
  public void schemataTask_keepsSchemaWhoseDescribeFails() throws Exception {
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA, "locked"),
                "DESCRIBE SCHEMA EXTENDED `legacy`",
                succeededRows(
                    ImmutableList.of(
                        ImmutableList.of("Comment", "Old data"),
                        ImmutableList.of("Owner", "alice"))),
                "DESCRIBE SCHEMA EXTENDED `locked`",
                failed(INSUFFICIENT_PERMISSIONS)));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreSchemataTask(cache).doRun(null, sink, handle));

    assertEquals(2, records.size());
    assertEquals("Old data", records.get(0).get(SchemataHeader.Comment));
    assertEquals("alice", records.get(0).get(SchemataHeader.Owner));
    assertEquals("", records.get(0).get(SchemataHeader.Properties));
    assertEquals("locked", records.get(1).get(SchemataHeader.SchemaName));
    assertEquals("", records.get(1).get(SchemataHeader.Owner));
    assertEquals("", records.get(1).get(SchemataHeader.StorageLocation));
    assertEquals("", records.get(1).get(SchemataHeader.Properties));
  }

  @Test
  public void schemataTask_writesOnlySchemasAcceptedByFilter() throws Exception {
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded("information_schema", SCHEMA, "other"),
                "DESCRIBE SCHEMA EXTENDED `legacy`",
                succeededRows(ImmutableList.of(ImmutableList.of("Owner", "alice")))));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();
    DatabricksFilter filter =
        new DatabricksFilter(ImmutableList.of(), ImmutableList.of(" LEGACY "));

    List<CSVRecord> records =
        runTask(
            statements,
            filter,
            (sink, handle) ->
                new DatabricksHiveMetastoreSchemataTask(cache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    assertEquals(SCHEMA, records.get(0).get(SchemataHeader.SchemaName));
    assertEquals("alice", records.get(0).get(SchemataHeader.Owner));
  }

  @Test
  public void parseSchemaProperties_splitsKeysAndValues() {
    ImmutableMap<String, String> properties =
        ImmutableMap.of("delimiter", "a,b", "owner_team", "data", "spaced key", "x y");

    assertEquals(
        ImmutableList.copyOf(properties.entrySet()),
        ImmutableList.copyOf(parseSchemaProperties(schemaPropertiesText(properties)).entrySet()));
  }

  @Test
  public void parseSchemaProperties_returnsEmptyMapForOtherText() {
    for (String text : Arrays.asList("", "()", "[a=b]")) {
      assertEquals(text, ImmutableMap.of(), parseSchemaProperties(text));
    }
  }

  @Test
  public void tablesTask_writesDescription() throws Exception {
    String location = "dbfs:/mnt/legacy/csv_events";
    String createdTime = "2026-09-27T21:19:25Z";
    String serdeLibrary = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
    String inputFormat = "org.apache.hadoop.mapred.TextInputFormat";
    String outputFormat = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
    ImmutableMap<String, String> storageProperties =
        ImmutableMap.of("field.delim", ",", "serialization.format", ",");
    ImmutableMap<String, String> tableProperties =
        ImmutableMap.of("transient_lastDdlTime", "1790541380");
    ObjectNode output = table(SCHEMA, "csv_events", "EXTERNAL", "hive");
    output.put("location", location);
    output.put("comment", "Raw events");
    output.put("owner", "alice");
    output.put("created_time", createdTime);
    output.put("serde_library", serdeLibrary);
    output.put("input_format", inputFormat);
    output.put("output_format", outputFormat);
    putObject(output, "storage_properties", storageProperties);
    putObject(output, "table_properties", tableProperties);
    addColumn(output, "id", type("int"), true);
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            mockStatements(responsesFor(output).build()),
            (sink, handle) ->
                new DatabricksHiveMetastoreTablesTask(cache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    CSVRecord record = records.get(0);
    assertEquals(HIVE_METASTORE_CATALOG, record.get(TablesHeader.TableCatalog));
    assertEquals(SCHEMA, record.get(TablesHeader.TableSchema));
    assertEquals("csv_events", record.get(TablesHeader.TableName));
    assertEquals("EXTERNAL", record.get(TablesHeader.TableType));
    assertEquals("", record.get(TablesHeader.SecurableType));
    assertEquals("", record.get(TablesHeader.SecurableKind));
    assertEquals("HIVE", record.get(TablesHeader.DataSourceFormat));
    assertEquals(location, record.get(TablesHeader.StorageLocation));
    assertEquals("Raw events", record.get(TablesHeader.Comment));
    assertEquals("alice", record.get(TablesHeader.Owner));
    assertEquals(createdTime, record.get(TablesHeader.CreatedAt));
    assertEquals("", record.get(TablesHeader.UpdatedAt));
    assertEquals(serdeLibrary, record.get(TablesHeader.SerdeLibrary));
    assertEquals(inputFormat, record.get(TablesHeader.InputFormat));
    assertEquals(outputFormat, record.get(TablesHeader.OutputFormat));
    assertEquals(storageProperties, parseJsonObject(record.get(TablesHeader.StorageProperties)));
    assertEquals(tableProperties, parseJsonObject(record.get(TablesHeader.TableProperties)));
    assertEquals("", record.get(TablesHeader.NumBuckets));
    assertEquals("", record.get(TablesHeader.BucketColumns));
    assertEquals("", record.get(TablesHeader.SortColumns));
    assertEquals("", record.get(TablesHeader.RowFilter));
  }

  @Test
  public void tablesTask_writesBucketingAsJsonArrays() throws Exception {
    int numBuckets = 8;
    ImmutableList<String> bucketColumns = ImmutableList.of("id", "region");
    ImmutableList<String> sortColumns = ImmutableList.of("id");
    ObjectNode output = table(SCHEMA, "bucketed", "MANAGED", "parquet");
    output.put("num_buckets", numBuckets);
    putList(output, "bucket_columns", bucketColumns);
    putList(output, "sort_columns", sortColumns);
    addColumn(output, "id", type("long"), true);
    addColumn(output, "region", type("string"), true);
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            mockStatements(responsesFor(output).build()),
            (sink, handle) ->
                new DatabricksHiveMetastoreTablesTask(cache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    CSVRecord record = records.get(0);
    assertEquals(String.valueOf(numBuckets), record.get(TablesHeader.NumBuckets));
    assertEquals(bucketColumns, parseJsonArray(record.get(TablesHeader.BucketColumns)));
    assertEquals(sortColumns, parseJsonArray(record.get(TablesHeader.SortColumns)));
  }

  @Test
  public void tablesTask_skipsTemporaryViews() throws Exception {
    ObjectNode events = deltaTable(SCHEMA, "events", 2);
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                succeededRows(
                    ImmutableList.of(
                        ImmutableList.of(SCHEMA, "events", "false"),
                        ImmutableList.of("", "session_view", "true"))),
                describeStatement(SCHEMA, "events"),
                describeResponse(events),
                describeDetailStatement(SCHEMA, "events"),
                describeDetailResponse(SCHEMA, "events", ImmutableList.of())));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreTablesTask(cache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    assertEquals("events", records.get(0).get(TablesHeader.TableName));
  }

  @Test
  public void tablesTask_skipsSchemaWhoseShowTablesFails() throws Exception {
    StatementExecutionAPI statements =
        mockStatements(
            responsesFor(deltaTable(SCHEMA, "events", 2))
                .put(SHOW_SCHEMAS, succeeded("locked", SCHEMA))
                .put(showTablesStatement("locked"), failed(INSUFFICIENT_PERMISSIONS))
                .buildKeepingLast());
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreTablesTask(cache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    assertEquals(SCHEMA, records.get(0).get(TablesHeader.TableSchema));
    assertEquals("events", records.get(0).get(TablesHeader.TableName));
  }

  @Test
  public void tablesTask_failsWhenShowTablesFailsForEverySchema() {
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS, succeeded(SCHEMA), SHOW_TABLES, failed(INSUFFICIENT_PERMISSIONS)));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    IOException thrown =
        assertThrows(
            IOException.class,
            () ->
                runTask(
                    statements,
                    (sink, handle) ->
                        new DatabricksHiveMetastoreTablesTask(cache).doRun(null, sink, handle)));

    assertTrue(thrown.getMessage(), thrown.getMessage().contains("SHOW TABLES"));
  }

  @Test
  public void tablesTask_keepsTableWhoseDescribeFailsWithOnlyItsName() throws Exception {
    ObjectNode events = deltaTable(SCHEMA, "events", 2);
    events.put("location", "dbfs:/user/hive/warehouse/legacy.db/events");
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                showTablesResponse(SCHEMA, "events", "locked", "garbled"),
                describeStatement(SCHEMA, "events"),
                describeResponse(events),
                describeDetailStatement(SCHEMA, "events"),
                describeDetailResponse(SCHEMA, "events", ImmutableList.of()),
                describeStatement(SCHEMA, "locked"),
                failed(INSUFFICIENT_PERMISSIONS),
                describeStatement(SCHEMA, "garbled"),
                succeeded("DESCRIBE TABLE output that isn't JSON")));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreTablesTask(cache).doRun(null, sink, handle));

    assertEquals(
        ImmutableList.of("events", "locked", "garbled"),
        columnValues(records, TablesHeader.TableName));
    assertEquals("MANAGED", records.get(0).get(TablesHeader.TableType));
    ImmutableSet<TablesHeader> nameColumns =
        ImmutableSet.of(
            TablesHeader.TableCatalog, TablesHeader.TableSchema, TablesHeader.TableName);
    for (CSVRecord record : records.subList(1, records.size())) {
      for (TablesHeader column : TablesHeader.values()) {
        if (!nameColumns.contains(column)) {
          assertEquals(column.name(), "", record.get(column));
        }
      }
    }
  }

  @Test
  public void tablesTask_failsWhenDescribeFailsForEveryTable() {
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                showTablesResponse(SCHEMA, "locked"),
                describeStatement(SCHEMA, "locked"),
                failed("[PARSE_SYNTAX_ERROR] Syntax error at or near 'AS'.")));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    IOException thrown =
        assertThrows(
            IOException.class,
            () ->
                runTask(
                    statements,
                    (sink, handle) ->
                        new DatabricksHiveMetastoreTablesTask(cache).doRun(null, sink, handle)));

    assertTrue(thrown.getMessage(), thrown.getMessage().contains("DESCRIBE TABLE"));
  }

  @Test
  public void tablesTask_throwsInterruptedIoExceptionWhenInterrupted() {
    StatementExecutionAPI statements =
        mockStatements(responsesFor(deltaTable(SCHEMA, "events", 2)).build());
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    Thread.currentThread().interrupt();
    try {
      assertThrows(
          InterruptedIOException.class,
          () ->
              runTask(
                  statements,
                  (sink, handle) ->
                      new DatabricksHiveMetastoreTablesTask(cache).doRun(null, sink, handle)));
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  public void columnsTask_writesColumnsInOrder() throws Exception {
    ObjectNode events = table(SCHEMA, "events", "MANAGED", "parquet");
    addColumn(events, "id", type("long"), false).put("comment", "event id");
    addColumn(events, "name", lengthType("varchar", 20), true).put("comment", "it's the name");
    addColumn(events, "tags", arrayType(lengthType("char", 2)), true);
    addColumn(events, "point", structType(ImmutableMap.of("x", type("double"))), true);
    addColumn(events, "dt", type("date"), true).put("comment", "event date");
    putList(events, "partition_columns", ImmutableList.of("dt"));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            mockStatements(responsesFor(events).build()),
            (sink, handle) ->
                new DatabricksHiveMetastoreColumnsTask(cache).doRun(null, sink, handle));

    ImmutableList.Builder<String> columns = ImmutableList.builder();
    for (CSVRecord record : records) {
      columns.add(
          String.join(
              "|",
              record.get(ColumnsHeader.TableName),
              record.get(ColumnsHeader.OrdinalPosition),
              record.get(ColumnsHeader.ColumnName),
              record.get(ColumnsHeader.DataType),
              record.get(ColumnsHeader.IsNullable),
              record.get(ColumnsHeader.Comment),
              record.get(ColumnsHeader.PartitionIndex)));
    }
    assertEquals(
        ImmutableList.of(
            "events|0|id|bigint|NO|event id|",
            "events|1|name|varchar(20)|YES|it's the name|",
            "events|2|tags|array<char(2)>|YES||",
            "events|3|point|struct<x:double>|YES||",
            "events|4|dt|date|YES|event date|0"),
        columns.build());
  }

  @Test
  public void columnsTask_writesDefaultsAndEmptyUnityCatalogOnlyColumns() throws Exception {
    String defaultExpression = "'new'";
    ObjectNode orders = deltaTable(SCHEMA, "orders", 7);
    addColumn(orders, "status", type("string"), true).put("default", defaultExpression);
    addColumn(orders, "id", type("int"), true);
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            mockStatements(responsesFor(orders).build()),
            (sink, handle) ->
                new DatabricksHiveMetastoreColumnsTask(cache).doRun(null, sink, handle));

    assertEquals(
        ImmutableList.of(defaultExpression, ""),
        columnValues(records, ColumnsHeader.ColumnDefault));
    for (CSVRecord record : records) {
      for (ColumnsHeader column :
          ImmutableList.of(
              ColumnsHeader.GenerationExpression,
              ColumnsHeader.IdentityGeneration,
              ColumnsHeader.IdentityStart,
              ColumnsHeader.IdentityIncrement,
              ColumnsHeader.Mask)) {
        assertEquals(column.name(), "", record.get(column));
      }
    }
  }

  @Test
  public void columnsTask_skipsTableWhoseDescribeFails() throws Exception {
    ObjectNode events = deltaTable(SCHEMA, "events", 2);
    addColumn(events, "id", type("int"), true);
    StatementExecutionAPI statements =
        mockStatements(
            responsesFor(events)
                .put(SHOW_TABLES, showTablesResponse(SCHEMA, "locked", "events"))
                .put(describeStatement(SCHEMA, "locked"), failed(INSUFFICIENT_PERMISSIONS))
                .put(showCreateTableStatement("locked"), failed(INSUFFICIENT_PERMISSIONS))
                .buildKeepingLast());
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreColumnsTask(cache).doRun(null, sink, handle));

    assertEquals(ImmutableList.of("events"), columnValues(records, ColumnsHeader.TableName));
  }

  @Test
  public void columnsTask_writesPartitionIndexesOfDeltaTablesFromDescribeDetail() throws Exception {
    ObjectNode events = deltaTable(SCHEMA, "events", 2);
    addColumn(events, "id", type("long"), false);
    addColumn(events, "Region", type("string"), true);
    addColumn(events, "dt", type("date"), true);
    StatementExecutionAPI statements =
        mockStatements(
            responsesFor(events)
                .put(
                    describeDetailStatement(SCHEMA, "events"),
                    describeDetailResponse(SCHEMA, "events", ImmutableList.of("dt", "region")))
                .buildKeepingLast());
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreColumnsTask(cache).doRun(null, sink, handle));

    assertEquals(
        ImmutableList.of("", "1", "0"), columnValues(records, ColumnsHeader.PartitionIndex));
  }

  @Test
  public void columnsTask_writesDeltaColumnsWithoutPartitionIndexesWhenDescribeDetailFails()
      throws Exception {
    ObjectNode events = deltaTable(SCHEMA, "events", 2);
    addColumn(events, "id", type("long"), false);
    addColumn(events, "dt", type("date"), true);
    StatementExecutionAPI statements =
        mockStatements(
            responsesFor(events)
                .put(describeDetailStatement(SCHEMA, "events"), failed(INSUFFICIENT_PERMISSIONS))
                .put(
                    showCreateTableStatement("events"),
                    succeeded(
                        "CREATE TABLE hive_metastore.legacy.events (\n  id BIGINT NOT NULL,\n"
                            + "  dt DATE)\nUSING delta\nPARTITIONED BY (dt)"))
                .buildKeepingLast());
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreColumnsTask(cache).doRun(null, sink, handle));

    assertEquals(ImmutableList.of("id", "dt"), columnValues(records, ColumnsHeader.ColumnName));
    assertEquals(ImmutableList.of("", ""), columnValues(records, ColumnsHeader.PartitionIndex));
  }

  @Test
  public void columnsTask_writesGenerationAndIdentityClausesFromShowCreateTable() throws Exception {
    ObjectNode orders = deltaTable(SCHEMA, "orders", 7);
    ((ObjectNode) orders.get("table_properties"))
        .put("delta.feature.generatedColumns", "supported")
        .put("delta.feature.identityColumns", "supported");
    addColumn(orders, "id", type("long"), true);
    addColumn(orders, "ts", type("timestamp_ltz"), true);
    addColumn(orders, "dt", type("date"), true);
    String ddl =
        String.join(
            "\n",
            "CREATE TABLE hive_metastore.legacy.orders (",
            "  id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 100 INCREMENT BY 10),",
            "  ts TIMESTAMP,",
            "  dt DATE GENERATED ALWAYS AS ( CAST(ts AS DATE) ))",
            "USING delta");
    StatementExecutionAPI statements =
        mockStatements(
            responsesFor(orders).put(showCreateTableStatement("orders"), succeeded(ddl)).build());
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreColumnsTask(cache).doRun(null, sink, handle));

    ImmutableList.Builder<String> clauses = ImmutableList.builder();
    for (CSVRecord record : records) {
      clauses.add(
          String.join(
              "|",
              record.get(ColumnsHeader.ColumnName),
              record.get(ColumnsHeader.GenerationExpression),
              record.get(ColumnsHeader.IdentityGeneration),
              record.get(ColumnsHeader.IdentityStart),
              record.get(ColumnsHeader.IdentityIncrement)));
    }
    assertEquals(
        ImmutableList.of("id||BY DEFAULT|100|10", "ts||||", "dt|CAST(ts AS DATE)|||"),
        clauses.build());
  }

  @Test
  public void columnsTask_writesColumnsWithoutClausesWhenShowCreateTableFails() throws Exception {
    ObjectNode orders = deltaTable(SCHEMA, "orders", 7);
    ((ObjectNode) orders.get("table_properties"))
        .put("delta.feature.generatedColumns", "supported");
    addColumn(orders, "id", type("long"), true);
    StatementExecutionAPI statements =
        mockStatements(
            responsesFor(orders)
                .put(showCreateTableStatement("orders"), failed(INSUFFICIENT_PERMISSIONS))
                .build());
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreColumnsTask(cache).doRun(null, sink, handle));

    assertEquals(ImmutableList.of("id"), columnValues(records, ColumnsHeader.ColumnName));
    assertEquals(ImmutableList.of(""), columnValues(records, ColumnsHeader.GenerationExpression));
  }

  @Test
  public void sessionCache_runsShowCreateTableOnceForColumnsAndDdls() throws Exception {
    ObjectNode orders = deltaTable(SCHEMA, "orders", 7);
    ((ObjectNode) orders.get("table_properties")).put("delta.feature.identityColumns", "supported");
    addColumn(orders, "id", type("long"), true);
    String showCreateTable = showCreateTableStatement("orders");
    StatementExecutionAPI statements =
        mockStatements(
            responsesFor(orders)
                .put(
                    showCreateTable,
                    succeeded(
                        "CREATE TABLE hive_metastore.legacy.orders (\n  id BIGINT GENERATED"
                            + " ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1))\nUSING delta"))
                .build());
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    runTask(
        statements,
        (sink, handle) -> new DatabricksHiveMetastoreColumnsTask(cache).doRun(null, sink, handle));
    List<CSVRecord> ddls =
        runTask(
            statements,
            (sink, handle) -> new DatabricksHiveMetastoreDdlsTask(cache).doRun(null, sink, handle));

    assertEquals(ImmutableList.of("orders"), columnValues(ddls, DdlsHeader.TableName));
    // SHOW SCHEMAS, SHOW TABLES, DESCRIBE TABLE, DESCRIBE DETAIL and SHOW CREATE TABLE, once each.
    verify(statements, times(5)).executeStatement(any(ExecuteStatementRequest.class));
  }

  @Test
  public void viewsTask_writesOnlyViews() throws Exception {
    String viewText = "SELECT id\nFROM legacy.events";
    String viewCatalogAndNamespace = "spark_catalog.legacy";
    ImmutableList<String> viewQueryOutputColumns = ImmutableList.of("id");
    String viewSchemaMode = "COMPENSATION";
    ObjectNode eventIds = view(SCHEMA, "event_ids", viewText);
    eventIds.put("view_catalog_and_namespace", viewCatalogAndNamespace);
    putList(eventIds, "view_query_output_columns", viewQueryOutputColumns);
    eventIds.put("view_schema_mode", viewSchemaMode);
    addColumn(eventIds, "id", type("int"), true);
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            mockStatements(responsesFor(deltaTable(SCHEMA, "events", 2), eventIds).build()),
            (sink, handle) ->
                new DatabricksHiveMetastoreViewsTask(cache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    CSVRecord record = records.get(0);
    assertEquals("event_ids", record.get(ViewsHeader.TableName));
    assertEquals(viewText, record.get(ViewsHeader.ViewDefinition));
    assertEquals(viewCatalogAndNamespace, record.get(ViewsHeader.ViewCatalogAndNamespace));
    assertEquals(
        viewQueryOutputColumns, parseJsonArray(record.get(ViewsHeader.ViewQueryOutputColumns)));
    assertEquals(viewSchemaMode, record.get(ViewsHeader.ViewSchemaMode));
    assertEquals("", record.get(ViewsHeader.SqlPath));
  }

  @Test
  public void tableConstraintsTask_writesDeltaCheckConstraints() throws Exception {
    ImmutableMap<String, String> checks =
        ImmutableMap.of(
            "positive_amount", "amount > 0", "known_status", "status IN ('new', 'done')");
    ImmutableMap.Builder<String, String> tableProperties = ImmutableMap.builder();
    for (Map.Entry<String, String> check : checks.entrySet()) {
      tableProperties.put("delta.constraints." + check.getKey(), check.getValue());
    }
    tableProperties.put("delta.minWriterVersion", "3");
    ObjectNode orders = table(SCHEMA, "orders", "MANAGED", "delta");
    putObject(orders, "table_properties", tableProperties.build());
    addColumn(orders, "amount", type("int"), true);
    addColumn(orders, "status", type("string"), true);
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            mockStatements(responsesFor(orders).build()),
            (sink, handle) ->
                new DatabricksHiveMetastoreTableConstraintsTask(cache).doRun(null, sink, handle));

    ImmutableList.Builder<String> expected = ImmutableList.builder();
    for (Map.Entry<String, String> check : checks.entrySet()) {
      expected.add(String.join("|", "orders", check.getKey(), "CHECK", check.getValue()));
    }
    ImmutableList.Builder<String> written = ImmutableList.builder();
    for (CSVRecord record : records) {
      Map<String, String> details =
          parseJsonObject(record.get(TableConstraintsHeader.ConstraintDetails));
      written.add(
          String.join(
              "|",
              record.get(TableConstraintsHeader.TableName),
              record.get(TableConstraintsHeader.ConstraintName),
              record.get(TableConstraintsHeader.ConstraintType),
              details.get("expression")));
    }
    assertEquals(expected.build(), written.build());
  }

  @Test
  public void tasks_writeHiveMetastoreAsCatalogWhenSparkCatalogIsReported() throws Exception {
    String reportedCatalog = "spark_catalog";
    ObjectNode orders = table(SCHEMA, "orders", "MANAGED", "delta");
    orders.put("catalog_name", reportedCatalog);
    putObject(
        orders, "table_properties", ImmutableMap.of("delta.constraints.positive", "amount > 0"));
    addColumn(orders, "amount", type("int"), true);
    ObjectNode orderAmounts = view(SCHEMA, "order_amounts", "SELECT amount FROM legacy.orders");
    orderAmounts.put("catalog_name", reportedCatalog);
    addColumn(orderAmounts, "amount", type("int"), true);
    // The table has no writer version, so the columns task also runs SHOW CREATE TABLE for it.
    StatementExecutionAPI statements =
        mockStatements(
            responsesFor(orders, orderAmounts)
                .put(
                    showCreateTableStatement("orders"),
                    succeeded(
                        "CREATE TABLE spark_catalog.legacy.orders (\n  amount INT)\nUSING delta"))
                .build());
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();
    ImmutableMap<Enum<?>, TaskBody> tasksByCatalogColumn =
        ImmutableMap.<Enum<?>, TaskBody>of(
            TablesHeader.TableCatalog,
            (sink, handle) ->
                new DatabricksHiveMetastoreTablesTask(cache).doRun(null, sink, handle),
            ColumnsHeader.TableCatalog,
            (sink, handle) ->
                new DatabricksHiveMetastoreColumnsTask(cache).doRun(null, sink, handle),
            ViewsHeader.TableCatalog,
            (sink, handle) -> new DatabricksHiveMetastoreViewsTask(cache).doRun(null, sink, handle),
            TableConstraintsHeader.TableCatalog,
            (sink, handle) ->
                new DatabricksHiveMetastoreTableConstraintsTask(cache).doRun(null, sink, handle));

    for (Map.Entry<Enum<?>, TaskBody> task : tasksByCatalogColumn.entrySet()) {
      List<CSVRecord> records = runTask(statements, task.getValue());

      String header = task.getKey().getDeclaringClass().getSimpleName();
      assertFalse(header, records.isEmpty());
      for (CSVRecord record : records) {
        assertEquals(header, HIVE_METASTORE_CATALOG, record.get(task.getKey()));
      }
    }
  }

  @Test
  public void sessionCache_describesEachTableOnceForAllTasks() throws Exception {
    ObjectNode[] outputs = {deltaTable(SCHEMA, "numbers", 2), deltaTable(SCHEMA, "letters", 2)};
    StatementExecutionAPI statements = mockStatements(responsesFor(outputs).build());
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    runTask(
        statements,
        (sink, handle) -> new DatabricksHiveMetastoreTablesTask(cache).doRun(null, sink, handle));
    runTask(
        statements,
        (sink, handle) -> new DatabricksHiveMetastoreColumnsTask(cache).doRun(null, sink, handle));
    runTask(
        statements,
        (sink, handle) -> new DatabricksHiveMetastoreViewsTask(cache).doRun(null, sink, handle));

    // One DESCRIBE TABLE and one DESCRIBE DETAIL for each Delta table.
    int statementCount = ImmutableList.of(SHOW_SCHEMAS, SHOW_TABLES).size() + 2 * outputs.length;
    verify(statements, times(statementCount)).executeStatement(any(ExecuteStatementRequest.class));
  }

  @Test
  public void sessionCache_doesNotRepeatFailedScan() throws Exception {
    StatementExecutionAPI statements =
        mockStatements(ImmutableMap.of(SHOW_SCHEMAS, failed(INSUFFICIENT_PERMISSIONS)));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    try (DatabricksHandle handle = newHandle(statements, acceptAllFilter())) {
      assertThrows(IOException.class, () -> cache.listParsedTables(handle));
      IOException repeated = assertThrows(IOException.class, () -> cache.listSchemas(handle));
      assertTrue(repeated.getMessage().contains("failed earlier"));
    }

    verify(statements, times(1)).executeStatement(any(ExecuteStatementRequest.class));
  }

  private List<CSVRecord> runTask(StatementExecutionAPI statements, TaskBody body)
      throws Exception {
    return runTask(statements, acceptAllFilter(), body);
  }

  private List<CSVRecord> runTask(
      StatementExecutionAPI statements, DatabricksFilter filter, TaskBody body) throws Exception {
    return DatabricksStatementStubs.runTask(tempFolder.newFile(), statements, filter, body);
  }

  private static String showCreateTableStatement(String tableName) {
    return "SHOW CREATE TABLE `" + SCHEMA + "`.`" + tableName + "`";
  }

  private static ImmutableMap.Builder<String, StatementResponse> responsesFor(
      ObjectNode... outputs) {
    return DescribeTableJson.responsesFor(SCHEMA, Arrays.asList(outputs));
  }

  private static ImmutableList<String> columnValues(List<CSVRecord> records, Enum<?> column) {
    ImmutableList.Builder<String> values = ImmutableList.builder();
    for (CSVRecord record : records) {
      values.add(record.get(column));
    }
    return values.build();
  }

  private static Map<String, String> parseJsonObject(String json) throws IOException {
    return JSON.readValue(json, new TypeReference<Map<String, String>>() {});
  }

  private static List<String> parseJsonArray(String json) throws IOException {
    return JSON.readValue(json, new TypeReference<List<String>>() {});
  }

  /**
   * Returns properties as {@code DESCRIBE SCHEMA EXTENDED} prints them, such as {@code ((k,v),
   * (k2,v2))}.
   */
  private static String schemaPropertiesText(Map<String, String> properties) {
    List<String> entries = new ArrayList<>();
    for (Map.Entry<String, String> property : properties.entrySet()) {
      entries.add("(" + property.getKey() + "," + property.getValue() + ")");
    }
    return "(" + String.join(", ", entries) + ")";
  }
}
