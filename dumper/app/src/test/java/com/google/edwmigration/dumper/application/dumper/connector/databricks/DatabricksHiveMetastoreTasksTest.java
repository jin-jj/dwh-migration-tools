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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreCatalogsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreColumnsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreSchemataTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreTableConstraintsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreTablesTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.DatabricksHiveMetastoreViewsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.HiveMetastoreSessionCache;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.CatalogsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.ColumnsHeader;
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
  private static final String SHOW_TABLES = "SHOW TABLE EXTENDED IN `legacy` LIKE '*'";
  private static final String VIEW_CATALOG_AND_NAMESPACE = "spark_catalog.legacy";
  private static final String VIEW_SCHEMA_MODE = "COMPENSATION";
  private static final ImmutableList<String> VIEW_QUERY_OUTPUT_COLUMNS = ImmutableList.of("id");
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
                failed("[INSUFFICIENT_PERMISSIONS] Insufficient privileges.")));
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
  public void tablesTask_writesHiveStorageColumns() throws Exception {
    String serdeLibrary = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
    String inputFormat = "org.apache.hadoop.mapred.TextInputFormat";
    String outputFormat = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
    ImmutableMap<String, String> storageProperties =
        ImmutableMap.of("field.delim", ",", "serialization.format", ",");
    ImmutableMap<String, String> tableProperties =
        ImmutableMap.of("transient_lastDdlTime", "1790541380");
    String information =
        "Catalog: hive_metastore\n"
            + "Database: legacy\n"
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
            + "Schema: root\n"
            + " |-- id: integer (nullable = true)\n";
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                showTableResponse("csv_events", information)));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreTablesTask(cache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    CSVRecord record = records.get(0);
    assertEquals(HIVE_METASTORE_CATALOG, record.get(TablesHeader.TableCatalog));
    assertEquals("csv_events", record.get(TablesHeader.TableName));
    assertEquals("EXTERNAL", record.get(TablesHeader.TableType));
    assertEquals("", record.get(TablesHeader.SecurableType));
    assertEquals("", record.get(TablesHeader.SecurableKind));
    assertEquals("HIVE", record.get(TablesHeader.DataSourceFormat));
    assertEquals("dbfs:/mnt/legacy/csv_events", record.get(TablesHeader.StorageLocation));
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
    String numBuckets = "8";
    ImmutableList<String> bucketColumns = ImmutableList.of("id", "region");
    ImmutableList<String> sortColumns = ImmutableList.of("id");
    String information =
        String.join(
            "\n",
            "Catalog: hive_metastore",
            "Database: legacy",
            "Table: bucketed",
            "Type: MANAGED",
            "Provider: parquet",
            "Num Buckets: " + numBuckets,
            "Bucket Columns: " + quotedList(bucketColumns),
            "Sort Columns: " + quotedList(sortColumns),
            "Schema: root",
            " |-- id: long (nullable = true)",
            " |-- region: string (nullable = true)",
            "");
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                showTableResponse("bucketed", information)));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreTablesTask(cache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    CSVRecord record = records.get(0);
    assertEquals(numBuckets, record.get(TablesHeader.NumBuckets));
    assertEquals(bucketColumns, parseJsonArray(record.get(TablesHeader.BucketColumns)));
    assertEquals(sortColumns, parseJsonArray(record.get(TablesHeader.SortColumns)));
  }

  @Test
  public void tablesTask_skipsTemporaryViews() throws Exception {
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                succeededRows(
                    ImmutableList.of(
                        ImmutableList.of(SCHEMA, "events", "false", tableInformation("events")),
                        ImmutableList.of("", "session_view", "true", viewInformation("x"))))));
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
  public void tablesTask_skipsSchemaWhoseShowTableExtendedFails() throws Exception {
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded("locked", SCHEMA),
                "SHOW TABLE EXTENDED IN `locked` LIKE '*'",
                failed("[INSUFFICIENT_PERMISSIONS] Insufficient privileges."),
                SHOW_TABLES,
                showTablesResponse("events")));
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
  public void tablesTask_failsWhenShowTableExtendedFailsForEverySchema() {
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                failed("[INSUFFICIENT_PERMISSIONS] Insufficient privileges.")));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    IOException thrown =
        assertThrows(
            IOException.class,
            () ->
                runTask(
                    statements,
                    (sink, handle) ->
                        new DatabricksHiveMetastoreTablesTask(cache).doRun(null, sink, handle)));

    assertTrue(thrown.getMessage().contains("SHOW TABLE EXTENDED"));
  }

  @Test
  public void tablesTask_throwsInterruptedIoExceptionWhenInterrupted() {
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS, succeeded(SCHEMA), SHOW_TABLES, showTablesResponse("events")));
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
  public void columnsTask_writesColumnsInSchemaOrder() throws Exception {
    String information =
        String.join(
            "\n",
            "Catalog: hive_metastore",
            "Database: legacy",
            "Table: events",
            "Type: MANAGED",
            "Provider: delta",
            "Partition Columns: [`dt`]",
            "Schema: root",
            " |-- id: long (nullable = false)",
            " |-- point: struct (nullable = true)",
            " |    |-- x: double (nullable = true)",
            " |-- dt: date (nullable = true)",
            "");
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                showTableResponse("events", information),
                "DESCRIBE TABLE `legacy`.`events`",
                succeededRows(
                    ImmutableList.of(
                        Arrays.asList("id", "bigint", "event id"),
                        Arrays.asList("point", "struct<x:double>", null),
                        Arrays.asList("dt", "date", "event date")))));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
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
            "events|1|point|struct<x:double>|YES||",
            "events|2|dt|date|YES|event date|0"),
        columns.build());
  }

  @Test
  public void columnsTask_writesEmptyUnityCatalogOnlyColumns() throws Exception {
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                showTablesResponse("numbers"),
                "DESCRIBE TABLE `legacy`.`numbers`",
                succeededRows(ImmutableList.of(Arrays.asList("id", "int", null)))));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreColumnsTask(cache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    for (ColumnsHeader column :
        ImmutableList.of(
            ColumnsHeader.ColumnDefault,
            ColumnsHeader.GenerationExpression,
            ColumnsHeader.IdentityGeneration,
            ColumnsHeader.IdentityStart,
            ColumnsHeader.IdentityIncrement,
            ColumnsHeader.Mask)) {
      assertEquals(column.name(), "", records.get(0).get(column));
    }
  }

  @Test
  public void columnsTask_writesCommentsAndCharVarcharLengthsFromDescribeTable() throws Exception {
    String information =
        String.join(
            "\n",
            "Catalog: hive_metastore",
            "Database: legacy",
            "Table: customers",
            "Type: MANAGED",
            "Provider: delta",
            "Partition Columns: [`dt`]",
            "Schema: root",
            " |-- id: long (nullable = false)",
            " |-- name: string (nullable = true)",
            " |-- tags: array (nullable = true)",
            " |    |-- element: string (containsNull = true)",
            " |-- dt: date (nullable = true)",
            "");
    ImmutableList<List<String>> columnRows =
        ImmutableList.of(
            Arrays.asList("id", "bigint", "primary id"),
            Arrays.asList("name", "varchar(20)", "customer's name"),
            Arrays.asList("tags", "array<char(2)>", null),
            Arrays.asList("dt", "date", null));
    List<List<String>> describeRows = new ArrayList<>(columnRows);
    describeRows.add(Arrays.asList("# Partition Information", "", ""));
    describeRows.add(Arrays.asList("# col_name", "data_type", "comment"));
    describeRows.add(Arrays.asList("dt", "date", null));
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                showTableResponse("customers", information),
                "DESCRIBE TABLE `legacy`.`customers`",
                succeededRows(describeRows)));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreColumnsTask(cache).doRun(null, sink, handle));

    ImmutableList.Builder<String> expected = ImmutableList.builder();
    for (List<String> row : columnRows) {
      expected.add(row.get(1) + "|" + Strings.nullToEmpty(row.get(2)));
    }
    ImmutableList.Builder<String> written = ImmutableList.builder();
    for (CSVRecord record : records) {
      written.add(record.get(ColumnsHeader.DataType) + "|" + record.get(ColumnsHeader.Comment));
    }
    assertEquals(expected.build(), written.build());
  }

  @Test
  public void columnsTask_describesEveryTableWithColumns() throws Exception {
    ImmutableList<String> tableNames = ImmutableList.of("numbers", "letters");
    ImmutableMap.Builder<String, StatementResponse> responses = ImmutableMap.builder();
    responses.put(SHOW_SCHEMAS, succeeded(SCHEMA));
    responses.put(SHOW_TABLES, showTablesResponse(tableNames.toArray(new String[0])));
    for (String tableName : tableNames) {
      responses.put(
          "DESCRIBE TABLE `legacy`.`" + tableName + "`",
          succeededRows(ImmutableList.of(Arrays.asList("id", "int", tableName + " id"))));
    }
    StatementExecutionAPI statements = mockStatements(responses.build());
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreColumnsTask(cache).doRun(null, sink, handle));

    ImmutableList.Builder<String> expected = ImmutableList.builder();
    for (String tableName : tableNames) {
      expected.add(tableName + "|int|" + tableName + " id");
    }
    ImmutableList.Builder<String> written = ImmutableList.builder();
    for (CSVRecord record : records) {
      written.add(
          String.join(
              "|",
              record.get(ColumnsHeader.TableName),
              record.get(ColumnsHeader.DataType),
              record.get(ColumnsHeader.Comment)));
    }
    assertEquals(expected.build(), written.build());
    int statementCount = ImmutableList.of(SHOW_SCHEMAS, SHOW_TABLES).size() + tableNames.size();
    verify(statements, times(statementCount)).executeStatement(any(ExecuteStatementRequest.class));
  }

  @Test
  public void columnsTask_runsNoDescribeTableForTableWithoutColumns() throws Exception {
    String information =
        String.join(
            "\n",
            "Catalog: hive_metastore",
            "Database: legacy",
            "Table: unreadable",
            "Type: EXTERNAL",
            "Provider: delta",
            "Location: s3://missing/unreadable",
            "");
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                showTableResponse("unreadable", information)));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreColumnsTask(cache).doRun(null, sink, handle));

    assertTrue(records.isEmpty());
    int showStatementCount = ImmutableList.of(SHOW_SCHEMAS, SHOW_TABLES).size();
    verify(statements, times(showStatementCount))
        .executeStatement(any(ExecuteStatementRequest.class));
  }

  @Test
  public void columnsTask_keepsParsedColumnsWhenDescribeTableFails() throws Exception {
    String information =
        String.join(
            "\n",
            "Catalog: hive_metastore",
            "Database: legacy",
            "Table: customers",
            "Type: MANAGED",
            "Provider: delta",
            "Schema: root",
            " |-- name: string (nullable = true)",
            "");
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                showTableResponse("customers", information),
                "DESCRIBE TABLE `legacy`.`customers`",
                failed("[INSUFFICIENT_PERMISSIONS] Insufficient privileges.")));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreColumnsTask(cache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    assertEquals("name", records.get(0).get(ColumnsHeader.ColumnName));
    assertEquals("string", records.get(0).get(ColumnsHeader.DataType));
    assertEquals("", records.get(0).get(ColumnsHeader.Comment));
  }

  @Test
  public void viewsTask_writesOnlyViews() throws Exception {
    String viewText = "SELECT id\nFROM legacy.events";
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                succeededRows(
                    ImmutableList.of(
                        ImmutableList.of(SCHEMA, "events", "false", tableInformation("events")),
                        ImmutableList.of(
                            SCHEMA, "event_ids", "false", viewInformation(viewText))))));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
            (sink, handle) ->
                new DatabricksHiveMetastoreViewsTask(cache).doRun(null, sink, handle));

    assertEquals(1, records.size());
    CSVRecord record = records.get(0);
    assertEquals("event_ids", record.get(ViewsHeader.TableName));
    assertEquals(viewText, record.get(ViewsHeader.ViewDefinition));
    assertEquals(VIEW_CATALOG_AND_NAMESPACE, record.get(ViewsHeader.ViewCatalogAndNamespace));
    assertEquals(
        VIEW_QUERY_OUTPUT_COLUMNS, parseJsonArray(record.get(ViewsHeader.ViewQueryOutputColumns)));
    assertEquals(VIEW_SCHEMA_MODE, record.get(ViewsHeader.ViewSchemaMode));
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
    tableProperties.put("delta.minReaderVersion", "1");
    String information =
        String.join(
            "\n",
            "Catalog: hive_metastore",
            "Database: legacy",
            "Table: orders",
            "Type: MANAGED",
            "Provider: delta",
            "Table Properties: " + bracketed(tableProperties.build()),
            "Schema: root",
            " |-- amount: integer (nullable = true)",
            " |-- status: integer (nullable = true)",
            "");
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                showTableResponse("orders", information)));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    List<CSVRecord> records =
        runTask(
            statements,
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
  public void tasks_writeHiveMetastoreAsCatalogWhenSparkCatalogIsPrinted() throws Exception {
    String printedCatalog = "spark_catalog";
    String tableInformation =
        String.join(
            "\n",
            "Catalog: " + printedCatalog,
            "Database: legacy",
            "Table: orders",
            "Type: MANAGED",
            "Provider: delta",
            "Table Properties: [delta.constraints.positive_amount=amount > 0]",
            "Schema: root",
            " |-- amount: integer (nullable = true)",
            "");
    String viewInformation =
        String.join(
            "\n",
            "Catalog: " + printedCatalog,
            "Database: legacy",
            "Table: order_amounts",
            "Type: VIEW",
            "View Text: SELECT amount FROM legacy.orders",
            "Schema: root",
            " |-- amount: integer (nullable = true)",
            "");
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_TABLES,
                succeededRows(
                    ImmutableList.of(
                        ImmutableList.of(SCHEMA, "orders", "false", tableInformation),
                        ImmutableList.of(SCHEMA, "order_amounts", "false", viewInformation))),
                "DESCRIBE TABLE `legacy`.`orders`",
                succeededRows(ImmutableList.of(Arrays.asList("amount", "int", null))),
                "DESCRIBE TABLE `legacy`.`order_amounts`",
                succeededRows(ImmutableList.of(Arrays.asList("amount", "int", null)))));
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
  public void sessionCache_runsShowTableExtendedOncePerSchemaForAllTasks() throws Exception {
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS, succeeded(SCHEMA), SHOW_TABLES, showTablesResponse("events")));
    HiveMetastoreSessionCache cache = new HiveMetastoreSessionCache();

    runTask(
        statements,
        (sink, handle) -> new DatabricksHiveMetastoreTablesTask(cache).doRun(null, sink, handle));
    runTask(
        statements,
        (sink, handle) -> new DatabricksHiveMetastoreViewsTask(cache).doRun(null, sink, handle));

    int showStatementCount = ImmutableList.of(SHOW_SCHEMAS, SHOW_TABLES).size();
    verify(statements, times(showStatementCount))
        .executeStatement(any(ExecuteStatementRequest.class));
  }

  @Test
  public void sessionCache_doesNotRepeatFailedScan() throws Exception {
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS, failed("[INSUFFICIENT_PERMISSIONS] Insufficient privileges.")));
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

  private static Map<String, String> parseJsonObject(String json) throws IOException {
    return JSON.readValue(json, new TypeReference<Map<String, String>>() {});
  }

  private static List<String> parseJsonArray(String json) throws IOException {
    return JSON.readValue(json, new TypeReference<List<String>>() {});
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

  /** Returns identifiers as {@code SHOW TABLE EXTENDED} prints them, such as {@code [`a`, `b`]}. */
  private static String quotedList(List<String> identifiers) {
    List<String> quoted = new ArrayList<>();
    for (String identifier : identifiers) {
      quoted.add("`" + identifier.replace("`", "``") + "`");
    }
    return "[" + String.join(", ", quoted) + "]";
  }

  private static StatementResponse showTableResponse(String tableName, String information) {
    return succeededRows(
        ImmutableList.of(ImmutableList.of(SCHEMA, tableName, "false", information)));
  }

  private static StatementResponse showTablesResponse(String... tableNames) {
    List<List<String>> tableRows = new ArrayList<>();
    for (String tableName : tableNames) {
      tableRows.add(ImmutableList.of(SCHEMA, tableName, "false", tableInformation(tableName)));
    }
    return succeededRows(tableRows);
  }

  /** Returns the {@code SHOW TABLE EXTENDED} information of a Delta table with an int column. */
  private static String tableInformation(String tableName) {
    return "Catalog: hive_metastore\n"
        + "Database: legacy\n"
        + "Table: "
        + tableName
        + "\n"
        + "Type: MANAGED\n"
        + "Provider: delta\n"
        + "Schema: root\n"
        + " |-- id: integer (nullable = false)\n";
  }

  /** Returns the {@code SHOW TABLE EXTENDED} information of a view with an int column. */
  private static String viewInformation(String viewText) {
    return "Catalog: hive_metastore\n"
        + "Database: legacy\n"
        + "Table: event_ids\n"
        + "Type: VIEW\n"
        + "View Text: "
        + viewText
        + "\n"
        + "View Original Text: "
        + viewText
        + "\n"
        + "View Schema Mode: "
        + VIEW_SCHEMA_MODE
        + "\n"
        + "View Catalog and Namespace: "
        + VIEW_CATALOG_AND_NAMESPACE
        + "\n"
        + "View Query Output Columns: "
        + quotedList(VIEW_QUERY_OUTPUT_COLUMNS)
        + "\n"
        + "Schema: root\n"
        + " |-- id: integer (nullable = true)\n";
  }
}
