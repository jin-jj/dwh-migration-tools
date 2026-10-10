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
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDdlTasks.needsShowCreateTable;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreFunctionDdlFormatter.formatCreateFunction;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.parseDescribeTableJson;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.undescribedTable;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.acceptAllFilter;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.failed;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.mockStatements;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.succeeded;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.deltaTable;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.describeDetailStatement;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.describeStatement;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.putObject;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.showTablesResponse;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.showTablesStatement;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.table;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DescribeTableJson.view;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.StatementExecutionAPI;
import com.databricks.sdk.service.sql.StatementResponse;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDdlTasks.DatabricksHiveMetastoreDdlsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDdlTasks.DatabricksHiveMetastoreFunctionDdlsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreFunctionParser.ParsedFunction;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreFunctionTasks.DatabricksHiveMetastoreFunctionsTask;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreFunctionTasks.HiveMetastoreFunctionCache;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.ParsedTable;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTasks.HiveMetastoreSessionCache;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataConnector.ShowCreateTableMode;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.DdlsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.FunctionDdlsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksStatementStubs.TaskBody;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksHiveMetastoreDdlTasksTest {

  private static final String SCHEMA = "legacy";
  private static final String SHOW_SCHEMAS = "SHOW SCHEMAS";
  private static final String SHOW_TABLES = showTablesStatement(SCHEMA);
  private static final String SHOW_FUNCTIONS = "SHOW USER FUNCTIONS IN `legacy`";

  /** A writer version of the Delta protocol with table features, which lists generated columns. */
  private static final int TABLE_FEATURES_WRITER_VERSION = 7;

  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void needsShowCreateTable_selectsDeltaTablesWithGeneratedOrIdentityColumnFeatures()
      throws IOException {
    for (String feature :
        Arrays.asList(
            "delta.feature.generatedColumns",
            "delta.feature.identityColumns",
            "delta.feature.GeneratedColumns")) {
      ObjectNode output = deltaTable(SCHEMA, "events", TABLE_FEATURES_WRITER_VERSION);
      ((ObjectNode) output.get("table_properties")).put(feature, "supported");

      assertTrue(feature, needsShowCreateTable(parse(output)));
    }
  }

  @Test
  public void needsShowCreateTable_skipsDeltaTablesWithOtherFeatures() throws IOException {
    ObjectNode output = deltaTable(SCHEMA, "events", TABLE_FEATURES_WRITER_VERSION);
    ((ObjectNode) output.get("table_properties"))
        .put("delta.feature.allowColumnDefaults", "supported")
        .put("delta.feature.clustering", "supported")
        .put("delta.feature.checkConstraints", "supported");

    assertFalse(needsShowCreateTable(parse(output)));
  }

  @Test
  public void needsShowCreateTable_selectsDeltaTablesOnWriterVersionsWithoutTableFeatures()
      throws IOException {
    ImmutableMap<Integer, Boolean> neededByWriterVersion =
        ImmutableMap.<Integer, Boolean>builder()
            .put(1, false)
            .put(2, false)
            .put(3, false)
            .put(4, true)
            .put(5, true)
            .put(6, true)
            .put(TABLE_FEATURES_WRITER_VERSION, false)
            .build();

    for (Map.Entry<Integer, Boolean> needed : neededByWriterVersion.entrySet()) {
      ObjectNode output = deltaTable(SCHEMA, "events", needed.getKey());

      assertEquals(
          "writer version " + needed.getKey(),
          needed.getValue(),
          needsShowCreateTable(parse(output)));
    }
  }

  @Test
  public void needsShowCreateTable_selectsDeltaTablesWithoutKnownWriterVersion()
      throws IOException {
    ObjectNode withoutProperties = table(SCHEMA, "events", "MANAGED", "delta");
    ObjectNode withUnparseableVersion = deltaTable(SCHEMA, "events", TABLE_FEATURES_WRITER_VERSION);
    ((ObjectNode) withUnparseableVersion.get("table_properties"))
        .put("delta.minWriterVersion", "seven");

    assertTrue(needsShowCreateTable(parse(withoutProperties)));
    assertTrue(needsShowCreateTable(parse(withUnparseableVersion)));
  }

  @Test
  public void needsShowCreateTable_skipsOtherTablesAndViews() throws IOException {
    ImmutableMap<String, String> legacyDeltaProperties =
        ImmutableMap.of(
            "delta.minWriterVersion", "4", "delta.feature.generatedColumns", "supported");
    List<ObjectNode> outputs = new ArrayList<>();
    for (String provider : Arrays.asList("parquet", "csv", "hive", "com.acme.CustomSource")) {
      ObjectNode output = table(SCHEMA, "events", "EXTERNAL", provider);
      putObject(output, "table_properties", legacyDeltaProperties);
      outputs.add(output);
    }
    outputs.add(view(SCHEMA, "event_ids", "SELECT id FROM legacy.events"));

    for (ObjectNode output : outputs) {
      assertFalse(output.toString(), needsShowCreateTable(parse(output)));
    }
  }

  @Test
  public void needsShowCreateTable_selectsUndescribedTables() {
    assertTrue(needsShowCreateTable(undescribedTable(SCHEMA, "locked")));
  }

  @Test
  public void needsShowCreateTable_selectsDeltaTablesWithUnknownPartitioning() throws IOException {
    ObjectNode output = deltaTable(SCHEMA, "events", TABLE_FEATURES_WRITER_VERSION);
    ParsedTable describedTable = parseDescribeTableJson(SCHEMA, "events", output.toString());

    assertTrue(needsShowCreateTable(describedTable));
    assertFalse(needsShowCreateTable(describedTable.withPartitionColumns(ImmutableList.of("dt"))));
  }

  @Test
  public void ddlsTask_runsShowCreateTableOnlyForNeededTablesByDefault() throws Exception {
    String generatedDdl =
        "CREATE TABLE hive_metastore.legacy.generated (\n"
            + "  ts TIMESTAMP,\n"
            + "  dt DATE GENERATED ALWAYS AS (CAST(ts AS DATE)))\n"
            + "USING delta";
    StatementExecutionAPI statements =
        mockStatements(
            responsesFor(neededAndOtherTables())
                .put(showCreateTable("generated"), succeeded(generatedDdl))
                .build());

    List<CSVRecord> records = runDdlsTask(statements, ShowCreateTableMode.NEEDED);

    assertEquals(1, records.size());
    assertEquals("generated", records.get(0).get(DdlsHeader.TableName));
    assertEquals(generatedDdl, records.get(0).get(DdlsHeader.Ddl));
    int describeDetailCount = 0;
    for (JsonNode output : neededAndOtherTables()) {
      describeDetailCount += "delta".equals(output.path("provider").asText()) ? 1 : 0;
    }
    int statementCount =
        ImmutableList.of(SHOW_SCHEMAS, SHOW_TABLES, showCreateTable("generated")).size()
            + neededAndOtherTables().size()
            + describeDetailCount;
    verify(statements, times(statementCount)).executeStatement(any(ExecuteStatementRequest.class));
  }

  @Test
  public void ddlsTask_runsShowCreateTableForEveryTableInAllMode() throws Exception {
    List<ObjectNode> outputs = neededAndOtherTables();
    ImmutableMap.Builder<String, StatementResponse> responses = responsesFor(outputs);
    List<String> expected = new ArrayList<>();
    for (JsonNode output : outputs) {
      String tableName = output.get("table_name").asText();
      String ddl = "CREATE TABLE hive_metastore.legacy." + tableName + " (id INT)";
      responses.put(showCreateTable(tableName), succeeded(ddl));
      expected.add(tableName + "|" + ddl);
    }
    StatementExecutionAPI statements = mockStatements(responses.build());

    List<CSVRecord> records = runDdlsTask(statements, ShowCreateTableMode.ALL);

    List<String> written = new ArrayList<>();
    for (CSVRecord record : records) {
      written.add(record.get(DdlsHeader.TableName) + "|" + record.get(DdlsHeader.Ddl));
    }
    assertEquals(expected, written);
  }

  @Test
  public void ddlsTask_runsShowCreateTableForTableWhoseDescribeFails() throws Exception {
    String ddl = "CREATE TABLE hive_metastore.legacy.locked (id INT) USING delta";
    StatementExecutionAPI statements =
        mockStatements(
            responsesFor(ImmutableList.of(deltaTable(SCHEMA, "plain", 2)))
                .put(SHOW_TABLES, showTablesResponse(SCHEMA, "plain", "locked"))
                .put(describeStatement(SCHEMA, "locked"), failed("[INTERNAL_ERROR] Timed out."))
                .put(showCreateTable("locked"), succeeded(ddl))
                .buildKeepingLast());

    List<CSVRecord> records = runDdlsTask(statements, ShowCreateTableMode.NEEDED);

    assertEquals(1, records.size());
    assertEquals("locked", records.get(0).get(DdlsHeader.TableName));
    assertEquals("", records.get(0).get(DdlsHeader.TableType));
    assertEquals(ddl, records.get(0).get(DdlsHeader.Ddl));
  }

  @Test
  public void ddlsTask_runsShowCreateTableForDeltaTableWhoseDescribeDetailFails() throws Exception {
    String ddl =
        "CREATE TABLE hive_metastore.legacy.events (dt DATE) USING delta PARTITIONED BY (dt)";
    StatementExecutionAPI statements =
        mockStatements(
            responsesFor(plainDeltaTables("plain", "events"))
                .put(
                    describeDetailStatement(SCHEMA, "events"),
                    failed("[INSUFFICIENT_PERMISSIONS] Insufficient privileges."))
                .put(showCreateTable("events"), succeeded(ddl))
                .buildKeepingLast());

    List<CSVRecord> records = runDdlsTask(statements, ShowCreateTableMode.NEEDED);

    assertEquals(1, records.size());
    assertEquals("events", records.get(0).get(DdlsHeader.TableName));
    assertEquals("MANAGED", records.get(0).get(DdlsHeader.TableType));
    assertEquals(ddl, records.get(0).get(DdlsHeader.Ddl));
  }

  @Test
  public void ddlsTask_skipsTableWithUnsupportedSerde() throws Exception {
    String deltaDdl = "CREATE TABLE hive_metastore.legacy.delta_tbl (id INT NOT NULL) USING delta";
    StatementExecutionAPI statements =
        mockStatements(
            responsesFor(plainDeltaTables("delta_tbl", "hive_serde_tbl"))
                .put(showCreateTable("delta_tbl"), succeeded(deltaDdl))
                .put(
                    showCreateTable("hive_serde_tbl"),
                    failed(
                        "[UNSUPPORTED_SHOW_CREATE_TABLE.WITH_UNSUPPORTED_SERDE_CONFIGURATION]"
                            + " Unsupported SHOW CREATE TABLE command."))
                .build());

    List<CSVRecord> records = runDdlsTask(statements, ShowCreateTableMode.ALL);

    assertEquals(1, records.size());
    assertEquals(HIVE_METASTORE_CATALOG, records.get(0).get(DdlsHeader.TableCatalog));
    assertEquals(SCHEMA, records.get(0).get(DdlsHeader.TableSchema));
    assertEquals("delta_tbl", records.get(0).get(DdlsHeader.TableName));
    assertEquals("MANAGED", records.get(0).get(DdlsHeader.TableType));
    assertEquals(deltaDdl, records.get(0).get(DdlsHeader.Ddl));
  }

  @Test
  public void ddlsTask_skipsTableWhoseResultDownloadFails() throws Exception {
    String goodDdl = "CREATE TABLE hive_metastore.legacy.good_tbl (id INT) USING delta";
    String brokenStatementId = "stmt-broken";
    StatementResponse brokenResponse =
        new StatementResponse()
            .setStatementId(brokenStatementId)
            .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED))
            .setResult(
                new ResultData()
                    .setChunkIndex(0L)
                    .setNextChunkIndex(1L)
                    .setDataArray(
                        ImmutableList.<Collection<String>>of(ImmutableList.of("CREATE TABLE"))));
    StatementExecutionAPI statements =
        mockStatements(
            responsesFor(plainDeltaTables("broken_tbl", "good_tbl"))
                .put(showCreateTable("broken_tbl"), brokenResponse)
                .put(showCreateTable("good_tbl"), succeeded(goodDdl))
                .build());
    when(statements.getStatementResultChunkN(brokenStatementId, 1L))
        .thenThrow(new DatabricksException("Connection reset"));

    List<CSVRecord> records = runDdlsTask(statements, ShowCreateTableMode.ALL);

    assertEquals(1, records.size());
    assertEquals("good_tbl", records.get(0).get(DdlsHeader.TableName));
    assertEquals(goodDdl, records.get(0).get(DdlsHeader.Ddl));
  }

  @Test
  public void ddlsTask_dropsTrailingLineBreak() throws Exception {
    String ddl = "CREATE TABLE hive_metastore.legacy.delta_tbl (\n  id INT)\nUSING delta";
    StatementExecutionAPI statements =
        mockStatements(
            responsesFor(plainDeltaTables("delta_tbl"))
                .put(showCreateTable("delta_tbl"), succeeded(ddl + "\n"))
                .build());

    List<CSVRecord> records = runDdlsTask(statements, ShowCreateTableMode.ALL);

    assertEquals(1, records.size());
    assertEquals(ddl, records.get(0).get(DdlsHeader.Ddl));
  }

  @Test
  public void ddlsTask_writesHiveMetastoreAsCatalogWhenSparkCatalogIsReported() throws Exception {
    ObjectNode output = deltaTable(SCHEMA, "delta_tbl", 2);
    output.put("catalog_name", "spark_catalog");
    StatementExecutionAPI statements =
        mockStatements(
            responsesFor(ImmutableList.of(output))
                .put(
                    showCreateTable("delta_tbl"),
                    succeeded("CREATE TABLE spark_catalog.legacy.delta_tbl (id INT) USING delta"))
                .build());

    List<CSVRecord> records = runDdlsTask(statements, ShowCreateTableMode.ALL);

    assertEquals(1, records.size());
    assertEquals(HIVE_METASTORE_CATALOG, records.get(0).get(DdlsHeader.TableCatalog));
  }

  @Test
  public void functionDdlsTask_writesDdlOfFunctionsWithBodyOrClass() throws Exception {
    ImmutableList<String> sqlLines =
        ImmutableList.of("Input: x INT", "Returns: INT", "Deterministic: true", "Body: x * 2");
    ImmutableList<String> classLines = ImmutableList.of("Class: com.acme.udf.CustomHashUdf");
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_FUNCTIONS,
                showUserFunctions("double_it", "custom_hash", "no_definition"),
                describeFunction("double_it"),
                succeeded(sqlLines.toArray(new String[0])),
                describeFunction("custom_hash"),
                succeeded(classLines.toArray(new String[0])),
                describeFunction("no_definition"),
                succeeded("Owner: bob")));

    List<CSVRecord> records = runFunctionDdlsTask(statements, newFunctionCache());

    ImmutableList<String> expected =
        ImmutableList.of(
            String.join(
                "|", "double_it", formatCreateFunction(parseFunction("double_it", sqlLines))),
            String.join(
                "|",
                "custom_hash",
                formatCreateFunction(parseFunction("custom_hash", classLines))));
    List<String> written = new ArrayList<>();
    for (CSVRecord record : records) {
      assertEquals(HIVE_METASTORE_CATALOG, record.get(FunctionDdlsHeader.FunctionCatalog));
      assertEquals(SCHEMA, record.get(FunctionDdlsHeader.FunctionSchema));
      written.add(
          String.join(
              "|",
              record.get(FunctionDdlsHeader.FunctionName),
              record.get(FunctionDdlsHeader.Ddl)));
    }
    assertEquals(expected, written);
  }

  @Test
  public void functionDdlsTask_skipsFunctionWhoseDescribeFails() throws Exception {
    ImmutableList<String> describeLines = ImmutableList.of("Returns: INT", "Body: 2");
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_FUNCTIONS,
                showUserFunctions("double_it", "broken_fn"),
                describeFunction("double_it"),
                succeeded(describeLines.toArray(new String[0])),
                describeFunction("broken_fn"),
                failed("[ROUTINE_NOT_FOUND] The routine `legacy`.`broken_fn` cannot be found.")));

    List<CSVRecord> records = runFunctionDdlsTask(statements, newFunctionCache());

    assertEquals(1, records.size());
    assertEquals("double_it", records.get(0).get(FunctionDdlsHeader.FunctionName));
    assertEquals(
        formatCreateFunction(parseFunction("double_it", describeLines)),
        records.get(0).get(FunctionDdlsHeader.Ddl));
  }

  @Test
  public void functionCache_describesFunctionsOnceForFunctionsAndFunctionDdls() throws Exception {
    String describeStatement = describeFunction("double_it");
    StatementExecutionAPI statements =
        mockStatements(
            ImmutableMap.of(
                SHOW_SCHEMAS,
                succeeded(SCHEMA),
                SHOW_FUNCTIONS,
                showUserFunctions("double_it"),
                describeStatement,
                succeeded("Body: 2")));
    HiveMetastoreFunctionCache functionCache = newFunctionCache();

    runTask(
        statements,
        (sink, handle) ->
            new DatabricksHiveMetastoreFunctionsTask(functionCache).doRun(null, sink, handle));
    runFunctionDdlsTask(statements, functionCache);

    int statementCount = ImmutableList.of(SHOW_SCHEMAS, SHOW_FUNCTIONS, describeStatement).size();
    verify(statements, times(statementCount)).executeStatement(any(ExecuteStatementRequest.class));
  }

  private List<CSVRecord> runDdlsTask(StatementExecutionAPI statements, ShowCreateTableMode mode)
      throws Exception {
    HiveMetastoreSessionCache sessionCache = new HiveMetastoreSessionCache();
    return runTask(
        statements,
        (sink, handle) ->
            new DatabricksHiveMetastoreDdlsTask(sessionCache, mode).doRun(null, sink, handle));
  }

  private List<CSVRecord> runFunctionDdlsTask(
      StatementExecutionAPI statements, HiveMetastoreFunctionCache functionCache) throws Exception {
    return runTask(
        statements,
        (sink, handle) ->
            new DatabricksHiveMetastoreFunctionDdlsTask(functionCache).doRun(null, sink, handle));
  }

  private List<CSVRecord> runTask(StatementExecutionAPI statements, TaskBody body)
      throws Exception {
    return DatabricksStatementStubs.runTask(
        tempFolder.newFile(), statements, acceptAllFilter(), body);
  }

  private static HiveMetastoreFunctionCache newFunctionCache() {
    return new HiveMetastoreFunctionCache(new HiveMetastoreSessionCache());
  }

  /**
   * Parses the output of a table. A Delta table gets no partition columns, as if {@code DESCRIBE
   * DETAIL} had reported none.
   */
  private static ParsedTable parse(ObjectNode output) throws IOException {
    ParsedTable table =
        parseDescribeTableJson(SCHEMA, output.get("table_name").asText(), output.toString());
    return table.isDelta() ? table.withPartitionColumns(ImmutableList.of()) : table;
  }

  /**
   * Returns a Delta table with a generated column, which needs {@code SHOW CREATE TABLE}, followed
   * by a Delta table, a Parquet table and a view, which don't.
   */
  private static List<ObjectNode> neededAndOtherTables() {
    ObjectNode generated = deltaTable(SCHEMA, "generated", TABLE_FEATURES_WRITER_VERSION);
    ((ObjectNode) generated.get("table_properties"))
        .put("delta.feature.generatedColumns", "supported");
    return ImmutableList.of(
        generated,
        deltaTable(SCHEMA, "plain", TABLE_FEATURES_WRITER_VERSION),
        table(SCHEMA, "files", "EXTERNAL", "parquet"),
        view(SCHEMA, "plain_ids", "SELECT id FROM legacy.plain"));
  }

  /** Returns Delta tables that don't need {@code SHOW CREATE TABLE}. */
  private static List<ObjectNode> plainDeltaTables(String... tableNames) {
    List<ObjectNode> outputs = new ArrayList<>();
    for (String tableName : tableNames) {
      outputs.add(deltaTable(SCHEMA, tableName, 2));
    }
    return outputs;
  }

  private static ImmutableMap.Builder<String, StatementResponse> responsesFor(
      List<ObjectNode> outputs) {
    return DescribeTableJson.responsesFor(SCHEMA, outputs);
  }

  private static String showCreateTable(String tableName) {
    return "SHOW CREATE TABLE `" + SCHEMA + "`.`" + tableName + "`";
  }

  /** Returns the {@code SHOW USER FUNCTIONS} result of functions of the schema. */
  private static StatementResponse showUserFunctions(String... functionNames) {
    List<String> qualifiedNames = new ArrayList<>();
    for (String functionName : functionNames) {
      qualifiedNames.add("spark_catalog." + SCHEMA + "." + functionName);
    }
    return succeeded(qualifiedNames.toArray(new String[0]));
  }

  private static String describeFunction(String functionName) {
    return "DESCRIBE FUNCTION EXTENDED `" + SCHEMA + "`.`" + functionName + "`";
  }

  private static ParsedFunction parseFunction(String functionName, List<String> lines) {
    return DatabricksHiveMetastoreFunctionParser.parseDescribeFunctionExtended(
        HIVE_METASTORE_CATALOG, SCHEMA, functionName, lines);
  }
}
