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
package com.google.edwmigration.dumper.application.dumper.connector.databricks.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TBoolColumn;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TCLIService;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TCloseOperationResp;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TCloseSessionResp;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TColumn;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TColumnDesc;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TColumnValue;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TExecuteStatementReq;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TExecuteStatementResp;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TFetchResultsResp;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TGetOperationStatusResp;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TGetResultSetMetadataResp;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.THandleIdentifier;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TI32Value;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TI64Column;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TOpenSessionReq;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TOpenSessionResp;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TOperationHandle;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TOperationState;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TOperationType;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TPrimitiveTypeEntry;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TProtocolVersion;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TRow;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TRowSet;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TSessionHandle;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TSparkDirectResults;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TStatus;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TStatusCode;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TStringColumn;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TStringValue;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TTableSchema;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TTypeDesc;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TTypeEntry;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TTypeId;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class ThriftSqlRunnerTest {

  private static final String CLUSTER_ID = "0123-456789-abcdefgh";
  private static final String WORKSPACE_ID = "1234567890";
  private static final TStatus SUCCESS = new TStatus(TStatusCode.SUCCESS_STATUS);

  private final List<String> requestPaths = new ArrayList<>();
  private final List<String> authorizations = new ArrayList<>();
  private HttpServer server;

  @After
  public void stopServer() {
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  public void toRows_readsColumnsWithNullBitmaps() {
    // Bit 1 of the bitmap of the string column marks its second value as null.
    TRowSet rowSet =
        new TRowSet(0, new ArrayList<>())
            .setColumns(
                ImmutableList.of(
                    TColumn.stringVal(
                        new TStringColumn(
                            Arrays.asList("a", ""), ByteBuffer.wrap(new byte[] {0b10}))),
                    TColumn.boolVal(
                        new TBoolColumn(
                            Arrays.asList(true, false), ByteBuffer.wrap(new byte[] {0}))),
                    TColumn.i64Val(
                        new TI64Column(Arrays.asList(1L, 1024L), ByteBuffer.wrap(new byte[0])))));

    assertEquals(
        ImmutableList.of(Arrays.asList("a", "true", "1"), Arrays.asList(null, "false", "1024")),
        ThriftSqlRunner.toRows(rowSet));
  }

  @Test
  public void toRows_readsRowsOfOldProtocolVersions() {
    TRowSet rowSet =
        new TRowSet(
            0,
            ImmutableList.of(
                new TRow(
                    ImmutableList.of(
                        TColumnValue.stringVal(new TStringValue().setValue("x")),
                        TColumnValue.i32Val(new TI32Value())))));

    assertEquals(ImmutableList.of(Arrays.asList("x", null)), ThriftSqlRunner.toRows(rowSet));
  }

  @Test
  public void createClusterUrl_findsWorkspaceIdInAzureHostOrConfig() throws IOException {
    assertEquals(
        "https://adb-2319810376257260.0.azuredatabricks.net/sql/protocolv1/o/2319810376257260/"
            + CLUSTER_ID,
        ThriftSqlRunner.createClusterUrl(
            new DatabricksConfig().setHost("https://adb-2319810376257260.0.azuredatabricks.net/"),
            CLUSTER_ID));
    assertEquals(
        "https://dbc-a1b2.cloud.databricks.com/sql/protocolv1/o/42/" + CLUSTER_ID,
        ThriftSqlRunner.createClusterUrl(
            new DatabricksConfig()
                .setHost("https://dbc-a1b2.cloud.databricks.com")
                .setWorkspaceId("42"),
            CLUSTER_ID));
    assertThrows(
        IOException.class,
        () ->
            ThriftSqlRunner.createClusterUrl(
                new DatabricksConfig().setHost("https://dbc-a1b2.cloud.databricks.com"),
                CLUSTER_ID));
  }

  @Test
  public void thriftRunner_runsStatementsInOneSessionWithTheWorkspaceToken() throws Exception {
    TCLIService.Iface service = mockService();
    List<String> statements = new ArrayList<>();
    when(service.ExecuteStatement(any()))
        .thenAnswer(
            invocation -> {
              String statement = invocation.<TExecuteStatementReq>getArgument(0).getStatement();
              statements.add(statement);
              return new TExecuteStatementResp(SUCCESS)
                  .setOperationHandle(operation(!statement.startsWith("USE CATALOG")));
            });
    when(service.GetResultSetMetadata(any()))
        .thenReturn(
            new TGetResultSetMetadataResp(SUCCESS)
                .setSchema(
                    new TTableSchema(
                        ImmutableList.of(column("database", 1), column("tableName", 2)))));
    when(service.FetchResults(any()))
        .thenReturn(
            new TFetchResultsResp(SUCCESS)
                .setHasMoreRows(false)
                .setResults(
                    new TRowSet(0, new ArrayList<>())
                        .setColumns(
                            ImmutableList.of(
                                TColumn.stringVal(
                                    new TStringColumn(
                                        Arrays.asList("s", "s"), ByteBuffer.wrap(new byte[0]))),
                                TColumn.stringVal(
                                    new TStringColumn(
                                        Arrays.asList("t1", "t2"),
                                        ByteBuffer.wrap(new byte[0])))))));
    ThriftSqlRunner runner = newRunner(startServer(service));

    StatementResult tables = runner.executeSqlInCatalog("hive_metastore", "SHOW TABLES IN `s`");
    runner.close();

    assertEquals(ImmutableList.of("database", "tableName"), tables.columnNames());
    assertEquals(ImmutableList.of("s", "t1"), tables.nextRow());
    assertEquals(ImmutableList.of("s", "t2"), tables.nextRow());
    assertEquals(
        ImmutableList.of("USE CATALOG `hive_metastore`", "SHOW TABLES IN `s`"), statements);
    assertTrue(
        authorizations.toString(), authorizations.stream().allMatch("Bearer test-token"::equals));
    assertTrue(
        requestPaths.toString(),
        requestPaths.stream()
            .allMatch(("/sql/protocolv1/o/" + WORKSPACE_ID + "/" + CLUSTER_ID)::equals));
    verify(service).CloseSession(any());
  }

  @Test
  public void thriftRunner_readsDirectResultsInOneRequest() throws Exception {
    TCLIService.Iface service = mockService();
    List<TExecuteStatementReq> requests = new ArrayList<>();
    TFetchResultsResp rows =
        new TFetchResultsResp(SUCCESS)
            .setHasMoreRows(false)
            .setResults(
                new TRowSet(0, new ArrayList<>())
                    .setColumns(
                        ImmutableList.of(
                            TColumn.stringVal(
                                new TStringColumn(
                                    Arrays.asList("{\"table_name\":\"t\"}"),
                                    ByteBuffer.wrap(new byte[0]))))));
    when(service.ExecuteStatement(any()))
        .thenAnswer(
            invocation -> {
              TExecuteStatementReq request = invocation.getArgument(0);
              requests.add(request);
              boolean hasResultSet = !request.getStatement().startsWith("USE CATALOG");
              TSparkDirectResults directResults =
                  new TSparkDirectResults()
                      .setOperationStatus(
                          new TGetOperationStatusResp(SUCCESS)
                              .setOperationState(TOperationState.FINISHED_STATE))
                      .setCloseOperation(new TCloseOperationResp(SUCCESS));
              if (hasResultSet) {
                directResults
                    .setResultSetMetadata(
                        new TGetResultSetMetadataResp(SUCCESS)
                            .setSchema(
                                new TTableSchema(ImmutableList.of(column("json_metadata", 1)))))
                    .setResultSet(rows);
              }
              return new TExecuteStatementResp(SUCCESS)
                  .setOperationHandle(operation(hasResultSet))
                  .setDirectResults(directResults);
            });
    ThriftSqlRunner runner = newRunner(startServer(service));

    StatementResult description =
        runner.executeSqlInCatalog("hive_metastore", "DESCRIBE TABLE EXTENDED `s`.`t` AS JSON");
    runner.close();

    assertEquals(ImmutableList.of("json_metadata"), description.columnNames());
    assertEquals(ImmutableList.of("{\"table_name\":\"t\"}"), description.nextRow());
    // Direct results need a Databricks protocol version.
    ArgumentCaptor<TOpenSessionReq> openSession = ArgumentCaptor.forClass(TOpenSessionReq.class);
    verify(service).OpenSession(openSession.capture());
    assertEquals(
        TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V1,
        openSession.getValue().getClient_protocol());
    assertEquals(2, requests.size());
    for (TExecuteStatementReq request : requests) {
      assertTrue(request.getStatement(), request.isSetGetDirectResults());
    }
    // OpenSession, USE CATALOG, the statement and CloseSession: one request per statement.
    assertEquals(4, requestPaths.size());
    verify(service, never()).GetResultSetMetadata(any());
    verify(service, never()).FetchResults(any());
    verify(service, never()).CloseOperation(any());
  }

  @Test
  public void thriftRunner_fetchesRowsAfterDirectResults() throws Exception {
    TCLIService.Iface service = mockService();
    // The direct results have the first rows and leave the operation open for the other rows.
    when(service.ExecuteStatement(any()))
        .thenAnswer(
            invocation -> {
              TExecuteStatementReq request = invocation.getArgument(0);
              boolean hasResultSet = !request.getStatement().startsWith("USE CATALOG");
              TSparkDirectResults directResults =
                  new TSparkDirectResults()
                      .setOperationStatus(
                          new TGetOperationStatusResp(SUCCESS)
                              .setOperationState(TOperationState.FINISHED_STATE));
              if (hasResultSet) {
                directResults
                    .setResultSetMetadata(
                        new TGetResultSetMetadataResp(SUCCESS)
                            .setSchema(new TTableSchema(ImmutableList.of(column("id", 1)))))
                    .setResultSet(idRows(true, 0L, 1L));
              } else {
                directResults.setCloseOperation(new TCloseOperationResp(SUCCESS));
              }
              return new TExecuteStatementResp(SUCCESS)
                  .setOperationHandle(operation(hasResultSet))
                  .setDirectResults(directResults);
            });
    when(service.FetchResults(any())).thenReturn(idRows(false, 2L));
    ThriftSqlRunner runner = newRunner(startServer(service));

    StatementResult ids =
        runner.executeSqlInCatalog("hive_metastore", "SELECT id FROM range(3) ORDER BY id");
    runner.close();

    List<List<String>> rows = new ArrayList<>();
    for (List<String> row = ids.nextRow(); row != null; row = ids.nextRow()) {
      rows.add(row);
    }
    assertEquals(
        ImmutableList.of(ImmutableList.of("0"), ImmutableList.of("1"), ImmutableList.of("2")),
        rows);
    verify(service).FetchResults(any());
    verify(service).CloseOperation(any());
  }

  @Test
  public void thriftRunner_throwsErrorMessageOfFailedStatement() throws Exception {
    TCLIService.Iface service = mockService();
    when(service.ExecuteStatement(any()))
        .thenReturn(
            new TExecuteStatementResp(
                new TStatus(TStatusCode.ERROR_STATUS)
                    .setSqlState("42601")
                    .setErrorMessage("[PARSE_SYNTAX_ERROR] Syntax error at or near 'JSON'.")));
    ThriftSqlRunner runner = newRunner(startServer(service));

    IOException error =
        assertThrows(
            IOException.class,
            () ->
                runner.executeSqlInCatalog("hive_metastore", "DESCRIBE TABLE EXTENDED t AS JSON"));
    runner.close();

    assertTrue(error.getMessage(), error.getMessage().contains("[PARSE_SYNTAX_ERROR]"));
  }

  private static TCLIService.Iface mockService() throws TException {
    TCLIService.Iface service = mock(TCLIService.Iface.class);
    // Like a cluster, the server answers with the protocol version of the client.
    when(service.OpenSession(any()))
        .thenAnswer(
            invocation ->
                new TOpenSessionResp(
                        SUCCESS, invocation.<TOpenSessionReq>getArgument(0).getClient_protocol())
                    .setSessionHandle(new TSessionHandle(handle())));
    when(service.CloseOperation(any())).thenReturn(new TCloseOperationResp(SUCCESS));
    when(service.CloseSession(any())).thenReturn(new TCloseSessionResp(SUCCESS));
    return service;
  }

  /** Starts an HTTP server that answers Thrift requests with {@code service}, like a cluster. */
  private int startServer(TCLIService.Iface service) throws IOException {
    TCLIService.Processor<TCLIService.Iface> processor = new TCLIService.Processor<>(service);
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          if (!"POST".equals(exchange.getRequestMethod())) {
            // The SDK probes the host for its configuration with GET requests.
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
            return;
          }

          requestPaths.add(exchange.getRequestURI().getPath());
          authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
          ByteArrayOutputStream response = new ByteArrayOutputStream();
          try {
            TBinaryProtocol protocol =
                new TBinaryProtocol(new TIOStreamTransport(exchange.getRequestBody(), response));
            processor.process(protocol, protocol);
          } catch (TException e) {
            throw new IOException(e);
          }
          exchange.getResponseHeaders().set("Content-Type", "application/x-thrift");
          exchange.sendResponseHeaders(200, response.size());
          exchange.getResponseBody().write(response.toByteArray());
          exchange.close();
        });
    server.start();
    return server.getAddress().getPort();
  }

  private static ThriftSqlRunner newRunner(int port) throws IOException {
    return new ThriftSqlRunner(
        new WorkspaceClient(
            new DatabricksConfig()
                .setHost("http://localhost:" + port)
                .setToken("test-token")
                .setAuthType("pat")
                .setWorkspaceId(WORKSPACE_ID)),
        CLUSTER_ID);
  }

  private static THandleIdentifier handle() {
    return new THandleIdentifier(ByteBuffer.wrap(new byte[16]), ByteBuffer.wrap(new byte[16]));
  }

  private static TOperationHandle operation(boolean hasResultSet) {
    return new TOperationHandle(handle(), TOperationType.EXECUTE_STATEMENT, hasResultSet);
  }

  /** Returns a batch of the rows of a bigint column {@code id}. */
  private static TFetchResultsResp idRows(boolean hasMoreRows, Long... ids) {
    return new TFetchResultsResp(SUCCESS)
        .setHasMoreRows(hasMoreRows)
        .setResults(
            new TRowSet(0, new ArrayList<>())
                .setColumns(
                    ImmutableList.of(
                        TColumn.i64Val(
                            new TI64Column(Arrays.asList(ids), ByteBuffer.wrap(new byte[0]))))));
  }

  private static TColumnDesc column(String name, int position) {
    return new TColumnDesc(
        name,
        new TTypeDesc(
            ImmutableList.of(
                TTypeEntry.primitiveEntry(new TPrimitiveTypeEntry(TTypeId.STRING_TYPE)))),
        position);
  }
}
