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

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.client.DatabricksSqlHelper.SqlQuoting.quoteIdentifier;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TCLIService;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TCloseOperationReq;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TCloseSessionReq;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TColumn;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TColumnDesc;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TColumnValue;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TExecuteStatementReq;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TExecuteStatementResp;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TFetchOrientation;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TFetchResultsReq;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TFetchResultsResp;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TGetOperationStatusReq;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TGetOperationStatusResp;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TGetResultSetMetadataReq;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TGetResultSetMetadataResp;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TOpenSessionReq;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TOpenSessionResp;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TOperationHandle;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TOperationState;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TProtocolVersion;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TRow;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TRowSet;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TSessionHandle;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TSparkDirectResults;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TSparkGetDirectResults;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TStatus;
import com.google.edwmigration.dumper.ext.hive.service.thrift.api.v3_1_3.TStatusCode;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.THttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes SQL statements on a running all-purpose cluster through its Thrift (HiveServer2)
 * endpoint, which the JDBC and ODBC drivers use, so that no SQL warehouse is needed. The statements
 * run one at a time in one session, which {@link #close()} closes, and their results are fetched in
 * batches, without a row limit. The catalog of a statement is set with {@code USE CATALOG} when it
 * changes.
 */
@ThreadSafe
public final class ThriftSqlRunner implements DatabricksSqlRunner {

  private static final Logger logger = LoggerFactory.getLogger(ThriftSqlRunner.class);
  // The HTTP path of the Thrift endpoint of an all-purpose cluster, after the workspace URL.
  private static final String CLUSTER_PATH_FORMAT = "%s/sql/protocolv1/o/%s/%s";
  // The workspace ID is in the host of an Azure or a GCP workspace, such as
  // adb-1234567890123456.7.azuredatabricks.net or 1234567890123456.7.gcp.databricks.com.
  private static final Pattern HOST_WORKSPACE_ID_PATTERN =
      Pattern.compile(
          "^https?://(?:adb-)?(\\d+)\\.\\d+\\.(?:azuredatabricks\\.net|gcp\\.databricks\\.com)");
  private static final int FETCH_ROWS = 10_000;
  // The first Databricks protocol version, which adds the direct results of a statement. The
  // server answers with the lower of this version and its own, and an older Hive version, such as
  // the default one, gets no direct results.
  private static final TProtocolVersion CLIENT_PROTOCOL =
      TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V1;
  private static final int CONNECT_TIMEOUT_MILLIS = 60_000;
  private static final int READ_TIMEOUT_MILLIS = 30 * 60_000;
  private static final Duration STATEMENT_TIMEOUT = Duration.ofMinutes(30);
  private static final long MIN_POLL_MILLIS = 50L;
  private static final long MAX_POLL_MILLIS = 1_000L;

  private final DatabricksConfig config;
  private final String url;
  private final Object lock = new Object();

  @GuardedBy("lock")
  private THttpClient transport;

  @GuardedBy("lock")
  private TCLIService.Client client;

  @GuardedBy("lock")
  private TSessionHandle session;

  @GuardedBy("lock")
  private String currentCatalog;

  /**
   * Creates the runner of the cluster {@code clusterId} of the workspace of {@code
   * workspaceClient}, which authenticates the requests. No request is made before the first
   * statement.
   */
  public ThriftSqlRunner(WorkspaceClient workspaceClient, String clusterId) throws IOException {
    Preconditions.checkNotNull(workspaceClient, "WorkspaceClient was null.");
    Preconditions.checkNotNull(clusterId, "Cluster ID was null.");
    this.config = workspaceClient.config();
    this.url = createClusterUrl(this.config, clusterId);
  }

  /** Returns the URL of the Thrift endpoint of a cluster. */
  @VisibleForTesting
  static String createClusterUrl(DatabricksConfig config, String clusterId) throws IOException {
    String host = Strings.nullToEmpty(config.getHost()).replaceAll("/+$", "");
    String workspaceId = config.getWorkspaceId();
    if (Strings.isNullOrEmpty(workspaceId)) {
      Matcher matcher = HOST_WORKSPACE_ID_PATTERN.matcher(host);
      if (!matcher.find()) {
        throw new IOException(
            String.format(
                "Can't find the workspace ID of %s for the Thrift endpoint of the cluster. Set the"
                    + " DATABRICKS_WORKSPACE_ID environment variable.",
                host));
      }
      workspaceId = matcher.group(1);
    }
    return String.format(CLUSTER_PATH_FORMAT, host, workspaceId, clusterId);
  }

  @Override
  public StatementResult executeSqlInCatalog(String catalog, String sql) throws IOException {
    synchronized (this.lock) {
      try {
        if (!catalog.equals(this.currentCatalog)) {
          execute("USE CATALOG " + quoteIdentifier(catalog));
          this.currentCatalog = catalog;
        }

        return execute(sql);
      } catch (TException e) {
        throw new IOException(
            String.format("Thrift request to %s failed: %s", this.url, e.getMessage()), e);
      }
    }
  }

  /** Closes the session, if any, ignoring failures. */
  @Override
  public void close() {
    synchronized (this.lock) {
      if (this.session != null) {
        try {
          authenticate();
          this.client.CloseSession(new TCloseSessionReq(this.session));
        } catch (TException | RuntimeException e) {
          logger.debug("Ignored error closing Thrift session of {}: {}", this.url, e.getMessage());
        }
        this.session = null;
      }

      if (this.transport != null) {
        this.transport.close();
        this.transport = null;
        this.client = null;
      }
    }
  }

  @GuardedBy("lock")
  private StatementResult execute(String sql) throws IOException, TException {
    TSessionHandle sessionHandle = findSession();
    authenticate();
    // The statement runs synchronously, and the response has its direct results: its status, its
    // columns, its first rows and, when they are all the rows, the closing of the operation. Then a
    // statement takes one request.
    TExecuteStatementResp response =
        this.client.ExecuteStatement(
            new TExecuteStatementReq(sessionHandle, sql)
                .setGetDirectResults(new TSparkGetDirectResults(FETCH_ROWS)));
    checkStatus(response.getStatus(), sql);
    TOperationHandle operation = response.getOperationHandle();
    TSparkDirectResults directResults = response.getDirectResults();
    boolean closed = directResults != null && directResults.isSetCloseOperation();
    try {
      if (directResults != null && directResults.isSetOperationStatus()) {
        waitUntilFinished(operation, directResults.getOperationStatus(), sql);
      }

      if (!operation.isHasResultSet()) {
        return new ListStatementResult(ImmutableList.of(), ImmutableList.of());
      }

      ImmutableList<String> columnNames =
          directResults != null && directResults.isSetResultSetMetadata()
              ? toColumnNames(directResults.getResultSetMetadata(), sql)
              : fetchColumnNames(operation, sql);
      List<List<String>> rows = new ArrayList<>();
      boolean mayHaveMoreRows = !closed;
      if (directResults != null && directResults.isSetResultSet()) {
        TFetchResultsResp firstRows = directResults.getResultSet();
        checkStatus(firstRows.getStatus(), sql);
        List<List<String>> batch = toRows(firstRows.getResults());
        rows.addAll(batch);
        mayHaveMoreRows &= firstRows.isHasMoreRows() || batch.size() >= FETCH_ROWS;
      }

      if (mayHaveMoreRows) {
        rows.addAll(fetchRows(operation, sql));
      }
      return new ListStatementResult(columnNames, rows);
    } finally {
      if (!closed) {
        closeQuietly(operation);
      }
    }
  }

  /**
   * Waits until a statement, which ran synchronously but can still run on the server, finishes, and
   * throws its error message if it fails.
   */
  @GuardedBy("lock")
  private void waitUntilFinished(
      TOperationHandle operation, TGetOperationStatusResp status, String sql)
      throws IOException, TException {
    long deadlineNanos = System.nanoTime() + STATEMENT_TIMEOUT.toNanos();
    long sleepMillis = MIN_POLL_MILLIS;
    while (true) {
      checkStatus(status.getStatus(), sql);
      TOperationState state = status.getOperationState();
      if (state == null || state == TOperationState.FINISHED_STATE) {
        return;
      }

      if (state != TOperationState.INITIALIZED_STATE
          && state != TOperationState.PENDING_STATE
          && state != TOperationState.RUNNING_STATE) {
        throw new IOException(
            String.format(
                "SQL statement ended in %s (SQLSTATE %s): %s",
                state, status.getSqlState(), Strings.nullToEmpty(status.getErrorMessage()).trim()));
      }

      if (System.nanoTime() - deadlineNanos >= 0L) {
        throw new IOException(
            String.format(
                "Timed out after %s waiting for SQL statement: %s", STATEMENT_TIMEOUT, sql));
      }

      sleepUninterruptibly(sleepMillis, MILLISECONDS);
      sleepMillis = min(sleepMillis * 2, MAX_POLL_MILLIS);
      authenticate();
      status = this.client.GetOperationStatus(new TGetOperationStatusReq(operation));
    }
  }

  @GuardedBy("lock")
  private TSessionHandle findSession() throws IOException, TException {
    if (this.session == null) {
      this.transport = new THttpClient(this.url);
      this.transport.setConnectTimeout(CONNECT_TIMEOUT_MILLIS);
      this.transport.setReadTimeout(READ_TIMEOUT_MILLIS);
      this.client = new TCLIService.Client(new TBinaryProtocol(this.transport));
      authenticate();
      TOpenSessionResp response = this.client.OpenSession(new TOpenSessionReq(CLIENT_PROTOCOL));
      checkStatus(response.getStatus(), "OpenSession");
      this.session = response.getSessionHandle();
      logger.debug(
          "Opened Thrift session of {} with protocol {}.",
          this.url,
          response.getServerProtocolVersion());
    }

    return this.session;
  }

  @GuardedBy("lock")
  private ImmutableList<String> fetchColumnNames(TOperationHandle operation, String sql)
      throws IOException, TException {
    authenticate();
    return toColumnNames(
        this.client.GetResultSetMetadata(new TGetResultSetMetadataReq(operation)), sql);
  }

  private static ImmutableList<String> toColumnNames(TGetResultSetMetadataResp response, String sql)
      throws IOException {
    checkStatus(response.getStatus(), sql);
    ImmutableList.Builder<String> columnNames = ImmutableList.builder();
    for (TColumnDesc column : response.getSchema().getColumns()) {
      columnNames.add(column.getColumnName());
    }
    return columnNames.build();
  }

  /**
   * Fetches the rows of a statement in batches. A full batch is followed by another fetch even if
   * {@code hasMoreRows} is false, because some HiveServer2 servers always return false.
   */
  @GuardedBy("lock")
  private List<List<String>> fetchRows(TOperationHandle operation, String sql)
      throws IOException, TException {
    List<List<String>> rows = new ArrayList<>();
    while (true) {
      authenticate();
      TFetchResultsResp response =
          this.client.FetchResults(
              new TFetchResultsReq(operation, TFetchOrientation.FETCH_NEXT, FETCH_ROWS));
      checkStatus(response.getStatus(), sql);
      List<List<String>> batch = toRows(response.getResults());
      rows.addAll(batch);
      if (batch.isEmpty() || (!response.isHasMoreRows() && batch.size() < FETCH_ROWS)) {
        return rows;
      }
    }
  }

  @GuardedBy("lock")
  private void closeQuietly(TOperationHandle operation) {
    try {
      authenticate();
      this.client.CloseOperation(new TCloseOperationReq(operation));
    } catch (TException | RuntimeException e) {
      logger.debug("Ignored error closing Thrift operation of {}: {}", this.url, e.getMessage());
    }
  }

  /** Sets the authentication headers of the next request, which can refresh an OAuth token. */
  @GuardedBy("lock")
  private void authenticate() {
    this.transport.setCustomHeaders(this.config.authenticate());
  }

  private static void checkStatus(TStatus status, String sql) throws IOException {
    TStatusCode code = status.getStatusCode();
    if (code == TStatusCode.ERROR_STATUS || code == TStatusCode.INVALID_HANDLE_STATUS) {
      throw new IOException(
          String.format(
              "SQL statement failed with %s (SQLSTATE %s): %s",
              code, status.getSqlState(), Strings.nullToEmpty(status.getErrorMessage()).trim()));
    }
  }

  /**
   * Returns the rows of a row set, with each cell as text, from its columns or, for an old protocol
   * version, from its rows.
   */
  @VisibleForTesting
  static List<List<String>> toRows(TRowSet rowSet) {
    List<List<String>> rows = new ArrayList<>();
    if (rowSet == null) {
      return rows;
    }

    if (rowSet.isSetColumns() && !rowSet.getColumns().isEmpty()) {
      List<List<String>> columns = new ArrayList<>();
      for (TColumn column : rowSet.getColumns()) {
        columns.add(toCellTexts(column));
      }
      int rowCount = columns.get(0).size();
      for (int index = 0; index < rowCount; index++) {
        List<String> row = new ArrayList<>(columns.size());
        for (List<String> column : columns) {
          row.add(column.get(index));
        }
        // The cells can be null, which ImmutableList doesn't allow.
        rows.add(Collections.unmodifiableList(row));
      }
      return rows;
    }

    for (TRow row : rowSet.getRows()) {
      List<String> cells = new ArrayList<>(row.getColValsSize());
      for (TColumnValue value : row.getColVals()) {
        cells.add(toCellText(value));
      }
      rows.add(Collections.unmodifiableList(cells));
    }
    return rows;
  }

  /** Returns the cells of a column as text, null where the null bitmap of the column has a 1. */
  private static List<String> toCellTexts(TColumn column) {
    List<?> values;
    byte[] nulls;
    switch (column.getSetField()) {
      case BOOL_VAL:
        values = column.getBoolVal().getValues();
        nulls = column.getBoolVal().getNulls();
        break;
      case BYTE_VAL:
        values = column.getByteVal().getValues();
        nulls = column.getByteVal().getNulls();
        break;
      case I16_VAL:
        values = column.getI16Val().getValues();
        nulls = column.getI16Val().getNulls();
        break;
      case I32_VAL:
        values = column.getI32Val().getValues();
        nulls = column.getI32Val().getNulls();
        break;
      case I64_VAL:
        values = column.getI64Val().getValues();
        nulls = column.getI64Val().getNulls();
        break;
      case DOUBLE_VAL:
        values = column.getDoubleVal().getValues();
        nulls = column.getDoubleVal().getNulls();
        break;
      case STRING_VAL:
        values = column.getStringVal().getValues();
        nulls = column.getStringVal().getNulls();
        break;
      case BINARY_VAL:
        values = column.getBinaryVal().getValues();
        nulls = column.getBinaryVal().getNulls();
        break;
      default:
        throw new IllegalStateException("Unknown column type: " + column.getSetField());
    }

    List<String> texts = new ArrayList<>(values.size());
    for (int index = 0; index < values.size(); index++) {
      boolean isNull =
          nulls != null && index / 8 < nulls.length && (nulls[index / 8] & (1 << (index % 8))) != 0;
      texts.add(isNull ? null : toText(values.get(index)));
    }
    return texts;
  }

  private static String toCellText(TColumnValue value) {
    switch (value.getSetField()) {
      case BOOL_VAL:
        return value.getBoolVal().isSetValue() ? toText(value.getBoolVal().isValue()) : null;
      case BYTE_VAL:
        return value.getByteVal().isSetValue() ? toText(value.getByteVal().getValue()) : null;
      case I16_VAL:
        return value.getI16Val().isSetValue() ? toText(value.getI16Val().getValue()) : null;
      case I32_VAL:
        return value.getI32Val().isSetValue() ? toText(value.getI32Val().getValue()) : null;
      case I64_VAL:
        return value.getI64Val().isSetValue() ? toText(value.getI64Val().getValue()) : null;
      case DOUBLE_VAL:
        return value.getDoubleVal().isSetValue() ? toText(value.getDoubleVal().getValue()) : null;
      case STRING_VAL:
        return value.getStringVal().getValue();
      default:
        throw new IllegalStateException("Unknown value type: " + value.getSetField());
    }
  }

  /** Returns a cell as text: binary as Base64, and other values with {@code toString()}. */
  private static String toText(Object value) {
    if (value instanceof ByteBuffer) {
      ByteBuffer bytes = ((ByteBuffer) value).duplicate();
      byte[] array = new byte[bytes.remaining()];
      bytes.get(array);
      return Base64.getEncoder().encodeToString(array);
    }

    return String.valueOf(value);
  }

  /** The rows of a statement, which are all fetched before the statement is closed. */
  private static final class ListStatementResult implements StatementResult {

    private final ImmutableList<String> columnNames;
    private final Iterator<List<String>> rows;

    private ListStatementResult(ImmutableList<String> columnNames, List<List<String>> rows) {
      this.columnNames = columnNames;
      this.rows = rows.iterator();
    }

    @Override
    public ImmutableList<String> columnNames() {
      return this.columnNames;
    }

    @Override
    public List<String> nextRow() {
      return this.rows.hasNext() ? this.rows.next() : null;
    }
  }
}
