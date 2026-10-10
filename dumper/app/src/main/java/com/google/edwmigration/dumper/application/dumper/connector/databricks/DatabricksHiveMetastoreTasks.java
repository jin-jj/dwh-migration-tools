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

import static com.google.common.base.CharMatcher.whitespace;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksFilter.HIVE_METASTORE_CATALOG;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDdlParser.parseColumnClauses;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.parseDeltaPartitionColumns;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.parseDescribeTableJson;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.undescribedTable;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksJson.toJsonOrEmpty;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.cell;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.cellOrEmpty;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.quoteIdentifier;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.io.ByteSink;
import com.google.common.primitives.Ints;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreDdlParser.ParsedColumnClauses;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.ParsedColumn;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.ParsedTable;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.ParsedTableConstraint;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataConnector.DatabricksProperty;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataConnector.ShowCreateTableMode;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.CatalogsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.ColumnsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.SchemataHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.TableConstraintsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.TablesHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.ViewsHeader;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extraction tasks for the legacy {@code hive_metastore} catalog using {@code SHOW SCHEMAS}, {@code
 * DESCRIBE SCHEMA EXTENDED}, {@code SHOW TABLES}, {@code DESCRIBE TABLE EXTENDED ... AS JSON},
 * {@code DESCRIBE DETAIL} for the partition columns of Delta tables, and {@code SHOW CREATE TABLE}
 * for the tables whose DDL has clauses that the other statements don't report.
 */
final class DatabricksHiveMetastoreTasks {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksHiveMetastoreTasks.class);

  /** The prefix and suffix of the {@code Properties} of {@code DESCRIBE SCHEMA EXTENDED}. */
  private static final String SCHEMA_PROPERTIES_PREFIX = "((";

  private static final String SCHEMA_PROPERTIES_SUFFIX = "))";

  /** The separator between two {@code (key,value)} entries of schema properties. */
  private static final String SCHEMA_PROPERTIES_SEPARATOR = "), (";

  /** The statement that describes each table and view, for log messages. */
  private static final String DESCRIBE_TABLE_STATEMENT = "DESCRIBE TABLE EXTENDED AS JSON";

  /** The column of {@code DESCRIBE DETAIL} with the partition columns of a Delta table. */
  private static final String PARTITION_COLUMNS_DETAIL = "partitionColumns";

  /**
   * The lower-case table properties of the Delta table features that allow the column clauses that
   * only the DDL shows: {@code GENERATED ALWAYS AS} and {@code GENERATED ... AS IDENTITY}.
   */
  private static final ImmutableSet<String> DDL_ONLY_FEATURE_PROPERTIES =
      ImmutableSet.of("delta.feature.generatedcolumns", "delta.feature.identitycolumns");

  private static final String MIN_WRITER_VERSION_PROPERTY = "delta.minWriterVersion";

  /**
   * The writer versions of the Delta protocols that allow generated columns (from 4) or identity
   * columns (6) without listing them as table features, which writer version 7 introduced.
   */
  private static final Range<Integer> LEGACY_GENERATED_COLUMNS_WRITER_VERSIONS = Range.closed(4, 6);

  /** The error class of {@code SHOW CREATE TABLE} for a table with an unsupported Hive SerDe. */
  private static final String UNSUPPORTED_SERDE_ERROR_CLASS =
      "UNSUPPORTED_SHOW_CREATE_TABLE.WITH_UNSUPPORTED_SERDE_CONFIGURATION";

  private DatabricksHiveMetastoreTasks() {}

  /**
   * Shared per-connector cache of {@code hive_metastore} schemas, parsed tables and DDLs.
   *
   * <p>Each list is loaded once. A failed load is remembered, so the other tasks that need the list
   * fail fast instead of repeating a scan that already failed.
   */
  static final class HiveMetastoreSessionCache {

    private final ShowCreateTableMode showCreateTableMode;
    private final Memoized<ImmutableList<String>> schemas =
        new Memoized<>("Listing hive_metastore schemas");
    private final Memoized<ImmutableList<ParsedTable>> tables =
        new Memoized<>("Listing hive_metastore tables");
    private final Memoized<ImmutableList<TableDdl>> ddls =
        new Memoized<>("Running SHOW CREATE TABLE for hive_metastore tables");

    /** Creates a cache that runs {@code SHOW CREATE TABLE} only for the tables that need it. */
    HiveMetastoreSessionCache() {
      this(ShowCreateTableMode.NEEDED);
    }

    HiveMetastoreSessionCache(@Nonnull ShowCreateTableMode showCreateTableMode) {
      this.showCreateTableMode = showCreateTableMode;
    }

    @Nonnull
    ImmutableList<String> listSchemas(@Nonnull DatabricksHandle handle) throws IOException {
      return schemas.load(() -> loadSchemas(handle));
    }

    @Nonnull
    ImmutableList<ParsedTable> listParsedTables(@Nonnull DatabricksHandle handle)
        throws IOException {
      return tables.load(() -> loadTables(handle));
    }

    /**
     * Returns the {@code SHOW CREATE TABLE} output of the tables that {@link #needsShowCreateTable}
     * selects, or of every table and view in {@link ShowCreateTableMode#ALL} mode, in the order of
     * the tables.
     *
     * <p>Databricks rejects {@code SHOW CREATE TABLE}, even {@code AS SERDE}, for Hive tables with
     * an unsupported SerDe configuration, such as some {@code RCFILE} and {@code SEQUENCEFILE}
     * tables. Those tables are left out with a warning; the tables file describes their storage.
     */
    @Nonnull
    ImmutableList<TableDdl> listDdls(@Nonnull DatabricksHandle handle) throws IOException {
      return ddls.load(() -> loadDdls(handle));
    }

    @Nonnull
    private ImmutableList<TableDdl> loadDdls(@Nonnull DatabricksHandle handle) throws IOException {
      ImmutableList.Builder<TableDdl> builder = ImmutableList.builder();
      handle
          .statementRunner()
          .runInOrder(
              "SHOW CREATE TABLE",
              selectTablesForShowCreateTable(listParsedTables(handle)),
              DatabricksHiveMetastoreTasks::quoteTableName,
              table -> showCreateTable(handle, table),
              (table, ddl) -> {
                if (!ddl.isEmpty()) {
                  builder.add(new TableDdl(table, ddl));
                }
              });
      return builder.build();
    }

    /** Returns the tables that get {@code SHOW CREATE TABLE} in the mode of this cache. */
    @Nonnull
    private ImmutableList<ParsedTable> selectTablesForShowCreateTable(
        @Nonnull ImmutableList<ParsedTable> tables) {
      if (showCreateTableMode == ShowCreateTableMode.ALL) {
        return tables;
      }

      ImmutableList.Builder<ParsedTable> builder = ImmutableList.builder();
      for (ParsedTable table : tables) {
        if (needsShowCreateTable(table)) {
          builder.add(table);
        }
      }
      ImmutableList<ParsedTable> selectedTables = builder.build();
      logger.info(
          "Running SHOW CREATE TABLE for {} of {} hive_metastore tables and views, whose DDL may have"
              + " clauses that DESCRIBE TABLE doesn't report. -D{}={} runs it for every table and"
              + " view.",
          selectedTables.size(),
          tables.size(),
          DatabricksProperty.SHOW_CREATE_TABLE.getName(),
          ShowCreateTableMode.ALL);
      return selectedTables;
    }

    @Nonnull
    private static ImmutableList<String> loadSchemas(@Nonnull DatabricksHandle handle)
        throws IOException {
      ImmutableList.Builder<String> builder = ImmutableList.builder();
      try (CloseableIterable<List<String>> rows =
          handle.sqlHelper().executeSqlInCatalog(HIVE_METASTORE_CATALOG, "SHOW SCHEMAS")) {
        for (List<String> row : rows) {
          String schemaName = cell(row, 0);
          if (schemaName != null && handle.filter().acceptsSchema(schemaName)) {
            builder.add(schemaName);
          }
        }
      }
      return builder.build();
    }

    /**
     * Lists the tables and views of each schema with {@code SHOW TABLES} and describes each of them
     * with {@code DESCRIBE TABLE EXTENDED ... AS JSON}. A table that can't be described is kept
     * with only its schema and name, so that the tables file still lists it and the DDL file still
     * gets its {@code SHOW CREATE TABLE} output.
     */
    @Nonnull
    private ImmutableList<ParsedTable> loadTables(@Nonnull DatabricksHandle handle)
        throws IOException {
      ImmutableList.Builder<ParsedTable> listedTablesBuilder = ImmutableList.builder();
      handle
          .statementRunner()
          .runInOrder(
              "SHOW TABLES",
              listSchemas(handle),
              DatabricksSqlHelper::quoteIdentifier,
              schema -> showTables(handle, schema),
              (schema, schemaTables) -> listedTablesBuilder.addAll(schemaTables));
      ImmutableList<ParsedTable> listedTables = listedTablesBuilder.build();

      // Keyed by the listed table. ParsedTable doesn't override equals, so this only makes the
      // identity comparison explicit.
      Map<ParsedTable, ParsedTable> describedTables = new IdentityHashMap<>();
      handle
          .statementRunner()
          .runInOrder(
              DESCRIBE_TABLE_STATEMENT,
              listedTables,
              DatabricksHiveMetastoreTasks::quoteTableName,
              table -> describeTable(handle, table),
              describedTables::put);

      ImmutableList.Builder<ParsedTable> builder = ImmutableList.builder();
      for (ParsedTable listedTable : listedTables) {
        ParsedTable table = describedTables.get(listedTable);
        if (table == null) {
          logger.warn(
              "Writing {} with only its name because {} failed for it.",
              quoteTableName(listedTable),
              DESCRIBE_TABLE_STATEMENT);
          builder.add(listedTable);
          continue;
        }

        if (table.columns.isEmpty() && !isView(table)) {
          logger.warn(
              "Table {} has no columns in {}.", quoteTableName(table), DESCRIBE_TABLE_STATEMENT);
        }
        builder.add(table);
      }
      return builder.build();
    }

    /** Returns the tables and views of a schema, with only their names. */
    @Nonnull
    private static ImmutableList<ParsedTable> showTables(
        @Nonnull DatabricksHandle handle, @Nonnull String schema) throws IOException {
      String query = "SHOW TABLES IN " + quoteIdentifier(schema);
      ImmutableList.Builder<ParsedTable> builder = ImmutableList.builder();
      try (CloseableIterable<List<String>> rows =
          handle.sqlHelper().executeSqlInCatalog(HIVE_METASTORE_CATALOG, query)) {
        // The columns are the schema, the table name and whether the table is a temporary view,
        // which SHOW TABLES lists in every schema.
        for (List<String> row : rows) {
          String tableName = cellOrEmpty(row, 1);
          if (tableName.isEmpty() || "true".equalsIgnoreCase(cell(row, 2))) {
            continue;
          }

          builder.add(undescribedTable(schema, tableName));
        }
      }
      return builder.build();
    }

    /**
     * Describes a table or view that {@code SHOW TABLES} listed. A Delta table also gets its
     * partition columns from {@code DESCRIBE DETAIL}, because {@code DESCRIBE TABLE ... AS JSON}
     * doesn't report them.
     */
    @Nonnull
    private static ParsedTable describeTable(
        @Nonnull DatabricksHandle handle, @Nonnull ParsedTable listedTable) throws IOException {
      String query = "DESCRIBE TABLE EXTENDED " + quoteTableName(listedTable) + " AS JSON";
      ParsedTable table;
      try (CloseableIterable<List<String>> rows =
          handle.sqlHelper().executeSqlInCatalog(HIVE_METASTORE_CATALOG, query)) {
        Iterator<List<String>> iterator = rows.iterator();
        String json = iterator.hasNext() ? cell(iterator.next(), 0) : null;
        table = parseDescribeTableJson(listedTable.schemaName, listedTable.tableName, json);
      }
      return table.isDelta() ? withDeltaPartitionColumns(handle, table) : table;
    }

    /**
     * Returns a Delta table with the partition columns that {@code DESCRIBE DETAIL} reports. If it
     * fails, the table is returned with unknown partitioning, so that its DDL is dumped instead.
     */
    @Nonnull
    private static ParsedTable withDeltaPartitionColumns(
        @Nonnull DatabricksHandle handle, @Nonnull ParsedTable table) {
      String query = "DESCRIBE DETAIL " + quoteTableName(table);
      try {
        ImmutableMap<String, String> detail =
            handle.sqlHelper().executeSqlInCatalogForFirstRow(HIVE_METASTORE_CATALOG, query);
        return table.withPartitionColumns(
            parseDeltaPartitionColumns(detail.get(PARTITION_COLUMNS_DETAIL)));
      } catch (IOException e) {
        return withUnknownPartitioning(table, e);
      } catch (UncheckedIOException e) {
        return withUnknownPartitioning(table, e.getCause());
      }
    }

    @Nonnull
    private static ParsedTable withUnknownPartitioning(
        @Nonnull ParsedTable table, @Nonnull IOException failure) {
      logger.warn(
          "Writing the columns of {} without partition indexes, and dumping its DDL instead,"
              + " because DESCRIBE DETAIL failed: {}",
          quoteTableName(table),
          failure.getMessage());
      return table;
    }
  }

  @Nonnull
  static String quoteTableName(@Nonnull ParsedTable table) {
    return quoteIdentifier(table.schemaName) + "." + quoteIdentifier(table.tableName);
  }

  private static boolean isView(@Nonnull ParsedTable table) {
    return "VIEW".equalsIgnoreCase(table.tableType);
  }

  /**
   * The {@code SHOW CREATE TABLE} output of a table or view, with the generation and identity
   * clauses of its columns, which only the DDL shows.
   */
  static final class TableDdl {

    final ParsedTable table;
    final String ddl;

    /** The generation and identity clauses of the columns, by lower-case column name. */
    final ImmutableMap<String, ParsedColumnClauses> columnClauses;

    TableDdl(@Nonnull ParsedTable table, @Nonnull String ddl) {
      this.table = table;
      this.ddl = ddl;
      this.columnClauses = parseColumnClauses(ddl);
    }
  }

  /**
   * Returns whether the DDL of a table may have clauses that the other statements don't report, so
   * that it needs {@code SHOW CREATE TABLE}. Those are the generated and identity columns of Delta
   * tables, which {@code DESCRIBE TABLE EXTENDED ... AS JSON} doesn't report. A Delta table may
   * have them if its protocol lists the {@code generatedColumns} or {@code identityColumns} table
   * feature, or if its writer version is from 4 to 6 or unknown. A Delta table whose partition
   * columns couldn't be read with {@code DESCRIBE DETAIL} also needs it, as does a table that
   * couldn't be described.
   *
   * <p>A table feature stays listed after the columns that needed it are dropped, so a table may
   * get {@code SHOW CREATE TABLE} without needing it.
   */
  static boolean needsShowCreateTable(@Nonnull ParsedTable table) {
    if (!table.described) {
      return true;
    }

    if (!table.isDelta()) {
      return false;
    }

    if (!table.partitioningKnown) {
      return true;
    }

    String minWriterVersion = null;
    for (Map.Entry<String, String> property : table.tableProperties.entrySet()) {
      String key = property.getKey();
      if (DDL_ONLY_FEATURE_PROPERTIES.contains(key.toLowerCase(Locale.ROOT))) {
        return true;
      }

      if (MIN_WRITER_VERSION_PROPERTY.equalsIgnoreCase(key)) {
        minWriterVersion = property.getValue();
      }
    }

    Integer writerVersion =
        minWriterVersion == null ? null : Ints.tryParse(minWriterVersion.trim());
    return writerVersion == null
        || LEGACY_GENERATED_COLUMNS_WRITER_VERSIONS.contains(writerVersion);
  }

  /** Returns the DDL of a table without the line break that Databricks appends to it. */
  @Nonnull
  private static String showCreateTable(
      @Nonnull DatabricksHandle handle, @Nonnull ParsedTable table) throws IOException {
    String query = "SHOW CREATE TABLE " + quoteTableName(table);
    StringBuilder ddl = new StringBuilder();
    try (CloseableIterable<List<String>> rows =
        handle.sqlHelper().executeSqlInCatalog(HIVE_METASTORE_CATALOG, query)) {
      for (List<String> row : rows) {
        String part = cell(row, 0);
        if (part == null) {
          continue;
        }

        if (ddl.length() > 0) {
          ddl.append('\n');
        }
        ddl.append(part);
      }
    } catch (IOException e) {
      warnIfUnsupportedSerde(table, e);
      throw e;
    } catch (UncheckedIOException e) {
      warnIfUnsupportedSerde(table, e.getCause());
      throw e;
    }
    return whitespace().trimTrailingFrom(ddl);
  }

  private static void warnIfUnsupportedSerde(
      @Nonnull ParsedTable table, @Nonnull IOException failure) {
    String message = failure.getMessage();
    if (message != null && message.contains(UNSUPPORTED_SERDE_ERROR_CLASS)) {
      logger.warn(
          "Databricks doesn't support SHOW CREATE TABLE for {} because of its Hive SerDe"
              + " configuration; its storage is described in {}.",
          quoteTableName(table),
          DatabricksMetadataDumpFormat.HIVE_METASTORE_TABLES_ENTRY_NAME);
    }
  }

  /**
   * Parses the {@code Properties} that {@code DESCRIBE SCHEMA EXTENDED} reports, such as {@code
   * ((owner_team,data), (purpose,legacy))}, in their printed order.
   *
   * <p>Spark prints the keys and values without escaping, so the entries are split on a best-effort
   * basis: an entry ends before {@code ), (}, and its key ends before its first comma.
   */
  @Nonnull
  static ImmutableMap<String, String> parseSchemaProperties(@Nonnull String text) {
    String trimmed = text.trim();
    if (!trimmed.startsWith(SCHEMA_PROPERTIES_PREFIX)
        || !trimmed.endsWith(SCHEMA_PROPERTIES_SUFFIX)
        || trimmed.length()
            < SCHEMA_PROPERTIES_PREFIX.length() + SCHEMA_PROPERTIES_SUFFIX.length()) {
      return ImmutableMap.of();
    }

    String body =
        trimmed.substring(
            SCHEMA_PROPERTIES_PREFIX.length(),
            trimmed.length() - SCHEMA_PROPERTIES_SUFFIX.length());
    Map<String, String> properties = new LinkedHashMap<>();
    int entryStart = 0;
    while (entryStart <= body.length()) {
      int entryEnd = body.indexOf(SCHEMA_PROPERTIES_SEPARATOR, entryStart);
      if (entryEnd < 0) {
        entryEnd = body.length();
      }
      String entry = body.substring(entryStart, entryEnd);
      int separator = entry.indexOf(',');
      if (separator < 0) {
        properties.put(entry, "");
      } else {
        properties.put(entry.substring(0, separator), entry.substring(separator + 1));
      }
      entryStart = entryEnd + SCHEMA_PROPERTIES_SEPARATOR.length();
    }
    return ImmutableMap.copyOf(properties);
  }

  abstract static class AbstractDatabricksHiveMetastoreTask extends AbstractTask<Void> {

    protected final HiveMetastoreSessionCache sessionCache;

    AbstractDatabricksHiveMetastoreTask(
        @Nonnull String targetPath, @Nonnull HiveMetastoreSessionCache sessionCache) {
      super(targetPath);
      this.sessionCache = sessionCache;
    }

    @Override
    @Nonnull
    public TaskCategory getCategory() {
      return TaskCategory.OPTIONAL;
    }
  }

  static final class DatabricksHiveMetastoreCatalogsTask
      extends AbstractDatabricksHiveMetastoreTask {

    DatabricksHiveMetastoreCatalogsTask(@Nonnull HiveMetastoreSessionCache sessionCache) {
      super(DatabricksMetadataDumpFormat.HIVE_METASTORE_CATALOGS_ENTRY_NAME, sessionCache);
    }

    @Override
    @Nonnull
    public String getName() {
      return "databricks-hms-catalogs";
    }

    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(CatalogsHeader.class).print(writer)) {
        Object[] record = new Object[CatalogsHeader.values().length];
        record[CatalogsHeader.CatalogName.ordinal()] = HIVE_METASTORE_CATALOG;
        csv.printRecord(record);
      }
      return null;
    }
  }

  static final class DatabricksHiveMetastoreSchemataTask
      extends AbstractDatabricksHiveMetastoreTask {

    DatabricksHiveMetastoreSchemataTask(@Nonnull HiveMetastoreSessionCache sessionCache) {
      super(DatabricksMetadataDumpFormat.HIVE_METASTORE_SCHEMATA_ENTRY_NAME, sessionCache);
    }

    @Override
    @Nonnull
    public String getName() {
      return "databricks-hms-schemata";
    }

    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      DatabricksHandle dbxHandle = (DatabricksHandle) handle;
      ImmutableList<String> schemas = sessionCache.listSchemas(dbxHandle);
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(SchemataHeader.class).print(writer)) {
        dbxHandle
            .statementRunner()
            .runInOrder(
                "DESCRIBE SCHEMA EXTENDED",
                schemas,
                DatabricksSqlHelper::quoteIdentifier,
                schema -> describeSchemaOrEmpty(dbxHandle, schema),
                (schema, description) ->
                    csv.printRecord(
                        HIVE_METASTORE_CATALOG,
                        schema,
                        description.comment,
                        description.owner,
                        "",
                        "",
                        "",
                        description.location,
                        toJsonOrEmpty(description.properties)));
      }
      return null;
    }

    /**
     * Describes a schema. The schema name is already known from {@code SHOW SCHEMAS}, so if {@code
     * DESCRIBE SCHEMA EXTENDED} fails, the schema is still written, without comment, owner,
     * location and properties.
     */
    @Nonnull
    private static SchemaDescription describeSchemaOrEmpty(
        @Nonnull DatabricksHandle handle, @Nonnull String schema) {
      String query = "DESCRIBE SCHEMA EXTENDED " + quoteIdentifier(schema);
      String comment = "";
      String owner = "";
      String location = "";
      ImmutableMap<String, String> properties = ImmutableMap.of();
      try (CloseableIterable<List<String>> rows =
          handle.sqlHelper().executeSqlInCatalog(HIVE_METASTORE_CATALOG, query)) {
        for (List<String> row : rows) {
          String key = cellOrEmpty(row, 0);
          String value = cellOrEmpty(row, 1);
          if ("Comment".equalsIgnoreCase(key)) {
            comment = value;
          } else if ("Owner".equalsIgnoreCase(key)) {
            owner = value;
          } else if ("Location".equalsIgnoreCase(key)) {
            location = value;
          } else if ("Properties".equalsIgnoreCase(key)) {
            properties = parseSchemaProperties(value);
          }
        }
      } catch (IOException e) {
        return emptySchemaDescription(schema, e);
      } catch (UncheckedIOException e) {
        return emptySchemaDescription(schema, e.getCause());
      }
      return new SchemaDescription(comment, owner, location, properties);
    }

    @Nonnull
    private static SchemaDescription emptySchemaDescription(
        @Nonnull String schema, @Nonnull IOException failure) {
      logger.warn(
          "Writing schema {} without comment, owner, location and properties because DESCRIBE"
              + " SCHEMA EXTENDED failed: {}",
          quoteIdentifier(schema),
          failure.getMessage());
      return new SchemaDescription("", "", "", ImmutableMap.of());
    }
  }

  private static final class SchemaDescription {

    final String comment;
    final String owner;
    final String location;
    final ImmutableMap<String, String> properties;

    SchemaDescription(
        @Nonnull String comment,
        @Nonnull String owner,
        @Nonnull String location,
        @Nonnull ImmutableMap<String, String> properties) {
      this.comment = comment;
      this.owner = owner;
      this.location = location;
      this.properties = properties;
    }
  }

  static final class DatabricksHiveMetastoreTablesTask extends AbstractDatabricksHiveMetastoreTask {

    DatabricksHiveMetastoreTablesTask(@Nonnull HiveMetastoreSessionCache sessionCache) {
      super(DatabricksMetadataDumpFormat.HIVE_METASTORE_TABLES_ENTRY_NAME, sessionCache);
    }

    @Override
    @Nonnull
    public String getName() {
      return "databricks-hms-tables";
    }

    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      DatabricksHandle dbxHandle = (DatabricksHandle) handle;
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(TablesHeader.class).print(writer)) {
        for (ParsedTable tbl : sessionCache.listParsedTables(dbxHandle)) {
          csv.printRecord(
              HIVE_METASTORE_CATALOG,
              tbl.schemaName,
              tbl.tableName,
              tbl.tableType,
              "",
              "",
              tbl.dataSourceFormat,
              tbl.storageLocation,
              tbl.comment,
              tbl.owner,
              tbl.createdAt,
              "",
              tbl.serdeLibrary,
              tbl.inputFormat,
              tbl.outputFormat,
              toJsonOrEmpty(tbl.storageProperties),
              toJsonOrEmpty(tbl.tableProperties),
              tbl.numBuckets,
              toJsonOrEmpty(tbl.bucketColumns),
              toJsonOrEmpty(tbl.sortColumns),
              "");
        }
      }
      return null;
    }
  }

  /**
   * Writes the columns of each table and view in {@code hive_metastore}, as {@code DESCRIBE TABLE
   * EXTENDED ... AS JSON} reports them, with the generation and identity clauses that only the
   * {@code SHOW CREATE TABLE} output of their tables shows.
   */
  static final class DatabricksHiveMetastoreColumnsTask
      extends AbstractDatabricksHiveMetastoreTask {

    DatabricksHiveMetastoreColumnsTask(@Nonnull HiveMetastoreSessionCache sessionCache) {
      super(DatabricksMetadataDumpFormat.HIVE_METASTORE_COLUMNS_ENTRY_NAME, sessionCache);
    }

    @Override
    @Nonnull
    public String getName() {
      return "databricks-hms-columns";
    }

    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      DatabricksHandle dbxHandle = (DatabricksHandle) handle;
      ImmutableList<ParsedTable> tables = sessionCache.listParsedTables(dbxHandle);
      Map<ParsedTable, TableDdl> ddlsByTable = findDdlsByTable(dbxHandle);
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(ColumnsHeader.class).print(writer)) {
        for (ParsedTable table : tables) {
          TableDdl tableDdl = ddlsByTable.get(table);
          for (ParsedColumn column : table.columns) {
            ParsedColumnClauses clauses =
                tableDdl == null
                    ? null
                    : tableDdl.columnClauses.get(column.columnName.toLowerCase(Locale.ROOT));
            csv.printRecord(
                HIVE_METASTORE_CATALOG,
                table.schemaName,
                table.tableName,
                String.valueOf(column.ordinalPosition),
                column.columnName,
                column.dataType,
                column.isNullable,
                column.comment,
                column.partitionIndex,
                column.defaultExpression,
                clauses == null ? "" : clauses.generationExpression,
                clauses == null ? "" : clauses.identityGeneration,
                clauses == null ? "" : clauses.identityStart,
                clauses == null ? "" : clauses.identityIncrement,
                "");
          }
        }
      }
      return null;
    }

    /**
     * Returns the DDLs of the tables by table. If {@code SHOW CREATE TABLE} fails for every table
     * that needs it, the columns are written without generation and identity clauses, and the DDLs
     * task fails.
     */
    @Nonnull
    private Map<ParsedTable, TableDdl> findDdlsByTable(@Nonnull DatabricksHandle handle)
        throws InterruptedIOException {
      // Keyed by the listed table, which doesn't override equals.
      Map<ParsedTable, TableDdl> ddlsByTable = new IdentityHashMap<>();
      try {
        for (TableDdl tableDdl : sessionCache.listDdls(handle)) {
          ddlsByTable.put(tableDdl.table, tableDdl);
        }
      } catch (InterruptedIOException e) {
        throw e;
      } catch (IOException e) {
        logger.warn(
            "Writing the columns without generation and identity clauses because SHOW CREATE TABLE"
                + " failed: {}",
            e.getMessage());
      }
      return ddlsByTable;
    }
  }

  static final class DatabricksHiveMetastoreViewsTask extends AbstractDatabricksHiveMetastoreTask {

    DatabricksHiveMetastoreViewsTask(@Nonnull HiveMetastoreSessionCache sessionCache) {
      super(DatabricksMetadataDumpFormat.HIVE_METASTORE_VIEWS_ENTRY_NAME, sessionCache);
    }

    @Override
    @Nonnull
    public String getName() {
      return "databricks-hms-views";
    }

    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      DatabricksHandle dbxHandle = (DatabricksHandle) handle;
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(ViewsHeader.class).print(writer)) {
        for (ParsedTable tbl : sessionCache.listParsedTables(dbxHandle)) {
          if (tbl.viewDefinition != null || isView(tbl)) {
            csv.printRecord(
                HIVE_METASTORE_CATALOG,
                tbl.schemaName,
                tbl.tableName,
                tbl.viewDefinition != null ? tbl.viewDefinition : "",
                tbl.viewCatalogAndNamespace,
                toJsonOrEmpty(tbl.viewQueryOutputColumns),
                tbl.viewSchemaMode,
                tbl.sqlPath);
          }
        }
      }
      return null;
    }
  }

  static final class DatabricksHiveMetastoreTableConstraintsTask
      extends AbstractDatabricksHiveMetastoreTask {

    DatabricksHiveMetastoreTableConstraintsTask(@Nonnull HiveMetastoreSessionCache sessionCache) {
      super(DatabricksMetadataDumpFormat.HIVE_METASTORE_TABLE_CONSTRAINTS_ENTRY_NAME, sessionCache);
    }

    @Override
    @Nonnull
    public String getName() {
      return "databricks-hms-table-constraints";
    }

    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      DatabricksHandle dbxHandle = (DatabricksHandle) handle;
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(TableConstraintsHeader.class).print(writer)) {
        for (ParsedTable tbl : sessionCache.listParsedTables(dbxHandle)) {
          for (ParsedTableConstraint tc : tbl.constraints) {
            csv.printRecord(
                HIVE_METASTORE_CATALOG,
                tbl.schemaName,
                tbl.tableName,
                tc.constraintName,
                tc.constraintType,
                toJsonOrEmpty(ImmutableMap.of("expression", tc.expression)));
          }
        }
      }
      return null;
    }
  }
}
