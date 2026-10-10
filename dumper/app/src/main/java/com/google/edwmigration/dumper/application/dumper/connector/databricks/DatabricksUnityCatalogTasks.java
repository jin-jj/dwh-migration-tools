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
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.parseDeltaCheckConstraints;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksJson.sortJsonObjectKeys;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksJson.toJsonOrEmpty;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.cell;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.quoteIdentifier;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.quoteStringLiteral;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.copyWithoutNulls;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.formatBoolean;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.formatDataType;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.formatEnum;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.formatForeignKeyDetails;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.formatFunctionDataType;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.formatMask;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.formatNullable;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.formatNumber;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.formatPrimaryKeyDetails;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.formatRoutineLanguage;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.formatRowFilter;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.formatSqlDataAccess;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.formatTime;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.formatViewCatalogAndNamespace;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.formatViewSchemaMode;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.parseColumnClauses;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.parseViewQueryOutputColumns;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.sortColumns;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.toFunctionParameters;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.databricks.sdk.core.utils.SerDeUtils;
import com.databricks.sdk.service.catalog.CatalogInfo;
import com.databricks.sdk.service.catalog.ColumnInfo;
import com.databricks.sdk.service.catalog.ForeignKeyConstraint;
import com.databricks.sdk.service.catalog.FunctionInfo;
import com.databricks.sdk.service.catalog.PrimaryKeyConstraint;
import com.databricks.sdk.service.catalog.SchemaInfo;
import com.databricks.sdk.service.catalog.SecurableKindManifest;
import com.databricks.sdk.service.catalog.TableConstraint;
import com.databricks.sdk.service.catalog.TableInfo;
import com.databricks.sdk.service.catalog.TableType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteSink;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreTableParser.ParsedTableConstraint;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.CatalogsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.ColumnsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.FunctionsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.SchemataHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.TableConstraintsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.TablesHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksMetadataDumpFormat.ViewsHeader;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.ColumnClauses;
import com.google.edwmigration.dumper.application.dumper.handle.Handle;
import com.google.edwmigration.dumper.application.dumper.task.AbstractTask;
import com.google.edwmigration.dumper.application.dumper.task.TaskCategory;
import com.google.edwmigration.dumper.application.dumper.task.TaskRunContext;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extraction tasks for the Unity Catalog catalogs using the Unity Catalog REST API. The table
 * constraints task also queries the information_schema of the catalogs on the SQL warehouse.
 *
 * <p>The tables and views of all schemas are listed once for the tables, columns, views and table
 * constraints tasks. The listing of a large metastore may not fit in memory, so it is written to a
 * temporary file, one table per line.
 */
final class DatabricksUnityCatalogTasks {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksUnityCatalogTasks.class);

  private static final String PRIMARY_KEY_CONSTRAINT_TYPE = "PRIMARY KEY";
  private static final String FOREIGN_KEY_CONSTRAINT_TYPE = "FOREIGN KEY";

  /** The table types of views, which have no data and no primary or foreign keys. */
  private static final ImmutableSet<TableType> VIEW_TABLE_TYPES =
      ImmutableSet.of(TableType.VIEW, TableType.METRIC_VIEW);

  /** The table types that the views file has a row for, even without a defining query. */
  private static final ImmutableSet<TableType> VIEWS_FILE_TABLE_TYPES =
      ImmutableSet.of(TableType.VIEW, TableType.MATERIALIZED_VIEW, TableType.METRIC_VIEW);

  /**
   * Writes and reads the table listing with the JSON mapper of the SDK, so that each table is read
   * back as the SDK parsed it from the response.
   */
  private static final ObjectMapper LISTING_MAPPER = SerDeUtils.createMapper();

  private static final ObjectWriter LISTING_WRITER = LISTING_MAPPER.writer();
  private static final ObjectReader LISTING_READER = LISTING_MAPPER.readerFor(TableInfo.class);

  private DatabricksUnityCatalogTasks() {}

  /**
   * Shared per-connector cache of the Unity Catalog catalogs, schemas and table listing.
   *
   * <p>Each one is loaded once. A failed load is remembered, so the other tasks that need it fail
   * fast instead of repeating requests that already failed.
   */
  static final class UnityCatalogSessionCache {

    private final Memoized<ImmutableList<CatalogInfo>> catalogs =
        new Memoized<>("Listing Unity Catalog catalogs");
    private final Memoized<ImmutableList<SchemaInfo>> schemas =
        new Memoized<>("Listing Unity Catalog schemas");
    private final Memoized<Path> tableListing = new Memoized<>("Listing Unity Catalog tables");

    /** Returns the catalogs that the filter accepts, without {@code hive_metastore}. */
    @Nonnull
    ImmutableList<CatalogInfo> listCatalogs(@Nonnull DatabricksHandle handle) throws IOException {
      return catalogs.load(() -> loadCatalogs(handle));
    }

    /**
     * Returns the schemas of the catalogs that the filter accepts. A catalog whose schemas can't be
     * listed is skipped.
     */
    @Nonnull
    ImmutableList<SchemaInfo> listSchemas(@Nonnull DatabricksHandle handle) throws IOException {
      return schemas.load(() -> loadSchemas(handle));
    }

    /**
     * Returns the tables and views of the schemas, which are listed once for all tasks. A schema
     * whose tables can't be listed is skipped. The returned iterable reads a file and must be
     * closed.
     */
    @Nonnull
    CloseableIterable<TableInfo> openTables(@Nonnull DatabricksHandle handle) throws IOException {
      return new TableListingReader(tableListing.load(() -> writeTableListing(handle)));
    }

    @Nonnull
    private static ImmutableList<CatalogInfo> loadCatalogs(@Nonnull DatabricksHandle handle)
        throws IOException {
      ImmutableList.Builder<CatalogInfo> builder = ImmutableList.builder();
      for (CatalogInfo catalog : handle.restHelper().listCatalogs()) {
        String catalogName = catalog.getName();
        if (catalogName != null
            && !HIVE_METASTORE_CATALOG.equalsIgnoreCase(catalogName)
            && handle.filter().acceptsCatalog(catalogName)) {
          builder.add(catalog);
        }
      }
      return builder.build();
    }

    @Nonnull
    private ImmutableList<SchemaInfo> loadSchemas(@Nonnull DatabricksHandle handle)
        throws IOException {
      ImmutableList.Builder<SchemaInfo> builder = ImmutableList.builder();
      handle
          .statementRunner()
          .runInOrder(
              "Unity Catalog schemas.list",
              listCatalogs(handle),
              catalog -> quoteIdentifier(catalog.getName()),
              catalog -> handle.restHelper().listSchemas(catalog.getName()),
              (catalog, catalogSchemas) -> {
                for (SchemaInfo schema : catalogSchemas) {
                  if (schema.getName() != null && handle.filter().acceptsSchema(schema.getName())) {
                    builder.add(schema.setCatalogName(catalog.getName()));
                  }
                }
              });
      return builder.build();
    }

    @Nonnull
    private Path writeTableListing(@Nonnull DatabricksHandle handle) throws IOException {
      ImmutableList<SchemaInfo> schemaInfos = listSchemas(handle);
      Path listing = handle.createTemporaryFile("databricks-unity-catalog-tables", ".jsonl");
      try (Writer writer = Files.newBufferedWriter(listing, UTF_8)) {
        handle
            .statementRunner()
            .runInOrder(
                "Unity Catalog tables.list",
                schemaInfos,
                DatabricksUnityCatalogTasks::quoteSchemaName,
                schema -> handle.restHelper().listTables(schema.getCatalogName(), schema.getName()),
                (schema, tables) -> {
                  for (TableInfo table : tables) {
                    table.setCatalogName(schema.getCatalogName()).setSchemaName(schema.getName());
                    writer.write(LISTING_WRITER.writeValueAsString(table));
                    writer.write('\n');
                  }
                });
      }
      return listing;
    }
  }

  /** Reads the tables of a listing file, which has the JSON object of one table per line. */
  private static final class TableListingReader implements CloseableIterable<TableInfo> {

    private final BufferedReader reader;
    private boolean iterated;

    TableListingReader(@Nonnull Path listing) throws IOException {
      this.reader = Files.newBufferedReader(listing, UTF_8);
    }

    @Override
    @Nonnull
    public Iterator<TableInfo> iterator() {
      Preconditions.checkState(!iterated, "The table listing can be read only once.");
      iterated = true;
      return new TableListingIterator(reader);
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }

  private static final class TableListingIterator extends AbstractIterator<TableInfo> {

    private final BufferedReader reader;

    TableListingIterator(@Nonnull BufferedReader reader) {
      this.reader = reader;
    }

    @Override
    @CheckForNull
    protected TableInfo computeNext() {
      try {
        String line = reader.readLine();
        if (line == null) {
          return endOfData();
        }
        return LISTING_READER.readValue(line);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to read the Unity Catalog table listing.", e);
      }
    }
  }

  @Nonnull
  private static String quoteSchemaName(@Nonnull SchemaInfo schema) {
    return quoteIdentifier(schema.getCatalogName()) + "." + quoteIdentifier(schema.getName());
  }

  @Nonnull
  private static String quoteTableName(@Nonnull TableInfo table) {
    return quoteIdentifier(table.getCatalogName())
        + "."
        + quoteIdentifier(table.getSchemaName())
        + "."
        + quoteIdentifier(table.getName());
  }

  /** Returns whether the table is a view, which has no primary or foreign keys. */
  private static boolean isView(@Nonnull TableInfo table) {
    return VIEW_TABLE_TYPES.contains(table.getTableType());
  }

  /** Returns the properties in key order without null values, or an empty map if null. */
  @Nonnull
  private static Map<String, String> sortedProperties(
      @CheckForNull Map<String, String> properties) {
    Map<String, String> sorted = new TreeMap<>();
    if (properties != null) {
      for (Map.Entry<String, String> property : properties.entrySet()) {
        if (property.getKey() != null && property.getValue() != null) {
          sorted.put(property.getKey(), property.getValue());
        }
      }
    }
    return sorted;
  }

  abstract static class AbstractDatabricksUnityCatalogTask extends AbstractTask<Void> {

    protected final UnityCatalogSessionCache sessionCache;

    AbstractDatabricksUnityCatalogTask(
        @Nonnull String targetPath, @Nonnull UnityCatalogSessionCache sessionCache) {
      super(targetPath);
      this.sessionCache = Preconditions.checkNotNull(sessionCache, "Session cache was null.");
    }

    @Override
    @Nonnull
    public TaskCategory getCategory() {
      return TaskCategory.OPTIONAL;
    }
  }

  static final class DatabricksUnityCatalogCatalogsTask extends AbstractDatabricksUnityCatalogTask {

    DatabricksUnityCatalogCatalogsTask(@Nonnull UnityCatalogSessionCache sessionCache) {
      super(DatabricksMetadataDumpFormat.UNITY_CATALOG_CATALOGS_ENTRY_NAME, sessionCache);
    }

    @Override
    @Nonnull
    public String getName() {
      return "databricks-uc-catalogs";
    }

    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      ImmutableList<CatalogInfo> catalogs = sessionCache.listCatalogs((DatabricksHandle) handle);
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(CatalogsHeader.class).print(writer)) {
        for (CatalogInfo catalog : catalogs) {
          csv.printRecord(
              catalog.getName(),
              formatEnum(catalog.getSecurableType()),
              formatEnum(catalog.getCatalogType()),
              catalog.getComment(),
              catalog.getOwner(),
              formatTime(catalog.getCreatedAt()),
              formatTime(catalog.getUpdatedAt()),
              catalog.getStorageRoot(),
              catalog.getStorageLocation(),
              catalog.getConnectionName(),
              catalog.getProviderName(),
              catalog.getShareName(),
              toJsonOrEmpty(catalog.getOptions()),
              toJsonOrEmpty(catalog.getProperties()));
        }
      }
      return null;
    }
  }

  static final class DatabricksUnityCatalogSchemataTask extends AbstractDatabricksUnityCatalogTask {

    DatabricksUnityCatalogSchemataTask(@Nonnull UnityCatalogSessionCache sessionCache) {
      super(DatabricksMetadataDumpFormat.UNITY_CATALOG_SCHEMATA_ENTRY_NAME, sessionCache);
    }

    @Override
    @Nonnull
    public String getName() {
      return "databricks-uc-schemata";
    }

    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      ImmutableList<SchemaInfo> schemas = sessionCache.listSchemas((DatabricksHandle) handle);
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(SchemataHeader.class).print(writer)) {
        for (SchemaInfo schema : schemas) {
          csv.printRecord(
              schema.getCatalogName(),
              schema.getName(),
              schema.getComment(),
              schema.getOwner(),
              formatTime(schema.getCreatedAt()),
              formatTime(schema.getUpdatedAt()),
              schema.getStorageRoot(),
              schema.getStorageLocation(),
              toJsonOrEmpty(schema.getProperties()));
        }
      }
      return null;
    }
  }

  static final class DatabricksUnityCatalogTablesTask extends AbstractDatabricksUnityCatalogTask {

    DatabricksUnityCatalogTablesTask(@Nonnull UnityCatalogSessionCache sessionCache) {
      super(DatabricksMetadataDumpFormat.UNITY_CATALOG_TABLES_ENTRY_NAME, sessionCache);
    }

    @Override
    @Nonnull
    public String getName() {
      return "databricks-uc-tables";
    }

    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      try (CloseableIterable<TableInfo> tables =
              sessionCache.openTables((DatabricksHandle) handle);
          Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(TablesHeader.class).print(writer)) {
        for (TableInfo table : tables) {
          SecurableKindManifest manifest = table.getSecurableKindManifest();
          csv.printRecord(
              table.getCatalogName(),
              table.getSchemaName(),
              table.getName(),
              formatEnum(table.getTableType()),
              manifest == null ? "" : formatEnum(manifest.getSecurableType()),
              manifest == null ? "" : formatEnum(manifest.getSecurableKind()),
              formatEnum(table.getDataSourceFormat()),
              table.getStorageLocation(),
              table.getComment(),
              table.getOwner(),
              formatTime(table.getCreatedAt()),
              formatTime(table.getUpdatedAt()),
              "",
              "",
              "",
              "",
              toJsonOrEmpty(table.getProperties()),
              "",
              "",
              "",
              formatRowFilter(table.getRowFilter()));
        }
      }
      return null;
    }
  }

  static final class DatabricksUnityCatalogColumnsTask extends AbstractDatabricksUnityCatalogTask {

    DatabricksUnityCatalogColumnsTask(@Nonnull UnityCatalogSessionCache sessionCache) {
      super(DatabricksMetadataDumpFormat.UNITY_CATALOG_COLUMNS_ENTRY_NAME, sessionCache);
    }

    @Override
    @Nonnull
    public String getName() {
      return "databricks-uc-columns";
    }

    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      try (CloseableIterable<TableInfo> tables =
              sessionCache.openTables((DatabricksHandle) handle);
          Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(ColumnsHeader.class).print(writer)) {
        for (TableInfo table : tables) {
          List<ColumnInfo> columns = sortColumns(table.getColumns());
          for (int index = 0; index < columns.size(); index++) {
            ColumnInfo column = columns.get(index);
            ColumnClauses clauses = parseColumnClauses(column.getTypeJson());
            csv.printRecord(
                table.getCatalogName(),
                table.getSchemaName(),
                table.getName(),
                column.getPosition() == null
                    ? String.valueOf(index)
                    : String.valueOf(column.getPosition()),
                column.getName(),
                formatDataType(column.getTypeText(), column.getTypeJson()),
                formatNullable(column.getNullable()),
                column.getComment(),
                formatNumber(column.getPartitionIndex()),
                clauses.defaultExpression,
                clauses.generationExpression,
                clauses.identityGeneration,
                clauses.identityStart,
                clauses.identityIncrement,
                formatMask(column.getMask()));
          }
        }
      }
      return null;
    }
  }

  static final class DatabricksUnityCatalogViewsTask extends AbstractDatabricksUnityCatalogTask {

    DatabricksUnityCatalogViewsTask(@Nonnull UnityCatalogSessionCache sessionCache) {
      super(DatabricksMetadataDumpFormat.UNITY_CATALOG_VIEWS_ENTRY_NAME, sessionCache);
    }

    @Override
    @Nonnull
    public String getName() {
      return "databricks-uc-views";
    }

    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      try (CloseableIterable<TableInfo> tables =
              sessionCache.openTables((DatabricksHandle) handle);
          Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(ViewsHeader.class).print(writer)) {
        for (TableInfo table : tables) {
          if (table.getViewDefinition() == null
              && !VIEWS_FILE_TABLE_TYPES.contains(table.getTableType())) {
            continue;
          }

          Map<String, String> properties = table.getProperties();
          csv.printRecord(
              table.getCatalogName(),
              table.getSchemaName(),
              table.getName(),
              table.getViewDefinition(),
              formatViewCatalogAndNamespace(properties),
              toJsonOrEmpty(parseViewQueryOutputColumns(properties)),
              formatViewSchemaMode(properties),
              table.getSqlPath());
        }
      }
      return null;
    }
  }

  /**
   * Writes the check constraints of the tables, and then their primary and foreign keys.
   *
   * <p>The listing of the tables has their {@code delta.constraints.*} properties but not their
   * primary and foreign keys. The information_schema of each catalog, queried on the SQL warehouse,
   * lists the tables that have keys, so a table request is sent only for those tables. The keys are
   * read from the table requests, because information_schema doesn't have their {@code RELY} and
   * {@code TIMESERIES} options. information_schema shows only the tables that the user has
   * privileges on, so the keys of a table that it doesn't show are missing. If the
   * information_schema of a catalog can't be read, a request is sent for each table of the catalog
   * other than a view.
   *
   * <p>The check constraints are written first, so that a table whose request fails keeps them.
   */
  static final class DatabricksUnityCatalogTableConstraintsTask
      extends AbstractDatabricksUnityCatalogTask {

    /**
     * Selects the schema and table names, which are lowercase, of the tables of a catalog that have
     * a primary or foreign key.
     */
    private static final String TABLES_WITH_KEYS_SQL_FORMAT =
        "SELECT DISTINCT table_schema, table_name FROM %s.information_schema.table_constraints"
            + " WHERE constraint_type IN ("
            + quoteStringLiteral(PRIMARY_KEY_CONSTRAINT_TYPE)
            + ", "
            + quoteStringLiteral(FOREIGN_KEY_CONSTRAINT_TYPE)
            + ")";

    DatabricksUnityCatalogTableConstraintsTask(@Nonnull UnityCatalogSessionCache sessionCache) {
      super(DatabricksMetadataDumpFormat.UNITY_CATALOG_TABLE_CONSTRAINTS_ENTRY_NAME, sessionCache);
    }

    @Override
    @Nonnull
    public String getName() {
      return "databricks-uc-table-constraints";
    }

    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      DatabricksHandle dbxHandle = (DatabricksHandle) handle;
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(TableConstraintsHeader.class).print(writer)) {
        ImmutableSet.Builder<String> catalogsWithTables = ImmutableSet.builder();
        try (CloseableIterable<TableInfo> tables = sessionCache.openTables(dbxHandle)) {
          for (TableInfo table : tables) {
            printCheckConstraints(csv, table);
            if (!isView(table)) {
              catalogsWithTables.add(table.getCatalogName());
            }
          }
        }

        ImmutableMap<String, ImmutableSet<String>> tablesWithKeys =
            readTablesWithKeys(dbxHandle, catalogsWithTables.build());
        try (CloseableIterable<TableInfo> tables = sessionCache.openTables(dbxHandle)) {
          dbxHandle
              .statementRunner()
              .runInOrder(
                  "Unity Catalog tables.get",
                  Iterables.filter(tables, table -> mayHaveKeys(table, tablesWithKeys)),
                  DatabricksUnityCatalogTasks::quoteTableName,
                  table -> fetchTableConstraints(dbxHandle, table),
                  (table, constraints) -> printKeys(csv, table, constraints));
        }
      }
      return null;
    }

    private static void printCheckConstraints(@Nonnull CSVPrinter csv, @Nonnull TableInfo table)
        throws IOException {
      for (ParsedTableConstraint constraint :
          parseDeltaCheckConstraints(sortedProperties(table.getProperties()))) {
        csv.printRecord(
            table.getCatalogName(),
            table.getSchemaName(),
            table.getName(),
            constraint.constraintName,
            constraint.constraintType,
            toJsonOrEmpty(ImmutableMap.of("expression", constraint.expression)));
      }
    }

    /**
     * Returns the {@link #normalizedTableName names} of the tables that have primary or foreign
     * keys, by catalog name. A catalog whose information_schema can't be read has no entry.
     */
    @Nonnull
    private static ImmutableMap<String, ImmutableSet<String>> readTablesWithKeys(
        @Nonnull DatabricksHandle handle, @Nonnull ImmutableSet<String> catalogNames)
        throws IOException {
      ImmutableMap.Builder<String, ImmutableSet<String>> builder = ImmutableMap.builder();
      handle
          .statementRunner()
          .runInOrder(
              "Unity Catalog information_schema query",
              catalogNames,
              DatabricksSqlHelper::quoteIdentifier,
              catalogName -> queryTablesWithKeys(handle, catalogName),
              (catalogName, catalogTablesWithKeys) -> builder.putAll(catalogTablesWithKeys));
      ImmutableMap<String, ImmutableSet<String>> tablesWithKeys = builder.build();
      int tableCount = 0;
      for (ImmutableSet<String> catalogTablesWithKeys : tablesWithKeys.values()) {
        tableCount += catalogTablesWithKeys.size();
      }

      logger.info(
          "The information_schema of {} of {} catalogs lists {} tables with primary or foreign"
              + " keys.",
          tablesWithKeys.size(),
          catalogNames.size(),
          tableCount);
      return tablesWithKeys;
    }

    /**
     * Returns the {@link #normalizedTableName names} of the tables of a catalog that have primary
     * or foreign keys, by catalog name, or an empty map if the information_schema of the catalog
     * can't be read.
     */
    @Nonnull
    private static ImmutableMap<String, ImmutableSet<String>> queryTablesWithKeys(
        @Nonnull DatabricksHandle handle, @Nonnull String catalogName) throws IOException {
      String sql = String.format(TABLES_WITH_KEYS_SQL_FORMAT, quoteIdentifier(catalogName));
      ImmutableSet.Builder<String> tablesWithKeys = ImmutableSet.builder();
      try (CloseableIterable<List<String>> rows =
          handle.sqlHelper().executeSqlInCatalog(catalogName, sql)) {
        for (List<String> row : rows) {
          String schemaName = cell(row, 0);
          String tableName = cell(row, 1);
          if (schemaName != null && tableName != null) {
            tablesWithKeys.add(normalizedTableName(schemaName, tableName));
          }
        }
      } catch (IOException | UncheckedIOException e) {
        if (Thread.currentThread().isInterrupted()) {
          throw e;
        }

        logger.warn(
            "Requesting each table of catalog {} for its primary and foreign keys, because its"
                + " information_schema can't be read: {}",
            quoteIdentifier(catalogName),
            e.getMessage());
        logger.debug(
            "The information_schema query of catalog {} failed.", quoteIdentifier(catalogName), e);
        return ImmutableMap.of();
      }

      return ImmutableMap.of(catalogName, tablesWithKeys.build());
    }

    /**
     * Returns whether a table request may find primary or foreign keys: the table is not a view,
     * and information_schema lists it or the information_schema of its catalog couldn't be read.
     */
    private static boolean mayHaveKeys(
        @Nonnull TableInfo table,
        @Nonnull ImmutableMap<String, ImmutableSet<String>> tablesWithKeys) {
      if (isView(table)) {
        return false;
      }

      ImmutableSet<String> catalogTablesWithKeys = tablesWithKeys.get(table.getCatalogName());
      return catalogTablesWithKeys == null
          || catalogTablesWithKeys.contains(
              normalizedTableName(table.getSchemaName(), table.getName()));
    }

    /** Returns the quoted schema and table name in lowercase, as information_schema has them. */
    @Nonnull
    private static String normalizedTableName(
        @Nonnull String schemaName, @Nonnull String tableName) {
      return quoteIdentifier(schemaName.toLowerCase(Locale.ROOT))
          + "."
          + quoteIdentifier(tableName.toLowerCase(Locale.ROOT));
    }

    @Nonnull
    private static ImmutableList<TableConstraint> fetchTableConstraints(
        @Nonnull DatabricksHandle handle, @Nonnull TableInfo table) throws IOException {
      TableInfo described =
          handle
              .restHelper()
              .getTable(table.getCatalogName(), table.getSchemaName(), table.getName());
      return copyWithoutNulls(described.getTableConstraints());
    }

    /** Writes the primary and foreign keys. Check constraints are already written. */
    private static void printKeys(
        @Nonnull CSVPrinter csv,
        @Nonnull TableInfo table,
        @Nonnull ImmutableList<TableConstraint> constraints)
        throws IOException {
      for (TableConstraint constraint : constraints) {
        PrimaryKeyConstraint primaryKey = constraint.getPrimaryKeyConstraint();
        if (primaryKey != null) {
          csv.printRecord(
              table.getCatalogName(),
              table.getSchemaName(),
              table.getName(),
              primaryKey.getName(),
              PRIMARY_KEY_CONSTRAINT_TYPE,
              formatPrimaryKeyDetails(primaryKey));
        }

        ForeignKeyConstraint foreignKey = constraint.getForeignKeyConstraint();
        if (foreignKey != null) {
          csv.printRecord(
              table.getCatalogName(),
              table.getSchemaName(),
              table.getName(),
              foreignKey.getName(),
              FOREIGN_KEY_CONSTRAINT_TYPE,
              formatForeignKeyDetails(foreignKey));
        }
      }
    }
  }

  static final class DatabricksUnityCatalogFunctionsTask
      extends AbstractDatabricksUnityCatalogTask {

    DatabricksUnityCatalogFunctionsTask(@Nonnull UnityCatalogSessionCache sessionCache) {
      super(DatabricksMetadataDumpFormat.UNITY_CATALOG_FUNCTIONS_ENTRY_NAME, sessionCache);
    }

    @Override
    @Nonnull
    public String getName() {
      return "databricks-uc-functions";
    }

    @Override
    protected Void doRun(TaskRunContext context, @Nonnull ByteSink sink, @Nonnull Handle handle)
        throws Exception {
      DatabricksHandle dbxHandle = (DatabricksHandle) handle;
      ImmutableList<SchemaInfo> schemas = sessionCache.listSchemas(dbxHandle);
      try (Writer writer = sink.asCharSink(UTF_8).openBufferedStream();
          CSVPrinter csv = FORMAT.withHeader(FunctionsHeader.class).print(writer)) {
        dbxHandle
            .statementRunner()
            .runInOrder(
                "Unity Catalog functions.list",
                schemas,
                DatabricksUnityCatalogTasks::quoteSchemaName,
                schema ->
                    dbxHandle.restHelper().listFunctions(schema.getCatalogName(), schema.getName()),
                (schema, functions) -> {
                  for (FunctionInfo function : functions) {
                    printFunction(csv, schema, function);
                  }
                });
      }
      return null;
    }

    private static void printFunction(
        @Nonnull CSVPrinter csv, @Nonnull SchemaInfo schema, @Nonnull FunctionInfo function)
        throws IOException {
      ImmutableList<DatabricksFunctionParameter> inputParams =
          toFunctionParameters(function.getInputParams());
      ImmutableList<DatabricksFunctionParameter> returnParams =
          toFunctionParameters(function.getReturnParams());
      csv.printRecord(
          schema.getCatalogName(),
          schema.getName(),
          function.getName(),
          formatFunctionDataType(function, returnParams),
          toJsonOrEmpty(inputParams),
          toJsonOrEmpty(returnParams),
          function.getRoutineDefinition(),
          formatRoutineLanguage(function),
          function.getComment(),
          function.getOwner(),
          formatBoolean(function.getIsDeterministic()),
          formatSqlDataAccess(function.getSqlDataAccess()),
          formatEnum(function.getSecurityType()),
          function.getSqlPath(),
          formatTime(function.getCreatedAt()),
          formatTime(function.getUpdatedAt()),
          "",
          function.getExternalName(),
          sortJsonObjectKeys(function.getProperties()));
    }
  }
}
