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

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.quoteIdentifier;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.quoteStringLiteral;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses the {@code DESCRIBE TABLE EXTENDED ... AS JSON} output of Databricks {@code
 * hive_metastore} tables and views, and the partition columns that {@code DESCRIBE DETAIL} reports
 * for Delta tables.
 */
final class DatabricksHiveMetastoreTableParser {

  private static final Logger logger =
      LoggerFactory.getLogger(DatabricksHiveMetastoreTableParser.class);

  private static final ObjectReader JSON_READER = new ObjectMapper().readerFor(JsonNode.class);

  /**
   * The type names of {@code DESCRIBE TABLE ... AS JSON} that the columns files write in another
   * way, such as {@code timestamp} for {@code timestamp_ltz}, as Unity Catalog does.
   */
  private static final ImmutableMap<String, String> TYPE_NAMES =
      ImmutableMap.of(
          "byte", "tinyint",
          "short", "smallint",
          "integer", "int",
          "long", "bigint",
          "timestamp_ltz", "timestamp");

  /** The collation of a {@code string} type that is written without {@code collate}. */
  private static final String DEFAULT_COLLATION = "UTF8_BINARY";

  /**
   * The format names of the class and package names that Spark accepts for its built-in data
   * sources, by lower-case name, such as {@code PARQUET} for {@code org.apache.spark.sql.parquet}.
   * A table keeps the provider that it was created with, so older tables may have these names.
   */
  private static final ImmutableMap<String, String> FORMAT_NAMES_BY_PROVIDER_CLASS =
      ImmutableMap.<String, String>builder()
          .put("org.apache.spark.sql.parquet", "PARQUET")
          .put("org.apache.spark.sql.parquet.defaultsource", "PARQUET")
          .put("org.apache.spark.sql.execution.datasources.parquet", "PARQUET")
          .put("org.apache.spark.sql.execution.datasources.parquet.defaultsource", "PARQUET")
          .put("org.apache.spark.sql.execution.datasources.parquet.parquetfileformat", "PARQUET")
          .put("org.apache.spark.sql.json", "JSON")
          .put("org.apache.spark.sql.json.defaultsource", "JSON")
          .put("org.apache.spark.sql.execution.datasources.json", "JSON")
          .put("org.apache.spark.sql.execution.datasources.json.defaultsource", "JSON")
          .put("org.apache.spark.sql.execution.datasources.json.jsonfileformat", "JSON")
          .put("org.apache.spark.sql.hive.orc", "ORC")
          .put("org.apache.spark.sql.hive.orc.defaultsource", "ORC")
          .put("org.apache.spark.sql.hive.orc.orcfileformat", "ORC")
          .put("org.apache.spark.sql.execution.datasources.orc", "ORC")
          .put("org.apache.spark.sql.execution.datasources.orc.defaultsource", "ORC")
          .put("org.apache.spark.sql.execution.datasources.orc.orcfileformat", "ORC")
          .put("com.databricks.spark.csv", "CSV")
          .put("org.apache.spark.sql.execution.datasources.csv.csvfileformat", "CSV")
          .put("com.databricks.spark.avro", "AVRO")
          .put("org.apache.spark.sql.avro.avrofileformat", "AVRO")
          .put("org.apache.spark.sql.execution.datasources.text.textfileformat", "TEXT")
          .put("org.apache.spark.sql.delta.sources.deltadatasource", "DELTA")
          .put("org.apache.spark.sql.jdbc", "JDBC")
          .put("org.apache.spark.sql.jdbc.defaultsource", "JDBC")
          .put("org.apache.spark.sql.execution.datasources.jdbc", "JDBC")
          .put("org.apache.spark.sql.execution.datasources.jdbc.defaultsource", "JDBC")
          .put("org.apache.spark.sql.execution.datasources.jdbc.jdbcrelationprovider", "JDBC")
          .build();

  private static final String DELTA_CONSTRAINTS_PREFIX = "delta.constraints.";

  private static final String CHECK_CONSTRAINT_TYPE = "CHECK";

  /** The data source format of Delta tables, as {@link #formatDataSourceFormat} writes it. */
  private static final String DELTA_FORMAT = "DELTA";

  /** Matches a struct field name that Spark writes without backticks. */
  private static final Pattern PLAIN_FIELD_NAME_PATTERN =
      Pattern.compile("[A-Za-z0-9_]*[A-Za-z_][A-Za-z0-9_]*");

  private DatabricksHiveMetastoreTableParser() {}

  /** A column of a table or view. */
  static final class ParsedColumn {
    final int ordinalPosition;
    final String columnName;
    final String dataType;
    final String isNullable;
    final String partitionIndex;
    final String comment;

    /** The SQL expression of the {@code DEFAULT} clause, or empty. */
    final String defaultExpression;

    ParsedColumn(
        int ordinalPosition,
        @Nonnull String columnName,
        @Nonnull String dataType,
        @Nonnull String isNullable,
        @Nonnull String partitionIndex,
        @Nonnull String comment,
        @Nonnull String defaultExpression) {
      this.ordinalPosition = ordinalPosition;
      this.columnName = columnName;
      this.dataType = dataType;
      this.isNullable = isNullable;
      this.partitionIndex = partitionIndex;
      this.comment = comment;
      this.defaultExpression = defaultExpression;
    }
  }

  /** A check constraint of a Delta table, from a {@code delta.constraints.<name>} property. */
  static final class ParsedTableConstraint {
    final String constraintName;
    final String constraintType;
    final String expression;

    ParsedTableConstraint(String constraintName, String constraintType, String expression) {
      this.constraintName = constraintName;
      this.constraintType = constraintType;
      this.expression = expression;
    }
  }

  /**
   * A table or view parsed from its {@code DESCRIBE TABLE EXTENDED ... AS JSON} output.
   *
   * <p>The catalog that Databricks reports, which may be {@code spark_catalog}, isn't kept: the
   * files always name the catalog {@code hive_metastore}. A table that couldn't be described has
   * only its schema and name.
   */
  static final class ParsedTable {
    final String schemaName;
    final String tableName;

    /** Whether the table was described, so that the values other than the names are known. */
    final boolean described;

    final String tableType;
    final String dataSourceFormat;
    final String storageLocation;
    final String comment;
    final String owner;
    final String createdAt;
    final String serdeLibrary;
    final String inputFormat;
    final String outputFormat;
    final ImmutableMap<String, String> storageProperties;
    final ImmutableMap<String, String> tableProperties;
    final String numBuckets;
    final ImmutableList<String> bucketColumns;
    final ImmutableList<String> sortColumns;
    @CheckForNull final String viewDefinition;
    final String viewCatalogAndNamespace;
    final ImmutableList<String> viewQueryOutputColumns;
    final String viewSchemaMode;
    final String sqlPath;
    final ImmutableList<ParsedColumn> columns;
    final ImmutableList<ParsedTableConstraint> constraints;

    /**
     * Whether the partition indexes of the columns are known. {@code DESCRIBE TABLE ... AS JSON}
     * doesn't report the partition columns of a Delta table, so they are unknown until {@link
     * #withPartitionColumns} sets them.
     */
    final boolean partitioningKnown;

    private ParsedTable(@Nonnull String schemaName, @Nonnull String tableName) {
      this.schemaName = schemaName;
      this.tableName = tableName;
      this.described = false;
      this.tableType = "";
      this.dataSourceFormat = "";
      this.storageLocation = "";
      this.comment = "";
      this.owner = "";
      this.createdAt = "";
      this.serdeLibrary = "";
      this.inputFormat = "";
      this.outputFormat = "";
      this.storageProperties = ImmutableMap.of();
      this.tableProperties = ImmutableMap.of();
      this.numBuckets = "";
      this.bucketColumns = ImmutableList.of();
      this.sortColumns = ImmutableList.of();
      this.viewDefinition = null;
      this.viewCatalogAndNamespace = "";
      this.viewQueryOutputColumns = ImmutableList.of();
      this.viewSchemaMode = "";
      this.sqlPath = "";
      this.columns = ImmutableList.of();
      this.constraints = ImmutableList.of();
      this.partitioningKnown = false;
    }

    private ParsedTable(
        @Nonnull String schemaName, @Nonnull String tableName, @Nonnull JsonNode description) {
      this.schemaName = schemaName;
      this.tableName = tableName;
      this.described = true;
      this.tableType = text(description, "type");
      this.dataSourceFormat = formatDataSourceFormat(text(description, "provider"));
      this.storageLocation = text(description, "location");
      this.comment = text(description, "comment");
      this.owner = text(description, "owner");
      this.createdAt =
          formatCreatedTime(
              text(description, "created_time"),
              quoteIdentifier(schemaName) + "." + quoteIdentifier(tableName));
      this.serdeLibrary = text(description, "serde_library");
      this.inputFormat = text(description, "input_format");
      this.outputFormat = text(description, "output_format");
      this.storageProperties = textMap(description, "storage_properties");
      this.tableProperties = textMap(description, "table_properties");
      this.numBuckets = text(description, "num_buckets");
      this.bucketColumns = textList(description, "bucket_columns");
      this.sortColumns = textList(description, "sort_columns");
      JsonNode viewText = description.path("view_text");
      this.viewDefinition = isAbsent(viewText) ? null : viewText.asText();
      this.viewCatalogAndNamespace = text(description, "view_catalog_and_namespace");
      this.viewQueryOutputColumns = textList(description, "view_query_output_columns");
      this.viewSchemaMode = text(description, "view_schema_mode");
      this.sqlPath = text(description, "sql_path");
      this.columns =
          withPartitionIndexes(
              parseColumns(description.path("columns")),
              textList(description, "partition_columns"));
      this.constraints = parseDeltaCheckConstraints(tableProperties);
      this.partitioningKnown = !DELTA_FORMAT.equals(dataSourceFormat);
    }

    /** Copies a table with other columns, whose partition indexes are known. */
    private ParsedTable(@Nonnull ParsedTable table, @Nonnull ImmutableList<ParsedColumn> columns) {
      this.schemaName = table.schemaName;
      this.tableName = table.tableName;
      this.described = table.described;
      this.tableType = table.tableType;
      this.dataSourceFormat = table.dataSourceFormat;
      this.storageLocation = table.storageLocation;
      this.comment = table.comment;
      this.owner = table.owner;
      this.createdAt = table.createdAt;
      this.serdeLibrary = table.serdeLibrary;
      this.inputFormat = table.inputFormat;
      this.outputFormat = table.outputFormat;
      this.storageProperties = table.storageProperties;
      this.tableProperties = table.tableProperties;
      this.numBuckets = table.numBuckets;
      this.bucketColumns = table.bucketColumns;
      this.sortColumns = table.sortColumns;
      this.viewDefinition = table.viewDefinition;
      this.viewCatalogAndNamespace = table.viewCatalogAndNamespace;
      this.viewQueryOutputColumns = table.viewQueryOutputColumns;
      this.viewSchemaMode = table.viewSchemaMode;
      this.sqlPath = table.sqlPath;
      this.columns = columns;
      this.constraints = table.constraints;
      this.partitioningKnown = true;
    }

    boolean isDelta() {
      return DELTA_FORMAT.equals(dataSourceFormat);
    }

    /**
     * Returns this table with the partition indexes of {@code partitionColumns}, the names of the
     * partition columns in their order, such as those that {@code DESCRIBE DETAIL} reports for a
     * Delta table.
     */
    @Nonnull
    ParsedTable withPartitionColumns(@Nonnull List<String> partitionColumns) {
      return new ParsedTable(this, withPartitionIndexes(columns, partitionColumns));
    }
  }

  /**
   * Parses the {@code DESCRIBE TABLE EXTENDED ... AS JSON} output of a table or view.
   *
   * @throws IOException if the output isn't a JSON object
   */
  @Nonnull
  static ParsedTable parseDescribeTableJson(
      @Nonnull String schemaName, @Nonnull String tableName, @CheckForNull String json)
      throws IOException {
    JsonNode description = json == null ? null : JSON_READER.readTree(json);
    if (description == null || !description.isObject()) {
      throw new IOException(
          String.format(
              "DESCRIBE TABLE AS JSON returned no JSON object for %s.%s.",
              quoteIdentifier(schemaName), quoteIdentifier(tableName)));
    }

    return new ParsedTable(schemaName, tableName, description);
  }

  /** Returns a table that couldn't be described, with only its schema and name. */
  @Nonnull
  static ParsedTable undescribedTable(@Nonnull String schemaName, @Nonnull String tableName) {
    return new ParsedTable(schemaName, tableName);
  }

  /**
   * Returns a column type of {@code DESCRIBE TABLE ... AS JSON}, such as {@code {"name": "decimal",
   * "precision": 10, "scale": 2}}, in the syntax of the columns files, such as {@code
   * decimal(10,2)} or {@code array<struct<x:bigint,y:string>>}. A {@code string} type with a
   * collation other than {@code UTF8_BINARY} is written as {@code string collate <collation>}. A
   * struct field is followed by {@code not null} and {@code comment '...'} when they apply, as in
   * {@code struct<id:bigint not null comment 'key'>}, and its name is quoted with backticks if it
   * isn't a plain identifier.
   */
  @Nonnull
  static String formatDataType(@Nonnull JsonNode type) {
    String name = type.path("name").asText("").toLowerCase(Locale.ROOT);
    switch (name) {
      case "decimal":
        if (isAbsent(type.path("precision"))) {
          return name;
        }

        return String.format(
            "decimal(%s,%s)", type.path("precision").asText(), type.path("scale").asText("0"));
      case "char":
      case "varchar":
        return name + "(" + type.path("length").asText() + ")";
      case "string":
        String collation = type.path("collation").asText("");
        return collation.isEmpty() || DEFAULT_COLLATION.equalsIgnoreCase(collation)
            ? name
            : name + " collate " + collation;
      case "interval":
        String startUnit = type.path("start_unit").asText("").toLowerCase(Locale.ROOT);
        String endUnit = type.path("end_unit").asText("").toLowerCase(Locale.ROOT);
        if (startUnit.isEmpty()) {
          return name;
        }

        return endUnit.isEmpty() || endUnit.equals(startUnit)
            ? name + " " + startUnit
            : name + " " + startUnit + " to " + endUnit;
      case "array":
        return "array<" + formatDataType(type.path("element_type")) + ">";
      case "map":
        return "map<"
            + formatDataType(type.path("key_type"))
            + ","
            + formatDataType(type.path("value_type"))
            + ">";
      case "struct":
        StringBuilder struct = new StringBuilder("struct<");
        for (JsonNode field : type.path("fields")) {
          if (struct.length() > "struct<".length()) {
            struct.append(',');
          }
          struct
              .append(quoteFieldNameIfNeeded(text(field, "name")))
              .append(':')
              .append(formatDataType(field.path("type")));
          if (!field.path("nullable").asBoolean(true)) {
            struct.append(" not null");
          }
          String comment = text(field, "comment");
          if (!comment.isEmpty()) {
            struct.append(" comment ").append(quoteStringLiteral(comment));
          }
        }
        return struct.append('>').toString();
      default:
        return TYPE_NAMES.getOrDefault(name, name);
    }
  }

  /**
   * Returns a struct field name as Spark writes it in a type: as is if it has only letters, digits
   * and underscores and isn't a number, and otherwise quoted with backticks.
   */
  @Nonnull
  private static String quoteFieldNameIfNeeded(@Nonnull String name) {
    return PLAIN_FIELD_NAME_PATTERN.matcher(name).matches() ? name : quoteIdentifier(name);
  }

  /**
   * Returns a time of {@code DESCRIBE TABLE ... AS JSON}, such as {@code 2026-09-14T16:19:04Z}, as
   * an ISO 8601 instant. A time in another format is logged and returned as an empty string, so
   * that the {@code CreatedAt} columns hold only instants.
   *
   * @param objectName the quoted name of the table, for the warning
   */
  @Nonnull
  private static String formatCreatedTime(@Nonnull String createdTime, @Nonnull String objectName) {
    if (createdTime.isEmpty()) {
      return "";
    }

    try {
      return OffsetDateTime.parse(createdTime).toInstant().toString();
    } catch (DateTimeParseException e) {
      logger.warn(
          "Writing {} without CreatedAt because its created time '{}' isn't an ISO 8601 time.",
          objectName,
          createdTime);
      return "";
    }
  }

  /**
   * Returns the data source format of a table from its {@code provider}, as Unity Catalog names the
   * formats: a short name in upper case, such as {@code PARQUET} for {@code parquet}. A class or
   * package name that Spark accepts for a built-in format, such as {@code
   * org.apache.spark.sql.parquet}, gets the name of that format. Another class name, such as that
   * of a custom data source, is kept as is, because class names are case-sensitive.
   */
  @Nonnull
  static String formatDataSourceFormat(@Nonnull String provider) {
    String formatName = FORMAT_NAMES_BY_PROVIDER_CLASS.get(provider.toLowerCase(Locale.ROOT));
    if (formatName != null) {
      return formatName;
    }

    return provider.contains(".") ? provider : provider.toUpperCase(Locale.ROOT);
  }

  /** Returns the check constraints in the {@code delta.constraints.<name>} table properties. */
  @Nonnull
  static ImmutableList<ParsedTableConstraint> parseDeltaCheckConstraints(
      @Nonnull Map<String, String> tableProperties) {
    ImmutableList.Builder<ParsedTableConstraint> constraints = ImmutableList.builder();
    for (Map.Entry<String, String> property : tableProperties.entrySet()) {
      String key = property.getKey();
      if (!key.toLowerCase(Locale.ROOT).startsWith(DELTA_CONSTRAINTS_PREFIX)) {
        continue;
      }

      String constraintName = key.substring(DELTA_CONSTRAINTS_PREFIX.length()).trim();
      if (!constraintName.isEmpty()) {
        constraints.add(
            new ParsedTableConstraint(
                constraintName, CHECK_CONSTRAINT_TYPE, property.getValue().trim()));
      }
    }
    return constraints.build();
  }

  /**
   * Returns the partition columns that {@code DESCRIBE DETAIL} reports for a Delta table: its
   * {@code partitionColumns} cell, a JSON array of names such as {@code ["region","dt"]}.
   *
   * @throws IOException if the cell isn't a JSON array of names
   */
  @Nonnull
  static ImmutableList<String> parseDeltaPartitionColumns(@CheckForNull String partitionColumns)
      throws IOException {
    JsonNode names = partitionColumns == null ? null : JSON_READER.readTree(partitionColumns);
    if (names == null || !names.isArray()) {
      throw new IOException(
          String.format(
              "DESCRIBE DETAIL returned partition columns that aren't a JSON array: %s",
              partitionColumns));
    }

    ImmutableList.Builder<String> builder = ImmutableList.builder();
    for (JsonNode name : names) {
      if (!name.isTextual()) {
        throw new IOException(
            String.format(
                "DESCRIBE DETAIL returned a partition column that isn't a name: %s",
                partitionColumns));
      }

      builder.add(name.asText());
    }
    return builder.build();
  }

  /** Returns the columns in their order, without partition indexes. */
  @Nonnull
  private static ImmutableList<ParsedColumn> parseColumns(@Nonnull JsonNode columns) {
    ImmutableList.Builder<ParsedColumn> parsedColumns = ImmutableList.builder();
    int ordinalPosition = 0;
    for (JsonNode column : columns) {
      parsedColumns.add(
          new ParsedColumn(
              ordinalPosition,
              text(column, "name"),
              formatDataType(column.path("type")),
              column.path("nullable").asBoolean(true) ? "YES" : "NO",
              "",
              text(column, "comment"),
              text(column, "default")));
      ordinalPosition++;
    }
    return parsedColumns.build();
  }

  /**
   * Returns the columns with the 0-based position of each partition column in the partitioning. The
   * names are matched case-insensitively, as Spark resolves them.
   */
  @Nonnull
  private static ImmutableList<ParsedColumn> withPartitionIndexes(
      @Nonnull List<ParsedColumn> columns, @Nonnull List<String> partitionColumns) {
    Map<String, Integer> partitionIndexByLowerName = new LinkedHashMap<>();
    for (int index = 0; index < partitionColumns.size(); index++) {
      partitionIndexByLowerName.putIfAbsent(
          partitionColumns.get(index).toLowerCase(Locale.ROOT), index);
    }

    ImmutableList.Builder<ParsedColumn> indexedColumns = ImmutableList.builder();
    for (ParsedColumn column : columns) {
      Integer partitionIndex =
          partitionIndexByLowerName.get(column.columnName.toLowerCase(Locale.ROOT));
      indexedColumns.add(
          new ParsedColumn(
              column.ordinalPosition,
              column.columnName,
              column.dataType,
              column.isNullable,
              partitionIndex == null ? "" : String.valueOf(partitionIndex),
              column.comment,
              column.defaultExpression));
    }
    return indexedColumns.build();
  }

  /** Returns the value of a field as text, as {@link #asText} does. */
  @Nonnull
  private static String text(@Nonnull JsonNode node, @Nonnull String field) {
    return asText(node.path(field));
  }

  /** Returns the values of a list as text, or an empty list if the list is absent. */
  @Nonnull
  private static ImmutableList<String> textList(@Nonnull JsonNode node, @Nonnull String field) {
    ImmutableList.Builder<String> values = ImmutableList.builder();
    for (JsonNode value : node.path(field)) {
      values.add(asText(value));
    }
    return values.build();
  }

  /** Returns the entries of an object as text, in their order, or an empty map if it's absent. */
  @Nonnull
  private static ImmutableMap<String, String> textMap(
      @Nonnull JsonNode node, @Nonnull String field) {
    JsonNode object = node.path(field);
    if (!object.isObject()) {
      return ImmutableMap.of();
    }

    Map<String, String> entries = new LinkedHashMap<>();
    for (Map.Entry<String, JsonNode> entry : object.properties()) {
      entries.put(entry.getKey(), asText(entry.getValue()));
    }
    return ImmutableMap.copyOf(entries);
  }

  /**
   * Returns a value as text: a string as is, another scalar as printed, a list or object as JSON,
   * and an absent value as an empty string.
   */
  @Nonnull
  private static String asText(@Nonnull JsonNode value) {
    if (isAbsent(value)) {
      return "";
    }

    return value.isContainerNode() ? value.toString() : value.asText();
  }

  private static boolean isAbsent(@Nonnull JsonNode value) {
    return value.isMissingNode() || value.isNull();
  }
}
