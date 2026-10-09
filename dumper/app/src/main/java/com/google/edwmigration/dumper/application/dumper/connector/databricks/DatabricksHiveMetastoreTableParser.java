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
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.cellOrEmpty;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.quoteIdentifier;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses the {@code SHOW TABLE EXTENDED} and {@code DESCRIBE TABLE} output of Databricks {@code
 * hive_metastore} tables.
 */
final class DatabricksHiveMetastoreTableParser {

  private static final Logger logger =
      LoggerFactory.getLogger(DatabricksHiveMetastoreTableParser.class);

  /** The prefix of every field line in a {@code Schema} tree. */
  private static final String SCHEMA_TREE_LINE_PREFIX = " |";

  /** The indentation that each nesting level adds to a field line in a {@code Schema} tree. */
  private static final String SCHEMA_TREE_INDENT = "    |";

  /** The marker between the indentation and the field in a {@code Schema} tree line. */
  private static final String SCHEMA_TREE_FIELD_MARKER = "-- ";

  /** The separator between the name and the type of a field in a {@code Schema} tree line. */
  private static final String SCHEMA_TREE_TYPE_SEPARATOR = ": ";

  /**
   * Matches the nullability that ends a {@code Schema} tree line, such as {@code (nullable =
   * true)}.
   */
  private static final Pattern SCHEMA_TREE_NULLABILITY_PATTERN =
      Pattern.compile(" \\((?:nullable|containsNull|valueContainsNull) = (true|false)\\)$");

  private static final String STRUCT_TYPE_NAME = "struct";

  /**
   * The Spark type names of the types whose element, key, value or field types are nested lines.
   */
  private static final ImmutableSet<String> NESTED_TYPE_NAMES =
      ImmutableSet.of("array", "map", STRUCT_TYPE_NAME);

  /**
   * The Spark type names that differ in Hive type syntax, such as {@code long} for {@code bigint}.
   */
  private static final ImmutableMap<String, String> HIVE_TYPE_NAMES =
      ImmutableMap.of("byte", "tinyint", "short", "smallint", "integer", "int", "long", "bigint");

  private static final String STRING_TYPE_NAME = "string";

  /**
   * Matches a {@code CHAR} or {@code VARCHAR} type with its length, such as {@code varchar(20)}.
   */
  private static final Pattern CHAR_VARCHAR_TYPE_PATTERN =
      Pattern.compile("\\b(?:var)?char\\(\\d+\\)", Pattern.CASE_INSENSITIVE);

  /** The format of {@code java.util.Date#toString}, which Spark uses for {@code Created Time}. */
  private static final String CREATED_TIME_PATTERN = "EEE MMM dd HH:mm:ss zzz yyyy";

  private static final DateTimeFormatter CREATED_TIME_FORMATTER =
      DateTimeFormatter.ofPattern(CREATED_TIME_PATTERN, Locale.US);

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

  /**
   * Matches the key that starts a property entry, such as {@code delta.minReaderVersion=}. A key
   * has no whitespace, so the text after {@code , } in a value such as {@code x IN (1, 2)} doesn't
   * start an entry.
   */
  private static final Pattern PROPERTY_KEY_PATTERN = Pattern.compile("[^\\s=,()\\[\\]]+=");

  /** The separator between the entries of a list in {@code SHOW TABLE EXTENDED} output. */
  private static final String LIST_SEPARATOR = ", ";

  /**
   * The keys of Spark's {@code CatalogTable.toLinkedHashMap}, in the order that {@code SHOW TABLE
   * EXTENDED} prints them. The metric view key {@code Language} is left out, because {@code
   * hive_metastore} has no metric views and a comment line such as {@code Language: English} would
   * otherwise end a comment.
   */
  private static final ImmutableList<String> TABLE_KEYS_IN_ORDER =
      ImmutableList.of(
          "Catalog",
          "Database",
          "Table",
          "Owner",
          "Created Time",
          "Last Access",
          "Created By",
          "Type",
          "Provider",
          "Num Buckets",
          "Bucket Columns",
          "Sort Columns",
          "Comment",
          "Collation",
          "View Text",
          "View Original Text",
          "View Schema Mode",
          "View Catalog and Namespace",
          "SQL Path",
          "View Query Output Columns",
          "Table Properties",
          "Statistics",
          "Location",
          "Serde Name",
          "Serde Library",
          "InputFormat",
          "OutputFormat",
          "Compressed",
          "Storage Properties",
          "Partition Provider",
          "Partition Columns",
          "Schema");

  /** The keys whose values Spark prints verbatim, so that a value may span lines. */
  private static final ImmutableSet<String> MULTI_LINE_TABLE_KEYS =
      ImmutableSet.of(
          "Comment", "View Text", "View Original Text", "Table Properties", "Storage Properties");

  private DatabricksHiveMetastoreTableParser() {}

  /**
   * A parsed column from the {@code SHOW TABLE EXTENDED} {@code Schema} tree. The {@code Schema}
   * tree has no column comments, so a parsed column has an empty comment until it is combined with
   * {@code DESCRIBE TABLE} output.
   */
  static final class ParsedColumn {
    final int ordinalPosition;
    final String columnName;
    final String dataType;
    final String isNullable;
    final String partitionIndex;
    final String comment;

    ParsedColumn(
        int ordinalPosition,
        String columnName,
        String dataType,
        String isNullable,
        String partitionIndex,
        String comment) {
      this.ordinalPosition = ordinalPosition;
      this.columnName = columnName;
      this.dataType = dataType;
      this.isNullable = isNullable;
      this.partitionIndex = partitionIndex;
      this.comment = comment;
    }

    /** Returns this column with another data type and comment. */
    @Nonnull
    ParsedColumn withDataTypeAndComment(@Nonnull String dataType, @Nonnull String comment) {
      return new ParsedColumn(
          ordinalPosition, columnName, dataType, isNullable, partitionIndex, comment);
    }
  }

  /** The type and comment of a column in {@code DESCRIBE TABLE} output. */
  static final class DescribedColumn {
    final String dataType;
    final String comment;

    DescribedColumn(@Nonnull String dataType, @Nonnull String comment) {
      this.dataType = dataType;
      this.comment = comment;
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

  /** A field line of a {@code Schema} tree, such as {@code |-- id: integer (nullable = true)}. */
  private static final class SchemaTreeLine {
    final int depth;
    final String name;
    final String typeName;
    final boolean nullable;

    SchemaTreeLine(int depth, String name, String typeName, boolean nullable) {
      this.depth = depth;
      this.name = name;
      this.typeName = typeName;
      this.nullable = nullable;
    }
  }

  /**
   * A table or view parsed from its {@code SHOW TABLE EXTENDED} information.
   *
   * <p>The {@code Catalog} that Spark prints, which may be {@code spark_catalog}, isn't kept: the
   * files always name the catalog {@code hive_metastore}.
   */
  static final class ParsedTableExtended {
    final String schemaName;
    final String tableName;
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

    private ParsedTableExtended(
        @Nonnull String defaultSchema,
        @Nonnull String defaultTable,
        @Nonnull ImmutableMap<String, String> values) {
      this.schemaName = valueOrDefault(values, "Database", defaultSchema);
      this.tableName = valueOrDefault(values, "Table", defaultTable);
      this.tableType = values.getOrDefault("Type", "");
      this.dataSourceFormat = formatDataSourceFormat(values.getOrDefault("Provider", ""));
      this.storageLocation = values.getOrDefault("Location", "");
      this.comment = values.getOrDefault("Comment", "");
      this.owner = values.getOrDefault("Owner", "");
      this.createdAt =
          formatCreatedTime(
              values.getOrDefault("Created Time", ""),
              quoteIdentifier(schemaName) + "." + quoteIdentifier(tableName));
      this.serdeLibrary = values.getOrDefault("Serde Library", "");
      this.inputFormat = values.getOrDefault("InputFormat", "");
      this.outputFormat = values.getOrDefault("OutputFormat", "");
      this.storageProperties =
          parseBracketedProperties(values.getOrDefault("Storage Properties", ""));
      this.tableProperties = parseBracketedProperties(values.getOrDefault("Table Properties", ""));
      this.numBuckets = values.getOrDefault("Num Buckets", "");
      this.bucketColumns = parseIdentifierList(values.getOrDefault("Bucket Columns", ""));
      this.sortColumns = parseIdentifierList(values.getOrDefault("Sort Columns", ""));
      this.viewDefinition = values.get("View Text");
      this.viewCatalogAndNamespace = values.getOrDefault("View Catalog and Namespace", "");
      this.viewQueryOutputColumns =
          parseIdentifierList(values.getOrDefault("View Query Output Columns", ""));
      this.viewSchemaMode = values.getOrDefault("View Schema Mode", "");
      this.sqlPath = values.getOrDefault("SQL Path", "");
      this.columns =
          parseSchemaColumns(
              values.getOrDefault("Schema", ""),
              parseIdentifierList(values.getOrDefault("Partition Columns", "")));
      this.constraints = parseDeltaCheckConstraints(tableProperties);
    }
  }

  @Nonnull
  static ParsedTableExtended parseShowTableExtended(
      @Nonnull String defaultSchema,
      @Nonnull String defaultTable,
      @CheckForNull String informationBlob) {
    return new ParsedTableExtended(
        defaultSchema, defaultTable, splitTableInformation(Strings.nullToEmpty(informationBlob)));
  }

  /**
   * Splits a {@code SHOW TABLE EXTENDED} information blob into values by key.
   *
   * <p>Spark prints one {@code Key: value} line per key, in {@link #TABLE_KEYS_IN_ORDER} order, and
   * prints the values of {@link #MULTI_LINE_TABLE_KEYS} verbatim, so a line of such a value may
   * look like a key line, as the comment line {@code Owner: data team} does. The key lines are
   * therefore the most lines that start with keys in that order. Of equally many, the key line
   * after a multi-line value is the latest candidate, because text that imitates a key line
   * precedes the real one, and the key line after any other value is the earliest candidate,
   * because Spark prints it right after that value.
   */
  @Nonnull
  private static ImmutableMap<String, String> splitTableInformation(
      @Nonnull String informationBlob) {
    String[] lines = informationBlob.split("\n");
    int[] keyIndexes = new int[lines.length];
    int keyLineCount = 0;
    for (int lineIndex = 0; lineIndex < lines.length; lineIndex++) {
      keyIndexes[lineIndex] = findTableKeyIndex(lines[lineIndex]);
      if (keyIndexes[lineIndex] >= 0) {
        keyLineCount++;
      }
    }

    // skippedCounts[p][state] is the fewest key lines to skip from the p-th key line on, where
    // state is 0 before the first key and otherwise one more than the index of the last key.
    int keyCount = TABLE_KEYS_IN_ORDER.size();
    int[][] skippedCounts = new int[keyLineCount + 1][keyCount + 1];
    int keyLinePosition = keyLineCount;
    for (int lineIndex = lines.length - 1; lineIndex >= 0; lineIndex--) {
      int keyIndex = keyIndexes[lineIndex];
      if (keyIndex < 0) {
        continue;
      }

      keyLinePosition--;
      for (int state = 0; state <= keyCount; state++) {
        int skippedIfNotStarted = skippedCounts[keyLinePosition + 1][state] + 1;
        skippedCounts[keyLinePosition][state] =
            keyIndex >= state
                ? Math.min(skippedCounts[keyLinePosition + 1][keyIndex + 1], skippedIfNotStarted)
                : skippedIfNotStarted;
      }
    }

    ImmutableMap.Builder<String, String> values = ImmutableMap.builder();
    String key = null;
    StringBuilder value = new StringBuilder();
    int state = 0;
    int keyLinesSeen = 0;
    for (int lineIndex = 0; lineIndex < lines.length; lineIndex++) {
      int keyIndex = keyIndexes[lineIndex];
      boolean startsKey = false;
      if (keyIndex >= 0) {
        keyLinesSeen++;
        if (keyIndex >= state) {
          int skippedIfStarted = skippedCounts[keyLinesSeen][keyIndex + 1];
          int skippedIfNotStarted = skippedCounts[keyLinesSeen][state] + 1;
          startsKey =
              skippedIfStarted < skippedIfNotStarted
                  || (skippedIfStarted == skippedIfNotStarted
                      && !MULTI_LINE_TABLE_KEYS.contains(key));
        }
      }

      if (startsKey) {
        if (key != null) {
          values.put(key, finishTableValue(key, value));
        }
        key = TABLE_KEYS_IN_ORDER.get(keyIndex);
        value.setLength(0);
        value.append(parseKeyLineValue(lines[lineIndex], key));
        state = keyIndex + 1;
      } else if (MULTI_LINE_TABLE_KEYS.contains(key) || "Schema".equals(key)) {
        value.append('\n').append(lines[lineIndex]);
      }
    }
    if (key != null) {
      values.put(key, finishTableValue(key, value));
    }
    return values.build();
  }

  /**
   * Returns the index in {@link #TABLE_KEYS_IN_ORDER} of the key that starts the line, as in {@code
   * Key: value} or, for an empty value, {@code Key}, or -1 if no key starts the line.
   */
  private static int findTableKeyIndex(@Nonnull String line) {
    for (int keyIndex = 0; keyIndex < TABLE_KEYS_IN_ORDER.size(); keyIndex++) {
      String key = TABLE_KEYS_IN_ORDER.get(keyIndex);
      if (line.startsWith(key)
          && (line.length() == key.length() || line.charAt(key.length()) == ':')) {
        return keyIndex;
      }
    }
    return -1;
  }

  /** Returns the value in a line that starts with the key, such as {@code v} in {@code Key: v}. */
  @Nonnull
  private static String parseKeyLineValue(@Nonnull String line, @Nonnull String key) {
    String value = line.substring(Math.min(line.length(), key.length() + 1));
    return value.startsWith(" ") ? value.substring(1) : value;
  }

  @Nonnull
  private static String finishTableValue(@Nonnull String key, @Nonnull StringBuilder value) {
    return MULTI_LINE_TABLE_KEYS.contains(key) ? value.toString() : value.toString().trim();
  }

  @Nonnull
  private static String valueOrDefault(
      @Nonnull Map<String, String> values, @Nonnull String key, @Nonnull String defaultValue) {
    String value = values.get(key);
    return Strings.isNullOrEmpty(value) ? defaultValue : value;
  }

  /**
   * Returns a time that Spark prints with {@code java.util.Date#toString}, such as the {@code
   * Created Time} {@code Sun Sep 27 21:19:25 UTC 2026}, as an ISO 8601 instant, such as {@code
   * 2026-09-27T21:19:25Z}. A time in another format is logged and returned as an empty string, so
   * that the {@code CreatedAt} columns hold only instants, as for Unity Catalog.
   *
   * @param objectName the quoted name of the table or function, for the warning
   */
  @Nonnull
  static String formatCreatedTime(@Nonnull String createdTime, @Nonnull String objectName) {
    if (createdTime.isEmpty()) {
      return "";
    }

    try {
      return ZonedDateTime.parse(createdTime, CREATED_TIME_FORMATTER).toInstant().toString();
    } catch (DateTimeParseException e) {
      logger.warn(
          "Writing {} without CreatedAt because its created time '{}' doesn't match '{}'.",
          objectName,
          createdTime,
          CREATED_TIME_PATTERN);
      return "";
    }
  }

  /**
   * Returns the data source format of a table from its {@code Provider}, as Unity Catalog names the
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

  /**
   * Parses properties printed as {@code [key=value, ...]}, such as {@code Table Properties}, in
   * their printed order.
   *
   * <p>Spark prints the keys and values without escaping, so the entries are split on a best-effort
   * basis: an entry ends before {@code , } followed by a key and {@code =}, unless that text is in
   * parentheses or brackets, as in the check constraint {@code status IN ('new', 'done')}. A value
   * that contains such text outside of parentheses, as in {@code a, b=c}, is split.
   */
  @Nonnull
  static ImmutableMap<String, String> parseBracketedProperties(@Nonnull String text) {
    String body = text.trim();
    if (body.startsWith("[") && body.endsWith("]")) {
      body = body.substring(1, body.length() - 1);
    }
    if (body.isEmpty()) {
      return ImmutableMap.of();
    }

    Map<String, String> properties = new LinkedHashMap<>();
    Matcher keyMatcher = PROPERTY_KEY_PATTERN.matcher(body);
    int depth = 0;
    int entryStart = 0;
    for (int index = 0; index < body.length(); index++) {
      char c = body.charAt(index);
      if (c == '(' || c == '[') {
        depth++;
      } else if ((c == ')' || c == ']') && depth > 0) {
        depth--;
      } else if (depth == 0
          && body.startsWith(LIST_SEPARATOR, index)
          && keyMatcher.region(index + LIST_SEPARATOR.length(), body.length()).lookingAt()) {
        putProperty(body.substring(entryStart, index), properties);
        entryStart = index + LIST_SEPARATOR.length();
      }
    }
    putProperty(body.substring(entryStart), properties);
    return ImmutableMap.copyOf(properties);
  }

  /** Adds a {@code key=value} entry. An entry without a key is kept as a key with no value. */
  private static void putProperty(@Nonnull String entry, @Nonnull Map<String, String> properties) {
    int separator = entry.indexOf('=');
    if (separator <= 0) {
      properties.put(entry, "");
    } else {
      properties.put(entry.substring(0, separator), entry.substring(separator + 1));
    }
  }

  /**
   * Parses a list of identifiers printed as {@code [`a`, `b`]}, such as {@code Partition Columns}.
   * Spark quotes each identifier with backticks and doubles a backtick in it. An identifier without
   * backticks ends before the next {@code , }.
   */
  @Nonnull
  static ImmutableList<String> parseIdentifierList(@Nonnull String text) {
    String body = text.trim();
    if (body.startsWith("[") && body.endsWith("]")) {
      body = body.substring(1, body.length() - 1);
    }

    ImmutableList.Builder<String> identifiers = ImmutableList.builder();
    int index = 0;
    while (index < body.length()) {
      char c = body.charAt(index);
      if (c == ',' || Character.isWhitespace(c)) {
        index++;
      } else if (c == '`') {
        StringBuilder identifier = new StringBuilder();
        index++;
        while (index < body.length()) {
          char next = body.charAt(index);
          if (next != '`') {
            identifier.append(next);
            index++;
          } else if (body.startsWith("``", index)) {
            identifier.append('`');
            index += 2;
          } else {
            index++;
            break;
          }
        }
        identifiers.add(identifier.toString());
      } else {
        int end = body.indexOf(LIST_SEPARATOR, index);
        if (end < 0) {
          end = body.length();
        }
        identifiers.add(body.substring(index, end).trim());
        index = end;
      }
    }
    return identifiers.build();
  }

  /**
   * Parses the columns of a {@code Schema} tree. Each type is written in Hive type syntax, such as
   * {@code bigint} for a {@code long} line and {@code array<struct<x:bigint>>} for an {@code array}
   * line and its nested lines.
   */
  @Nonnull
  private static ImmutableList<ParsedColumn> parseSchemaColumns(
      @Nonnull String schemaTree, @Nonnull List<String> partitionCols) {
    Map<String, Integer> partitionIndexByLowerCol = new LinkedHashMap<>();
    for (int idx = 0; idx < partitionCols.size(); idx++) {
      partitionIndexByLowerCol.put(partitionCols.get(idx).toLowerCase(Locale.ROOT), idx);
    }

    ImmutableList.Builder<SchemaTreeLine> treeLinesBuilder = ImmutableList.builder();
    for (String line : schemaTree.split("\n")) {
      SchemaTreeLine treeLine = parseSchemaTreeLine(line);
      if (treeLine != null) {
        treeLinesBuilder.add(treeLine);
      }
    }
    ImmutableList<SchemaTreeLine> treeLines = treeLinesBuilder.build();

    ImmutableList.Builder<ParsedColumn> columns = ImmutableList.builder();
    int ordinal = 0;
    int index = 0;
    while (index < treeLines.size()) {
      SchemaTreeLine field = treeLines.get(index);
      if (field.depth > 0) {
        index++;
        continue;
      }

      StringBuilder dataType = new StringBuilder();
      index = appendHiveType(treeLines, index, dataType);
      Integer pIdx = partitionIndexByLowerCol.get(field.name.toLowerCase(Locale.ROOT));
      columns.add(
          new ParsedColumn(
              ordinal,
              field.name,
              dataType.toString(),
              field.nullable ? "YES" : "NO",
              pIdx != null ? String.valueOf(pIdx) : "",
              ""));
      ordinal++;
    }
    return columns.build();
  }

  /**
   * Parses a field line of a {@code Schema} tree, or returns null for another line. The name is the
   * text before the last colon and space, because a name may contain a colon and a space and a type
   * name doesn't.
   */
  @CheckForNull
  private static SchemaTreeLine parseSchemaTreeLine(@Nonnull String line) {
    if (!line.startsWith(SCHEMA_TREE_LINE_PREFIX)) {
      return null;
    }

    int offset = SCHEMA_TREE_LINE_PREFIX.length();
    int depth = 0;
    while (line.startsWith(SCHEMA_TREE_INDENT, offset)) {
      offset += SCHEMA_TREE_INDENT.length();
      depth++;
    }
    if (!line.startsWith(SCHEMA_TREE_FIELD_MARKER, offset)) {
      return null;
    }

    String field =
        whitespace().trimTrailingFrom(line.substring(offset + SCHEMA_TREE_FIELD_MARKER.length()));
    boolean nullable = true;
    Matcher nullability = SCHEMA_TREE_NULLABILITY_PATTERN.matcher(field);
    if (nullability.find()) {
      nullable = Boolean.parseBoolean(nullability.group(1));
      field = field.substring(0, nullability.start());
    }
    int separator = field.lastIndexOf(SCHEMA_TREE_TYPE_SEPARATOR);
    if (separator < 0) {
      return null;
    }

    return new SchemaTreeLine(
        depth,
        field.substring(0, separator),
        field.substring(separator + SCHEMA_TREE_TYPE_SEPARATOR.length()),
        nullable);
  }

  /**
   * Appends the Hive type of the field at {@code index}, including the types of its nested lines,
   * and returns the index of the first line after them.
   */
  private static int appendHiveType(
      @Nonnull List<SchemaTreeLine> treeLines, int index, @Nonnull StringBuilder dataType) {
    SchemaTreeLine field = treeLines.get(index);
    if (!NESTED_TYPE_NAMES.contains(field.typeName)) {
      dataType.append(HIVE_TYPE_NAMES.getOrDefault(field.typeName, field.typeName));
      return index + 1;
    }

    dataType.append(field.typeName).append('<');
    int next = index + 1;
    while (next < treeLines.size() && treeLines.get(next).depth > field.depth) {
      if (next > index + 1) {
        dataType.append(',');
      }
      if (STRUCT_TYPE_NAME.equals(field.typeName)) {
        dataType.append(treeLines.get(next).name).append(':');
      }
      next = appendHiveType(treeLines, next, dataType);
    }
    dataType.append('>');
    return next;
  }

  /**
   * Parses the column types and comments of {@code DESCRIBE TABLE} rows, such as {@code
   * varchar(20)}, by column name. The columns end at the first row without a type, such as {@code #
   * Partition Information}, which starts a section that lists the partition columns again.
   */
  @Nonnull
  static ImmutableMap<String, DescribedColumn> parseDescribeTableColumns(
      @Nonnull Iterable<? extends List<String>> rows) {
    Map<String, DescribedColumn> columnsByName = new LinkedHashMap<>();
    for (List<String> row : rows) {
      String dataType = cellOrEmpty(row, 1);
      if (dataType.isEmpty()) {
        break;
      }

      columnsByName.putIfAbsent(
          cellOrEmpty(row, 0), new DescribedColumn(dataType, cellOrEmpty(row, 2)));
    }
    return ImmutableMap.copyOf(columnsByName);
  }

  /**
   * Returns the columns with the comments, and the {@code CHAR} and {@code VARCHAR} types, that
   * {@code DESCRIBE TABLE} reports. The {@code Schema} tree has no comments and reports {@code
   * CHAR} and {@code VARCHAR} types as {@code string}.
   *
   * <p>A described type replaces the type of a column only if both are equal once each {@code CHAR}
   * and {@code VARCHAR} type is replaced by {@code string}. So a column keeps its type if {@code
   * DESCRIBE TABLE} truncated the type, as it does for a struct with many fields, such as {@code
   * struct<f00:int,...,f24:int,... 5 more fields>}.
   */
  @Nonnull
  static ImmutableList<ParsedColumn> withDescribedColumns(
      @Nonnull List<ParsedColumn> columns, @Nonnull Map<String, DescribedColumn> describedColumns) {
    ImmutableList.Builder<ParsedColumn> builder = ImmutableList.builder();
    for (ParsedColumn column : columns) {
      DescribedColumn described = describedColumns.get(column.columnName);
      if (described == null) {
        builder.add(column);
        continue;
      }

      boolean differsOnlyInLengths =
          CHAR_VARCHAR_TYPE_PATTERN
              .matcher(described.dataType)
              .replaceAll(STRING_TYPE_NAME)
              .equals(column.dataType);
      builder.add(
          column.withDataTypeAndComment(
              differsOnlyInLengths ? described.dataType : column.dataType, described.comment));
    }
    return builder.build();
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
}
