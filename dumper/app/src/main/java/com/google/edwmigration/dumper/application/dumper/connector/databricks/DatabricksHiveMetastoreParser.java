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
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.quoteStringLiteral;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * Parses Databricks {@code SHOW TABLE EXTENDED}, {@code DESCRIBE TABLE}, and {@code DESCRIBE
 * FUNCTION EXTENDED} text outputs for {@code hive_metastore}.
 */
final class DatabricksHiveMetastoreParser {

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

  /** Matches the {@code string} type in a Hive type, such as in {@code array<string>}. */
  private static final Pattern STRING_TYPE_PATTERN = Pattern.compile("\\bstring\\b");

  /**
   * Matches a {@code CHAR} or {@code VARCHAR} type with its length, such as {@code varchar(20)}.
   */
  private static final Pattern CHAR_VARCHAR_TYPE_PATTERN =
      Pattern.compile("\\b(?:var)?char\\(\\d+\\)", Pattern.CASE_INSENSITIVE);

  /** The format of {@code java.util.Date#toString}, which Spark uses for {@code Created Time}. */
  private static final DateTimeFormatter CREATED_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);

  /**
   * Matches {@code Key: value}. {@code DOTALL} lets the value span lines, because {@code DESCRIBE
   * FUNCTION EXTENDED} returns a multi-line {@code Body} as one cell with embedded newlines.
   */
  private static final Pattern KEY_VALUE_LINE_PATTERN =
      Pattern.compile("^([A-Za-z][A-Za-z0-9 _-]*):(?:\\s*(.*))?$", Pattern.DOTALL);

  private static final String DEFAULT_KEYWORD = "DEFAULT";

  /** Characters that leave a SQL expression waiting for another operand, as in {@code 'a' ||}. */
  private static final String OPERATOR_CHARACTERS = "|+-*/%=<>!&^~,(:";

  /** Keywords that leave a SQL expression waiting for another operand, as in {@code x LIKE}. */
  private static final ImmutableSet<String> OPERAND_EXPECTING_KEYWORDS =
      ImmutableSet.of(
          "AND", "BETWEEN", "CASE", "ELSE", "ESCAPE", "ILIKE", "IN", "IS", "LIKE", "NOT", "OR",
          "REGEXP", "RLIKE", "THEN", "WHEN");

  /** The {@code Data Access} values that are {@code CREATE FUNCTION} characteristics. */
  private static final ImmutableSet<String> DATA_ACCESS_CHARACTERISTICS =
      ImmutableSet.of("CONTAINS SQL", "READS SQL DATA");

  private DatabricksHiveMetastoreParser() {}

  static final class ParsedColumn {
    final int ordinalPosition;
    final String columnName;
    final String dataType;
    final String isNullable;
    final String partitionIndex;

    ParsedColumn(
        int ordinalPosition,
        String columnName,
        String dataType,
        String isNullable,
        String partitionIndex) {
      this.ordinalPosition = ordinalPosition;
      this.columnName = columnName;
      this.dataType = dataType;
      this.isNullable = isNullable;
      this.partitionIndex = partitionIndex;
    }

    /** Returns this column with another data type. */
    @Nonnull
    ParsedColumn withDataType(@Nonnull String dataType) {
      return new ParsedColumn(ordinalPosition, columnName, dataType, isNullable, partitionIndex);
    }
  }

  static final class ParsedTableConstraint {
    final String constraintName;
    final String constraintType;
    final String constraintDetails;

    ParsedTableConstraint(String constraintName, String constraintType, String constraintDetails) {
      this.constraintName = constraintName;
      this.constraintType = constraintType;
      this.constraintDetails = constraintDetails;
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

  static final class ParsedTableExtended {
    final String catalogName;
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
    final String storageProperties;
    final String tableProperties;
    @CheckForNull final String viewDefinition;
    final ImmutableList<ParsedColumn> columns;
    final ImmutableList<ParsedTableConstraint> constraints;

    ParsedTableExtended(
        String catalogName,
        String schemaName,
        String tableName,
        String tableType,
        String dataSourceFormat,
        String storageLocation,
        String comment,
        String owner,
        String createdAt,
        String serdeLibrary,
        String inputFormat,
        String outputFormat,
        String storageProperties,
        String tableProperties,
        @CheckForNull String viewDefinition,
        ImmutableList<ParsedColumn> columns,
        ImmutableList<ParsedTableConstraint> constraints) {
      this.catalogName = catalogName;
      this.schemaName = schemaName;
      this.tableName = tableName;
      this.tableType = tableType;
      this.dataSourceFormat = dataSourceFormat;
      this.storageLocation = storageLocation;
      this.comment = comment;
      this.owner = owner;
      this.createdAt = createdAt;
      this.serdeLibrary = serdeLibrary;
      this.inputFormat = inputFormat;
      this.outputFormat = outputFormat;
      this.storageProperties = storageProperties;
      this.tableProperties = tableProperties;
      this.viewDefinition = viewDefinition;
      this.columns = columns;
      this.constraints = constraints;
    }
  }

  static final class ParsedFunctionExtended {
    final String functionCatalog;
    final String functionSchema;
    final String functionName;
    final String dataType;
    final String inputParams;
    final String routineDefinition;
    final String routineLanguage;
    final String comment;
    final String owner;
    final String ddl;

    ParsedFunctionExtended(
        String functionCatalog,
        String functionSchema,
        String functionName,
        String dataType,
        String inputParams,
        String routineDefinition,
        String routineLanguage,
        String comment,
        String owner,
        String ddl) {
      this.functionCatalog = functionCatalog;
      this.functionSchema = functionSchema;
      this.functionName = functionName;
      this.dataType = dataType;
      this.inputParams = inputParams;
      this.routineDefinition = routineDefinition;
      this.routineLanguage = routineLanguage;
      this.comment = comment;
      this.owner = owner;
      this.ddl = ddl;
    }
  }

  private static final String DELTA_CONSTRAINTS_PREFIX = "delta.constraints.";

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

  @Nonnull
  static ParsedTableExtended parseShowTableExtended(
      @Nonnull String defaultSchema,
      @Nonnull String defaultTable,
      @CheckForNull String informationBlob) {
    ImmutableMap<String, String> values =
        splitTableInformation(Strings.nullToEmpty(informationBlob));
    String tableProperties = values.getOrDefault("Table Properties", "");
    List<ParsedTableConstraint> constraints = new ArrayList<>();
    parseDeltaCheckConstraintsFromProperties(tableProperties, constraints);
    List<String> partitionCols =
        parsePartitionColumnsList(values.getOrDefault("Partition Columns", ""));

    return new ParsedTableExtended(
        valueOrDefault(values, "Catalog", DatabricksFilter.HIVE_METASTORE_CATALOG),
        valueOrDefault(values, "Database", defaultSchema),
        valueOrDefault(values, "Table", defaultTable),
        values.getOrDefault("Type", ""),
        values.getOrDefault("Provider", "").toUpperCase(Locale.ROOT),
        values.getOrDefault("Location", ""),
        values.getOrDefault("Comment", ""),
        values.getOrDefault("Owner", ""),
        formatCreatedTime(values.getOrDefault("Created Time", "")),
        values.getOrDefault("Serde Library", ""),
        values.getOrDefault("InputFormat", ""),
        values.getOrDefault("OutputFormat", ""),
        values.getOrDefault("Storage Properties", ""),
        tableProperties,
        values.get("View Text"),
        parseSchemaColumns(values.getOrDefault("Schema", ""), partitionCols),
        ImmutableList.copyOf(constraints));
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
   * Returns a {@code Created Time} value, such as {@code Sun Sep 27 21:19:25 UTC 2026}, as an ISO
   * 8601 instant, such as {@code 2026-09-27T21:19:25Z}, or unchanged if it has another format.
   */
  @Nonnull
  private static String formatCreatedTime(@Nonnull String createdTime) {
    try {
      return ZonedDateTime.parse(createdTime, CREATED_TIME_FORMATTER).toInstant().toString();
    } catch (DateTimeParseException e) {
      // The value is kept as Databricks printed it, so no information is lost.
      return createdTime;
    }
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
              pIdx != null ? String.valueOf(pIdx) : ""));
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
   * Returns whether the type of a column contains {@code string}, which the {@code Schema} tree
   * also reports for {@code CHAR} and {@code VARCHAR} types.
   */
  static boolean hasStringColumn(@Nonnull ParsedTableExtended table) {
    for (ParsedColumn column : table.columns) {
      if (STRING_TYPE_PATTERN.matcher(column.dataType).find()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Parses the column types of {@code DESCRIBE TABLE} rows, such as {@code varchar(20)}, by column
   * name. The columns end at the first row without a type, such as {@code # Partition Information},
   * which starts a section that lists the partition columns again.
   */
  @Nonnull
  static ImmutableMap<String, String> parseDescribeTableTypes(
      @Nonnull Iterable<? extends List<String>> rows) {
    Map<String, String> typesByColumnName = new LinkedHashMap<>();
    for (List<String> row : rows) {
      String dataType = cellOrEmpty(row, 1);
      if (dataType.isEmpty()) {
        break;
      }

      typesByColumnName.putIfAbsent(cellOrEmpty(row, 0), dataType);
    }
    return ImmutableMap.copyOf(typesByColumnName);
  }

  /**
   * Returns the columns with the {@code CHAR} and {@code VARCHAR} types that {@code DESCRIBE TABLE}
   * reports, which the {@code Schema} tree reports as {@code string}.
   *
   * <p>A described type replaces the type of a column only if both are equal once each {@code CHAR}
   * and {@code VARCHAR} type is replaced by {@code string}. So a column keeps its type if {@code
   * DESCRIBE TABLE} truncated the type, as it does for a struct with many fields, such as {@code
   * struct<f00:int,...,f24:int,... 5 more fields>}.
   */
  @Nonnull
  static ImmutableList<ParsedColumn> withCharVarcharLengths(
      @Nonnull List<ParsedColumn> columns, @Nonnull Map<String, String> describedTypes) {
    ImmutableList.Builder<ParsedColumn> builder = ImmutableList.builder();
    for (ParsedColumn column : columns) {
      String describedType = describedTypes.get(column.columnName);
      boolean differsOnlyInLengths =
          describedType != null
              && CHAR_VARCHAR_TYPE_PATTERN
                  .matcher(describedType)
                  .replaceAll(STRING_TYPE_NAME)
                  .equals(column.dataType);
      builder.add(differsOnlyInLengths ? column.withDataType(describedType) : column);
    }
    return builder.build();
  }

  private static void parseDeltaCheckConstraintsFromProperties(
      String rawProps, List<ParsedTableConstraint> out) {
    String stripped = rawProps.trim();
    if (stripped.startsWith("[") && stripped.endsWith("]")) {
      stripped = stripped.substring(1, stripped.length() - 1);
    }
    if (stripped.isEmpty()) {
      return;
    }
    for (String entry : splitTopLevelEntries(stripped)) {
      int eqIdx = entry.indexOf('=');
      if (eqIdx <= 0) {
        continue;
      }
      String propKey = entry.substring(0, eqIdx).trim();
      String propVal = entry.substring(eqIdx + 1).trim();
      if (propKey.toLowerCase(Locale.ROOT).startsWith(DELTA_CONSTRAINTS_PREFIX)) {
        String constraintName = propKey.substring(DELTA_CONSTRAINTS_PREFIX.length()).trim();
        if (!constraintName.isEmpty()) {
          out.add(new ParsedTableConstraint(constraintName, "CHECK", propVal));
        }
      }
    }
  }

  private static List<String> splitTopLevelEntries(String input) {
    List<String> entries = new ArrayList<>();
    int parenDepth = 0;
    boolean inSingleQuotes = false;
    boolean inDoubleQuotes = false;
    int start = 0;
    for (int i = 0; i < input.length(); i++) {
      char c = input.charAt(i);
      if (c == '\'' && !inDoubleQuotes) {
        inSingleQuotes = !inSingleQuotes;
      } else if (c == '"' && !inSingleQuotes) {
        inDoubleQuotes = !inDoubleQuotes;
      } else if (!inSingleQuotes && !inDoubleQuotes) {
        if (c == '(' || c == '[') {
          parenDepth++;
        } else if (c == ')' || c == ']') {
          if (parenDepth > 0) {
            parenDepth--;
          }
        } else if (c == ',' && parenDepth == 0) {
          entries.add(input.substring(start, i).trim());
          start = i + 1;
        }
      }
    }
    if (start <= input.length()) {
      String tail = input.substring(start).trim();
      if (!tail.isEmpty()) {
        entries.add(tail);
      }
    }
    return entries;
  }

  private static List<String> parsePartitionColumnsList(String raw) {
    List<String> result = new ArrayList<>();
    String stripped = raw.trim();
    if (stripped.startsWith("[") && stripped.endsWith("]")) {
      stripped = stripped.substring(1, stripped.length() - 1);
    }
    if (stripped.trim().isEmpty()) {
      return result;
    }
    String[] parts = stripped.split(",");
    for (String part : parts) {
      String cleaned = part.trim();
      if (cleaned.startsWith("`") && cleaned.endsWith("`") && cleaned.length() >= 2) {
        cleaned = cleaned.substring(1, cleaned.length() - 1);
      }
      if (!cleaned.isEmpty()) {
        result.add(cleaned);
      }
    }
    return result;
  }

  /**
   * Parses {@code DESCRIBE FUNCTION EXTENDED} output and synthesizes a {@code CREATE FUNCTION}
   * statement, because Databricks has no {@code SHOW CREATE FUNCTION}.
   *
   * <p>The statement states the determinism and SQL data access that Databricks reports, because
   * both are otherwise derived from the body and a declared {@code DETERMINISTIC} may differ from
   * the derived value.
   */
  @Nonnull
  static ParsedFunctionExtended parseDescribeFunctionExtended(
      @Nonnull String catalogName,
      @Nonnull String schemaName,
      @Nonnull String functionName,
      @Nonnull List<String> lines) {
    String funcType = "";
    List<String> inputRows = new ArrayList<>();
    List<String> returnRows = new ArrayList<>();
    String body = null;
    String className = null;
    String comment = "";
    String owner = "";
    String deterministic = "";
    String dataAccess = "";

    int i = 0;
    while (i < lines.size()) {
      String line = lines.get(i);
      if (line == null) {
        i++;
        continue;
      }
      Matcher kv = KEY_VALUE_LINE_PATTERN.matcher(line);
      if (kv.matches()) {
        String key = kv.group(1).trim();
        String val = kv.group(2) != null ? kv.group(2).trim() : "";
        if ("Type".equalsIgnoreCase(key)) {
          funcType = val;
        } else if ("Input".equalsIgnoreCase(key)) {
          i = collectSectionRows(lines, i, inputRows);
        } else if ("Returns".equalsIgnoreCase(key)) {
          i = collectSectionRows(lines, i, returnRows);
        } else if ("Comment".equalsIgnoreCase(key)) {
          comment = val;
        } else if ("Owner".equalsIgnoreCase(key)) {
          owner = val;
        } else if ("Deterministic".equalsIgnoreCase(key)) {
          deterministic = val;
        } else if ("Data Access".equalsIgnoreCase(key)) {
          dataAccess = val.replaceAll("\\s+", " ").toUpperCase(Locale.ROOT);
        } else if ("Class".equalsIgnoreCase(key)) {
          className = val;
        } else if ("Body".equalsIgnoreCase(key)) {
          StringBuilder bodySb = new StringBuilder(val);
          while (i + 1 < lines.size()) {
            String next = lines.get(i + 1);
            if (next == null || KEY_VALUE_LINE_PATTERN.matcher(next).matches()) {
              break;
            }
            bodySb.append("\n").append(next);
            i++;
          }
          body = bodySb.toString();
        }
      }
      i++;
    }

    String inputParams = String.join(", ", parseSectionRows(inputRows));
    String dataType;
    if (returnRows.isEmpty()) {
      dataType = "";
    } else if ("TABLE".equalsIgnoreCase(funcType)) {
      dataType = "TABLE(" + String.join(", ", parseSectionRows(returnRows)) + ")";
    } else {
      dataType = String.join(" ", trimmedSectionRows(returnRows));
    }

    String qualifiedName =
        quoteIdentifier(catalogName)
            + "."
            + quoteIdentifier(schemaName)
            + "."
            + quoteIdentifier(functionName);
    String routineDef = "";
    String routineLang = "";
    String ddl = "";
    if (body != null && !body.isEmpty()) {
      routineDef = body;
      routineLang = "SQL";
      StringBuilder ddlSb = new StringBuilder();
      ddlSb.append("CREATE FUNCTION ").append(qualifiedName).append("(").append(inputParams);
      ddlSb.append(")");
      if (!dataType.isEmpty()) {
        ddlSb.append("\nRETURNS ").append(dataType);
      }
      if ("true".equalsIgnoreCase(deterministic)) {
        ddlSb.append("\nDETERMINISTIC");
      } else if ("false".equalsIgnoreCase(deterministic)) {
        ddlSb.append("\nNOT DETERMINISTIC");
      }
      if (DATA_ACCESS_CHARACTERISTICS.contains(dataAccess)) {
        ddlSb.append("\n").append(dataAccess);
      }
      if (!comment.isEmpty()) {
        ddlSb.append("\nCOMMENT ").append(quoteStringLiteral(comment));
      }
      ddlSb.append("\nRETURN ").append(body);
      ddl = ddlSb.toString();
    } else if (className != null && !className.isEmpty()) {
      routineDef = className;
      routineLang = "JAVA";
      ddl = "CREATE FUNCTION " + qualifiedName + " AS " + quoteStringLiteral(className);
    }

    return new ParsedFunctionExtended(
        catalogName,
        schemaName,
        functionName,
        dataType,
        inputParams,
        routineDef,
        routineLang,
        comment,
        owner,
        ddl);
  }

  /**
   * Adds the section line at {@code index} and its continuation lines to {@code rows}.
   *
   * @return the index of the last added line
   */
  private static int collectSectionRows(
      @Nonnull List<String> lines, int index, @Nonnull List<String> rows) {
    rows.add(lines.get(index));
    int last = index;
    while (last + 1 < lines.size()) {
      String next = lines.get(last + 1);
      if (next == null || KEY_VALUE_LINE_PATTERN.matcher(next).matches()) {
        break;
      }
      rows.add(next);
      last++;
    }
    return last;
  }

  /**
   * Replaces the {@code Key:} label of the first section row with spaces, so that all rows keep
   * their column offsets, and drops blank rows.
   */
  @Nonnull
  private static ImmutableList<String> alignSectionRows(@Nonnull List<String> rows) {
    ImmutableList.Builder<String> aligned = ImmutableList.builder();
    for (int index = 0; index < rows.size(); index++) {
      String row = rows.get(index);
      if (index == 0) {
        int labelEnd = row.indexOf(':') + 1;
        row = Strings.repeat(" ", labelEnd) + row.substring(labelEnd);
      }
      if (!row.trim().isEmpty()) {
        aligned.add(row);
      }
    }
    return aligned.build();
  }

  @Nonnull
  private static ImmutableList<String> trimmedSectionRows(@Nonnull List<String> rows) {
    ImmutableList.Builder<String> trimmed = ImmutableList.builder();
    for (String row : alignSectionRows(rows)) {
      trimmed.add(row.trim());
    }
    return trimmed.build();
  }

  /**
   * Parses the {@code Input} section, or the {@code Returns} section of a TABLE function, into SQL
   * definitions such as {@code `x` INT DEFAULT 1 COMMENT 'the input'}.
   *
   * <p>Each row reads {@code name type [DEFAULT expression] ['comment']}, with the name and type
   * columns padded to their widest value. Names and comments are printed without escaping, so a
   * name ends where the type column starts in every row, and the comment is the quoted text at the
   * end of a row. A name that contains a space is only recognized in a section with several rows.
   */
  @Nonnull
  private static ImmutableList<String> parseSectionRows(@Nonnull List<String> rows) {
    ImmutableList<String> aligned = alignSectionRows(rows);
    if (aligned.isEmpty() || (aligned.size() == 1 && aligned.get(0).trim().equals("()"))) {
      return ImmutableList.of();
    }

    int nameStart = Integer.MAX_VALUE;
    for (String row : aligned) {
      nameStart = Math.min(nameStart, indexOfNonWhitespace(row, 0));
    }
    int typeStart = findTypeColumn(aligned, nameStart);
    ImmutableList.Builder<String> definitions = ImmutableList.builder();
    for (String row : aligned) {
      String name = typeStart < 0 ? "" : row.substring(nameStart, typeStart).trim();
      if (name.isEmpty()) {
        definitions.add(row.trim());
      } else {
        definitions.add(
            quoteIdentifier(name)
                + " "
                + formatTypeDefaultAndComment(row.substring(typeStart).trim()));
      }
    }
    return definitions.build();
  }

  /**
   * Returns the first offset after {@code nameStart} where every row has a value that follows a
   * space, which is where the padded name column ends, or -1 if there is no such offset.
   */
  private static int findTypeColumn(@Nonnull List<String> rows, int nameStart) {
    int shortestRowLength = Integer.MAX_VALUE;
    for (String row : rows) {
      shortestRowLength = Math.min(shortestRowLength, row.length());
    }

    for (int offset = nameStart + 2; offset < shortestRowLength; offset++) {
      boolean valueStartsInEveryRow = true;
      for (String row : rows) {
        if (!Character.isWhitespace(row.charAt(offset - 1))
            || Character.isWhitespace(row.charAt(offset))) {
          valueStartsInEveryRow = false;
          break;
        }
      }

      if (valueStartsInEveryRow) {
        return offset;
      }
    }
    return -1;
  }

  /**
   * Converts {@code type [DEFAULT expression] ['comment']} into {@code type [DEFAULT expression]
   * [COMMENT 'comment']} with an escaped comment.
   */
  @Nonnull
  private static String formatTypeDefaultAndComment(@Nonnull String value) {
    int typeEnd = findTypeEnd(value);
    String type = value.substring(0, typeEnd).trim();
    String rest = value.substring(typeEnd).trim();
    String defaultExpression = null;
    String comment = null;
    if (startsWithKeyword(rest, DEFAULT_KEYWORD)) {
      String expression = rest.substring(DEFAULT_KEYWORD.length()).trim();
      int commentStart = findTrailingComment(expression);
      if (commentStart < 0) {
        defaultExpression = expression;
      } else {
        defaultExpression = expression.substring(0, commentStart).trim();
        comment = expression.substring(commentStart + 1, expression.length() - 1);
      }
    } else if (isQuotedText(rest)) {
      comment = rest.substring(1, rest.length() - 1);
    } else if (!rest.isEmpty()) {
      return value;
    }

    StringBuilder definition = new StringBuilder(type);
    if (defaultExpression != null) {
      definition.append(" ").append(DEFAULT_KEYWORD).append(" ").append(defaultExpression);
    }
    if (comment != null) {
      definition.append(" COMMENT ").append(quoteStringLiteral(comment));
    }
    return definition.toString();
  }

  /**
   * Returns the offset of the whitespace that ends the type, where {@code DEFAULT} or the quoted
   * comment follows, or the length of {@code value} if it holds only a type. A type may contain
   * spaces, as in {@code INTERVAL DAY TO SECOND} and {@code MAP<STRING, INT>}.
   */
  private static int findTypeEnd(@Nonnull String value) {
    int depth = 0;
    int index = 0;
    while (index < value.length()) {
      char c = value.charAt(index);
      if (c == '\'' || c == '"' || c == '`') {
        index = skipQuoted(value, index);
        continue;
      }

      if (c == '<' || c == '(') {
        depth++;
      } else if ((c == '>' || c == ')') && depth > 0) {
        depth--;
      } else if (depth == 0 && Character.isWhitespace(c)) {
        String next = value.substring(indexOfNonWhitespace(value, index));
        if (next.startsWith("'") || startsWithKeyword(next, DEFAULT_KEYWORD)) {
          return index;
        }
      }
      index++;
    }
    return value.length();
  }

  /**
   * Returns the offset of the quote that opens the comment at the end of a default {@code
   * expression}, or -1 if there is no comment.
   *
   * <p>The expression is SQL, but the comment is printed without escaping, so the comment is taken
   * to be the first quoted text at the top level that follows a complete expression, and the text
   * after its opening quote is not scanned. An expression like {@code 'a' || 'b'} ends with a
   * string literal but has no comment.
   */
  private static int findTrailingComment(@Nonnull String expression) {
    int length = expression.length();
    if (length < 2 || expression.charAt(length - 1) != '\'') {
      return -1;
    }

    int depth = 0;
    int index = 0;
    while (index < length - 1) {
      char c = expression.charAt(index);
      if (c == '\''
          && depth == 0
          && index > 0
          && Character.isWhitespace(expression.charAt(index - 1))
          && isCompleteExpression(expression.substring(0, index))) {
        return index;
      }

      if (c == '\'' || c == '"' || c == '`') {
        index = skipQuoted(expression, index);
        continue;
      }

      if (c == '(') {
        depth++;
      } else if (c == ')' && depth > 0) {
        depth--;
      }
      index++;
    }
    return -1;
  }

  /** Returns whether {@code prefix} is a SQL expression that does not wait for another operand. */
  private static boolean isCompleteExpression(@Nonnull String prefix) {
    String trimmed = prefix.trim();
    if (trimmed.isEmpty()) {
      return false;
    }

    if (OPERATOR_CHARACTERS.indexOf(trimmed.charAt(trimmed.length() - 1)) >= 0) {
      return false;
    }

    int wordStart = trimmed.length();
    while (wordStart > 0 && Character.isLetter(trimmed.charAt(wordStart - 1))) {
      wordStart--;
    }
    boolean isWord = wordStart == 0 || !isIdentifierCharacter(trimmed.charAt(wordStart - 1));
    String lastWord = trimmed.substring(wordStart).toUpperCase(Locale.ROOT);
    return !(isWord && OPERAND_EXPECTING_KEYWORDS.contains(lastWord));
  }

  private static boolean isIdentifierCharacter(char c) {
    return Character.isLetterOrDigit(c) || c == '_';
  }

  private static boolean isQuotedText(@Nonnull String text) {
    return text.length() >= 2 && text.charAt(0) == '\'' && text.charAt(text.length() - 1) == '\'';
  }

  private static boolean startsWithKeyword(@Nonnull String text, @Nonnull String keyword) {
    return text.regionMatches(true, 0, keyword, 0, keyword.length())
        && (text.length() == keyword.length()
            || Character.isWhitespace(text.charAt(keyword.length())));
  }

  private static int indexOfNonWhitespace(@Nonnull String text, int from) {
    int index = from;
    while (index < text.length() && Character.isWhitespace(text.charAt(index))) {
      index++;
    }
    return index;
  }

  /**
   * Returns the offset after the quoted text that starts at {@code start}, or the length of {@code
   * text} if the quote is not closed. Backslash escapes and doubled quotes stay inside the text.
   */
  private static int skipQuoted(@Nonnull String text, int start) {
    char quote = text.charAt(start);
    int index = start + 1;
    while (index < text.length()) {
      char c = text.charAt(index);
      if (c == '\\' && quote != '`') {
        index += 2;
      } else if (c != quote) {
        index++;
      } else if (index + 1 < text.length() && text.charAt(index + 1) == quote) {
        index += 2;
      } else {
        return index + 1;
      }
    }
    return text.length();
  }
}
