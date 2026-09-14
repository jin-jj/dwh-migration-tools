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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One table as described by the {@code information} column of {@code SHOW TABLE EXTENDED}.
 *
 * <p>The legacy {@code hive_metastore} catalog has no {@code information_schema}, so the only way
 * to read its metadata over SQL is to parse this text blob. {@code SHOW TABLE EXTENDED IN <schema>
 * LIKE '*'} returns one blob per table, which means the whole schema costs a single query. The
 * obvious alternative, {@code DESCRIBE TABLE} per table, costs one query per table and still omits
 * the storage location, the table type and the owner.
 *
 * <p>The blob looks like this:
 *
 * <pre>
 * Database: sales
 * Table: orders
 * Owner: alice@example.com
 * Created Time: Thu Jan 01 00:00:00 UTC 2025
 * Type: EXTERNAL
 * Provider: delta
 * Location: s3://bucket/orders
 * Partition Columns: [`country`]
 * Schema: root
 *  |-- id: long (nullable = false)
 *  |-- country: string (nullable = true)
 * </pre>
 */
final class DatabricksHiveMetastoreTable {

  private static final Logger logger = LoggerFactory.getLogger(DatabricksHiveMetastoreTable.class);

  /**
   * Field labels Spark emits. A line only starts a new field if its label is one of these, which is
   * what lets multi-line values such as {@code View Text} and {@code Schema} be reassembled.
   */
  private static final ImmutableSet<String> FIELD_LABELS =
      ImmutableSet.of(
          "Catalog",
          "Database",
          "Table",
          "Owner",
          "Created Time",
          "Last Access",
          "Created By",
          "Type",
          "Provider",
          "Comment",
          "Collation",
          "Table Properties",
          "Statistics",
          "Location",
          "Serde Library",
          "InputFormat",
          "OutputFormat",
          "Storage Properties",
          "Num Buckets",
          "Bucket Columns",
          "Sort Columns",
          "Partition Provider",
          "Partition Columns",
          "Schema",
          "View Text",
          "View Original Text",
          "View Catalog and Namespace",
          "View Query Output Columns",
          "View Schema Mode",
          "Ignored Properties",
          "Time Travel");

  private static final String CREATED_TIME_PATTERN = "EEE MMM dd HH:mm:ss zzz yyyy";

  /** Spark prints Catalyst type names in the schema tree; the outputs use SQL names. */
  private static final ImmutableMap<String, String> SQL_TYPE_NAMES =
      ImmutableMap.<String, String>builder()
          .put("byte", "TINYINT")
          .put("short", "SMALLINT")
          .put("integer", "INT")
          .put("long", "BIGINT")
          .put("float", "FLOAT")
          .put("double", "DOUBLE")
          .put("string", "STRING")
          .put("binary", "BINARY")
          .put("boolean", "BOOLEAN")
          .put("date", "DATE")
          .put("timestamp", "TIMESTAMP")
          .put("timestamp_ntz", "TIMESTAMP_NTZ")
          .put("void", "VOID")
          .build();

  /** One top-level column. Nested struct fields are folded into the parent's data type. */
  static final class Column {

    private final String name;
    private final String dataType;
    private final boolean nullable;

    Column(@Nonnull String name, @Nonnull String dataType, boolean nullable) {
      this.name = name;
      this.dataType = dataType;
      this.nullable = nullable;
    }

    @Nonnull
    String name() {
      return name;
    }

    @Nonnull
    String dataType() {
      return dataType;
    }

    boolean nullable() {
      return nullable;
    }

    @Override
    public String toString() {
      return name + ":" + dataType + (nullable ? "" : " NOT NULL");
    }
  }

  private final Map<String, String> fields;
  private final ImmutableList<Column> columns;
  private final ImmutableList<String> partitionColumns;

  private DatabricksHiveMetastoreTable(
      Map<String, String> fields,
      ImmutableList<Column> columns,
      ImmutableList<String> partitionColumns) {
    this.fields = fields;
    this.columns = columns;
    this.partitionColumns = partitionColumns;
  }

  /** Parses one {@code information} blob. Never fails: unrecognised content is simply absent. */
  @Nonnull
  static DatabricksHiveMetastoreTable parse(@Nonnull String information) {
    Preconditions.checkNotNull(information, "Information blob was null.");
    Map<String, String> fields = parseFields(information);
    return new DatabricksHiveMetastoreTable(
        fields,
        parseSchema(fields.get("Schema")),
        parseBracketedList(fields.get("Partition Columns")));
  }

  @CheckForNull
  String database() {
    return fields.get("Database");
  }

  @CheckForNull
  String name() {
    return fields.get("Table");
  }

  @CheckForNull
  String owner() {
    return fields.get("Owner");
  }

  @CheckForNull
  String comment() {
    return fields.get("Comment");
  }

  /** {@code MANAGED}, {@code EXTERNAL} or {@code VIEW}. */
  @CheckForNull
  String type() {
    return fields.get("Type");
  }

  /** The storage format, for example {@code delta} or {@code parquet}. */
  @CheckForNull
  String provider() {
    return fields.get("Provider");
  }

  @CheckForNull
  String location() {
    return fields.get("Location");
  }

  /** The view body, or {@code null} for tables. */
  @CheckForNull
  String viewText() {
    String viewText = fields.get("View Text");
    return viewText != null ? viewText : fields.get("View Original Text");
  }

  boolean isView() {
    return "VIEW".equalsIgnoreCase(type());
  }

  /** Creation time as epoch milliseconds, matching what the Unity Catalog outputs carry. */
  @CheckForNull
  Long createdAtMillis() {
    String createdTime = fields.get("Created Time");
    if (createdTime == null || createdTime.isEmpty()) {
      return null;
    }
    try {
      return new SimpleDateFormat(CREATED_TIME_PATTERN, Locale.US).parse(createdTime).getTime();
    } catch (ParseException e) {
      logger.debug("Unparseable 'Created Time' value '{}'.", createdTime);
      return null;
    }
  }

  @Nonnull
  ImmutableList<Column> columns() {
    return columns;
  }

  /**
   * Returns the 1-based position of {@code columnName} among the partition columns, or {@code null}
   * if it does not partition the table.
   */
  @CheckForNull
  Integer partitionIndexOf(@Nonnull String columnName) {
    int index = partitionColumns.indexOf(columnName);
    return index < 0 ? null : index + 1;
  }

  private static Map<String, String> parseFields(String information) {
    Map<String, String> fields = new LinkedHashMap<>();
    StringBuilder value = new StringBuilder();
    String label = null;
    for (String line : information.split("\n", -1)) {
      String nextLabel = labelOf(line);
      if (nextLabel == null) {
        if (label != null) {
          value.append('\n').append(line);
        }
        continue;
      }
      if (label != null) {
        fields.put(label, value.toString());
      }
      label = nextLabel;
      value.setLength(0);
      value.append(line.substring(nextLabel.length() + 1).trim());
    }
    if (label != null) {
      fields.put(label, value.toString());
    }
    return fields;
  }

  /** Returns the field label this line starts, or {@code null} if it continues the previous one. */
  @CheckForNull
  private static String labelOf(String line) {
    int colon = line.indexOf(':');
    if (colon <= 0) {
      return null;
    }
    String candidate = line.substring(0, colon);
    return FIELD_LABELS.contains(candidate) ? candidate : null;
  }

  /** Parses {@code [`a`, `b`]} into {@code [a, b]}. */
  private static ImmutableList<String> parseBracketedList(@Nullable String value) {
    if (value == null) {
      return ImmutableList.of();
    }
    String trimmed = value.trim();
    if (trimmed.length() < 2 || !trimmed.startsWith("[") || !trimmed.endsWith("]")) {
      return ImmutableList.of();
    }
    String body = trimmed.substring(1, trimmed.length() - 1).trim();
    if (body.isEmpty()) {
      return ImmutableList.of();
    }
    List<String> names = new ArrayList<>();
    for (String element : body.split(",")) {
      String name = element.trim();
      if (name.startsWith("`") && name.endsWith("`") && name.length() >= 2) {
        name = name.substring(1, name.length() - 1);
      }
      if (!name.isEmpty()) {
        names.add(name);
      }
    }
    return ImmutableList.copyOf(names);
  }

  /**
   * Parses the {@code Schema: root} tree into top-level columns.
   *
   * <p>Nested fields are folded back into a single SQL type string, so {@code struct} columns keep
   * their field list instead of degrading to the bare word {@code struct}.
   */
  private static ImmutableList<Column> parseSchema(@Nullable String schema) {
    if (schema == null) {
      return ImmutableList.of();
    }
    List<SchemaNode> nodes = new ArrayList<>();
    for (String line : schema.split("\n", -1)) {
      SchemaNode node = SchemaNode.parse(line);
      if (node != null) {
        nodes.add(node);
      }
    }
    List<Column> columns = new ArrayList<>();
    for (int i = 0; i < nodes.size(); i++) {
      SchemaNode node = nodes.get(i);
      if (node.depth != 1) {
        continue;
      }
      columns.add(new Column(node.name, renderType(nodes, i), node.nullable));
    }
    return ImmutableList.copyOf(columns);
  }

  /** Renders the type of {@code nodes[index]}, recursing into the children that follow it. */
  private static String renderType(List<SchemaNode> nodes, int index) {
    SchemaNode node = nodes.get(index);
    List<Integer> children = new ArrayList<>();
    for (int i = index + 1; i < nodes.size() && nodes.get(i).depth > node.depth; i++) {
      if (nodes.get(i).depth == node.depth + 1) {
        children.add(i);
      }
    }
    if (children.isEmpty()) {
      return sqlTypeName(node.type);
    }
    if ("array".equals(node.type)) {
      return "ARRAY<" + renderType(nodes, children.get(0)) + ">";
    }
    if ("map".equals(node.type)) {
      String keyType = renderType(nodes, children.get(0));
      String valueType = children.size() > 1 ? renderType(nodes, children.get(1)) : "STRING";
      return "MAP<" + keyType + ", " + valueType + ">";
    }
    StringBuilder rendered = new StringBuilder("STRUCT<");
    for (int i = 0; i < children.size(); i++) {
      if (i > 0) {
        rendered.append(", ");
      }
      SchemaNode child = nodes.get(children.get(i));
      rendered.append(child.name).append(": ").append(renderType(nodes, children.get(i)));
    }
    return rendered.append('>').toString();
  }

  private static String sqlTypeName(String catalystType) {
    String sqlName = SQL_TYPE_NAMES.get(catalystType);
    if (sqlName != null) {
      return sqlName;
    }
    // Parameterised types such as decimal(10,2) are already in SQL form apart from their case.
    int parenthesis = catalystType.indexOf('(');
    if (parenthesis > 0) {
      String base = catalystType.substring(0, parenthesis);
      return base.toUpperCase(Locale.ROOT) + catalystType.substring(parenthesis);
    }
    return catalystType.toUpperCase(Locale.ROOT);
  }

  /** One {@code |-- name: type (nullable = x)} line of the schema tree. */
  private static final class SchemaNode {

    /** Each extra level of nesting indents the tree marker by this many characters. */
    private static final int INDENT_WIDTH = 5;

    private static final String MARKER = "|-- ";

    private final int depth;
    private final String name;
    private final String type;
    private final boolean nullable;

    private SchemaNode(int depth, String name, String type, boolean nullable) {
      this.depth = depth;
      this.name = name;
      this.type = type;
      this.nullable = nullable;
    }

    @CheckForNull
    static SchemaNode parse(String line) {
      int marker = line.indexOf(MARKER);
      if (marker < 1) {
        return null;
      }
      int depth = (marker - 1) / INDENT_WIDTH + 1;
      String body = line.substring(marker + MARKER.length());
      int colon = body.indexOf(':');
      if (colon < 0) {
        return null;
      }
      String name = body.substring(0, colon).trim();
      String remainder = body.substring(colon + 1).trim();

      // Attributes are printed as a trailing parenthesised clause, for example
      // "(nullable = true)", "(containsNull = true)" or "(valueContainsNull = true)". A
      // parameterised type such as decimal(10,2) has no space before its parenthesis, so looking
      // for " (" distinguishes the two.
      String type = remainder;
      boolean nullable = true;
      int attributes = remainder.lastIndexOf(" (");
      if (attributes > 0 && remainder.endsWith(")")) {
        type = remainder.substring(0, attributes).trim();
        nullable = remainder.substring(attributes).contains("= true");
      }
      if (name.isEmpty() || type.isEmpty()) {
        return null;
      }
      return new SchemaNode(depth, name, type, nullable);
    }
  }
}
