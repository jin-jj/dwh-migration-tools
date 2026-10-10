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

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksJson.toJsonOrEmpty;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.quoteIdentifierIfNeeded;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.quoteStringLiteral;

import com.databricks.sdk.service.catalog.ColumnInfo;
import com.databricks.sdk.service.catalog.ColumnMask;
import com.databricks.sdk.service.catalog.ForeignKeyConstraint;
import com.databricks.sdk.service.catalog.FunctionInfo;
import com.databricks.sdk.service.catalog.FunctionInfoRoutineBody;
import com.databricks.sdk.service.catalog.FunctionInfoSqlDataAccess;
import com.databricks.sdk.service.catalog.FunctionParameterInfo;
import com.databricks.sdk.service.catalog.FunctionParameterInfos;
import com.databricks.sdk.service.catalog.PolicyFunctionArgument;
import com.databricks.sdk.service.catalog.PrimaryKeyConstraint;
import com.databricks.sdk.service.catalog.TableRowFilter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * Converts the Unity Catalog metadata of the REST API to the values of the CSV columns of {@link
 * DatabricksMetadataDumpFormat}, in the same form as the values of {@code hive_metastore}.
 */
final class DatabricksUnityCatalogFormatter {

  /** The keys of the column metadata in {@code type_json} that hold the clauses of a column. */
  private static final String CURRENT_DEFAULT_KEY = "CURRENT_DEFAULT";

  private static final String GENERATION_EXPRESSION_KEY = "delta.generationExpression";
  private static final String IDENTITY_START_KEY = "delta.identity.start";
  private static final String IDENTITY_STEP_KEY = "delta.identity.step";
  private static final String IDENTITY_ALLOW_EXPLICIT_INSERT_KEY =
      "delta.identity.allowExplicitInsert";

  private static final String IDENTITY_ALWAYS = "ALWAYS";
  private static final String IDENTITY_BY_DEFAULT = "BY DEFAULT";

  /** The view properties in which Spark stores the context of the view text. */
  private static final String VIEW_CATALOG_AND_NAMESPACE_PARTS_KEY =
      "view.catalogAndNamespace.numParts";

  private static final String VIEW_CATALOG_AND_NAMESPACE_PART_KEY_PREFIX =
      "view.catalogAndNamespace.part.";
  private static final String VIEW_QUERY_OUTPUT_COLUMNS_KEY = "view.query.out.numCols";
  private static final String VIEW_QUERY_OUTPUT_COLUMN_KEY_PREFIX = "view.query.out.col.";
  private static final String VIEW_SCHEMA_MODE_KEY = "view.schemaMode";

  private static final String SQL_LANGUAGE = "SQL";

  private static final ObjectReader TYPE_JSON_READER = new ObjectMapper().reader();

  /** The metadata key of the comment of a struct field in {@code type_json}. */
  private static final String COMMENT_KEY = "comment";

  /**
   * The metadata key of the collations of the strings in the type of a field in {@code type_json},
   * by their path in the field, such as {@code tags.element} for {@code array<string collate
   * UTF8_LCASE>}.
   */
  private static final String COLLATIONS_KEY = "__COLLATIONS";

  /** The collation that a string has unless another is given, which types don't write. */
  private static final String DEFAULT_COLLATION = "UTF8_BINARY";

  /** The names of the {@code type_json} types that {@code type_text} writes differently. */
  private static final ImmutableMap<String, String> TYPE_TEXT_NAMES =
      ImmutableMap.of("integer", "int", "long", "bigint", "short", "smallint", "byte", "tinyint");

  private DatabricksUnityCatalogFormatter() {}

  /**
   * The {@code DEFAULT}, {@code GENERATED ALWAYS AS} and {@code GENERATED ... AS IDENTITY} clauses
   * of a column. A clause that the column doesn't have is empty.
   */
  static final class ColumnClauses {

    final String defaultExpression;
    final String generationExpression;

    /** {@code ALWAYS} or {@code BY DEFAULT}. */
    final String identityGeneration;

    final String identityStart;
    final String identityIncrement;

    private ColumnClauses(
        @Nonnull String defaultExpression,
        @Nonnull String generationExpression,
        @Nonnull String identityGeneration,
        @Nonnull String identityStart,
        @Nonnull String identityIncrement) {
      this.defaultExpression = defaultExpression;
      this.generationExpression = generationExpression;
      this.identityGeneration = identityGeneration;
      this.identityStart = identityStart;
      this.identityIncrement = identityIncrement;
    }
  }

  /**
   * Returns epoch milliseconds as an ISO 8601 instant, such as {@code 2026-09-27T21:19:25.123Z}, or
   * an empty string if they are null.
   */
  @Nonnull
  static String formatTime(@CheckForNull Long epochMillis) {
    return epochMillis == null ? "" : Instant.ofEpochMilli(epochMillis).toString();
  }

  /** Returns the name of an enum value, such as {@code MANAGED}, or an empty string if null. */
  @Nonnull
  static String formatEnum(@CheckForNull Enum<?> value) {
    return value == null ? "" : value.name();
  }

  @Nonnull
  static String formatNumber(@CheckForNull Long value) {
    return value == null ? "" : String.valueOf(value);
  }

  /** Returns {@code true}, {@code false}, or an empty string if the value is null. */
  @Nonnull
  static String formatBoolean(@CheckForNull Boolean value) {
    return value == null ? "" : String.valueOf(value);
  }

  /** Returns {@code NO} for a column that isn't nullable and {@code YES} otherwise. */
  @Nonnull
  static String formatNullable(@CheckForNull Boolean nullable) {
    return Boolean.FALSE.equals(nullable) ? "NO" : "YES";
  }

  /** Returns the SQL data access as in {@code CREATE FUNCTION}, such as {@code READS SQL DATA}. */
  @Nonnull
  static String formatSqlDataAccess(@CheckForNull FunctionInfoSqlDataAccess sqlDataAccess) {
    return sqlDataAccess == null ? "" : sqlDataAccess.name().replace('_', ' ');
  }

  /** Returns the columns in the order of their positions; columns without a position go last. */
  @Nonnull
  static ImmutableList<ColumnInfo> sortColumns(@CheckForNull Collection<ColumnInfo> columns) {
    return sortByPosition(columns, ColumnInfo::getPosition);
  }

  /**
   * Returns the clauses of a column from the metadata in its {@code type_json}, such as {@code
   * {"metadata": {"delta.generationExpression": "CAST(ts AS DATE)"}}}. All clauses are empty if the
   * JSON can't be parsed.
   */
  @Nonnull
  static ColumnClauses parseColumnClauses(@CheckForNull String typeJson) {
    JsonNode metadata = readColumnMetadata(typeJson);
    String identityStart = scalarTextOrEmpty(metadata, IDENTITY_START_KEY);
    String identityGeneration = "";
    String identityIncrement = "";
    if (!identityStart.isEmpty()) {
      boolean allowsExplicitInsert =
          metadata.path(IDENTITY_ALLOW_EXPLICIT_INSERT_KEY).asBoolean(false);
      identityGeneration = allowsExplicitInsert ? IDENTITY_BY_DEFAULT : IDENTITY_ALWAYS;
      identityIncrement = scalarTextOrEmpty(metadata, IDENTITY_STEP_KEY);
    }

    return new ColumnClauses(
        scalarTextOrEmpty(metadata, CURRENT_DEFAULT_KEY),
        scalarTextOrEmpty(metadata, GENERATION_EXPRESSION_KEY),
        identityGeneration,
        identityStart,
        identityIncrement);
  }

  @Nonnull
  private static JsonNode readColumnMetadata(@CheckForNull String typeJson) {
    return readTypeJson(typeJson).path("metadata");
  }

  /** Returns the {@code type_json} of a column, or a missing node if it can't be parsed. */
  @Nonnull
  private static JsonNode readTypeJson(@CheckForNull String typeJson) {
    if (Strings.isNullOrEmpty(typeJson)) {
      return MissingNode.getInstance();
    }

    try {
      return TYPE_JSON_READER.readTree(typeJson);
    } catch (IOException e) {
      return MissingNode.getInstance();
    }
  }

  /**
   * Returns the type of a column in the syntax of the columns files. A struct, array or map type is
   * written from the {@code type_json} of the column, because {@code type_text} drops the {@code
   * not null} and comments of struct fields and doesn't quote field names, as in {@code
   * struct<id:bigint not null comment 'key',`a-b`:int>}. That is the syntax of the {@code
   * hive_metastore} types. Other types, and types that {@code type_json} doesn't describe in a
   * known way, are written as {@code type_text}.
   */
  @Nonnull
  static String formatDataType(@CheckForNull String typeText, @CheckForNull String typeJson) {
    JsonNode column = readTypeJson(typeJson);
    JsonNode type = column.path("type");
    if (type.isObject()) {
      String formattedType =
          formatType(
              type, column.path("name").asText(), column.path("metadata").path(COLLATIONS_KEY));
      if (formattedType != null) {
        return formattedType;
      }
    }
    return Strings.nullToEmpty(typeText);
  }

  /**
   * Returns a type of {@code type_json}, or null if the type isn't known.
   *
   * @param path the path of the type in the nearest field, such as {@code tags.element}
   * @param collations the collations of the strings in the type of the nearest field, by path
   */
  @CheckForNull
  private static String formatType(
      @Nonnull JsonNode type, @Nonnull String path, @Nonnull JsonNode collations) {
    if (type.isTextual()) {
      return formatPrimitiveType(type.asText(), collations.path(path).asText(""));
    }

    switch (type.path("type").asText()) {
      case "struct":
        return formatStructType(type);
      case "array":
        String elementType = formatType(type.path("elementType"), path + ".element", collations);
        return elementType == null ? null : "array<" + elementType + ">";
      case "map":
        String keyType = formatType(type.path("keyType"), path + ".key", collations);
        String valueType = formatType(type.path("valueType"), path + ".value", collations);
        return keyType == null || valueType == null
            ? null
            : "map<" + keyType + "," + valueType + ">";
      default:
        return null;
    }
  }

  @CheckForNull
  private static String formatStructType(@Nonnull JsonNode type) {
    StringBuilder struct = new StringBuilder("struct<");
    for (JsonNode field : type.path("fields")) {
      String name = field.path("name").asText();
      JsonNode metadata = field.path("metadata");
      String fieldType = formatType(field.path("type"), name, metadata.path(COLLATIONS_KEY));
      if (fieldType == null) {
        return null;
      }

      if (struct.length() > "struct<".length()) {
        struct.append(',');
      }
      struct.append(quoteIdentifierIfNeeded(name)).append(':').append(fieldType);
      if (!field.path("nullable").asBoolean(true)) {
        struct.append(" not null");
      }

      String comment = metadata.path(COMMENT_KEY).asText("");
      if (!comment.isEmpty()) {
        struct.append(" comment ").append(quoteStringLiteral(comment));
      }
    }
    return struct.append('>').toString();
  }

  /**
   * Returns a primitive type of {@code type_json}, such as {@code integer} or {@code
   * decimal(10,2)}, as {@code type_text} writes it, such as {@code int}. A string with a collation
   * other than {@link #DEFAULT_COLLATION}, such as {@code spark.UTF8_LCASE}, is written as {@code
   * string collate UTF8_LCASE}.
   */
  @Nonnull
  private static String formatPrimitiveType(@Nonnull String typeName, @Nonnull String collation) {
    // The collation is qualified by its provider, such as spark or icu.
    String collationName = collation.substring(collation.indexOf('.') + 1);
    if (typeName.equals("string")
        && !collationName.isEmpty()
        && !collationName.equalsIgnoreCase(DEFAULT_COLLATION)) {
      return "string collate " + collationName;
    }

    String typeTextName = TYPE_TEXT_NAMES.get(typeName);
    return typeTextName == null ? typeName : typeTextName;
  }

  @Nonnull
  private static String scalarTextOrEmpty(@Nonnull JsonNode node, @Nonnull String fieldName) {
    JsonNode value = node.get(fieldName);
    if (value == null || value.isNull() || value.isContainerNode()) {
      return "";
    }
    return value.asText();
  }

  /**
   * Returns the column mask as a JSON object: {@code function}, {@code usingColumns} and, if
   * present, {@code usingArguments}. Returns an empty string if the column has no mask.
   */
  @Nonnull
  static String formatMask(@CheckForNull ColumnMask mask) {
    if (mask == null) {
      return "";
    }

    Map<String, Object> details = new LinkedHashMap<>();
    details.put("function", Strings.nullToEmpty(mask.getFunctionName()));
    details.put("usingColumns", copyWithoutNulls(mask.getUsingColumnNames()));
    putArgumentsIfPresent(details, "usingArguments", mask.getUsingArguments());
    return toJsonOrEmpty(details);
  }

  /**
   * Returns the row filter as a JSON object: {@code function}, {@code inputColumns} and, if
   * present, {@code inputArguments}. Returns an empty string if the table has no row filter.
   */
  @Nonnull
  static String formatRowFilter(@CheckForNull TableRowFilter rowFilter) {
    if (rowFilter == null) {
      return "";
    }

    Map<String, Object> details = new LinkedHashMap<>();
    details.put("function", Strings.nullToEmpty(rowFilter.getFunctionName()));
    details.put("inputColumns", copyWithoutNulls(rowFilter.getInputColumnNames()));
    putArgumentsIfPresent(details, "inputArguments", rowFilter.getInputArguments());
    return toJsonOrEmpty(details);
  }

  private static void putArgumentsIfPresent(
      @Nonnull Map<String, Object> details,
      @Nonnull String key,
      @CheckForNull Collection<PolicyFunctionArgument> arguments) {
    if (arguments == null || arguments.isEmpty()) {
      return;
    }

    ImmutableList.Builder<ImmutableMap<String, String>> builder = ImmutableList.builder();
    for (PolicyFunctionArgument argument : arguments) {
      if (argument == null) {
        continue;
      }

      ImmutableMap.Builder<String, String> fields = ImmutableMap.builder();
      if (argument.getColumn() != null) {
        fields.put("column", argument.getColumn());
      }
      if (argument.getConstant() != null) {
        fields.put("constant", argument.getConstant());
      }
      builder.add(fields.build());
    }
    details.put(key, builder.build());
  }

  /**
   * Returns the current catalog and schema when a view was created, such as {@code main.sales},
   * from its {@code view.catalogAndNamespace.*} properties, as {@code DESCRIBE TABLE EXTENDED}
   * prints them. Returns an empty string if the properties are absent or incomplete.
   */
  @Nonnull
  static String formatViewCatalogAndNamespace(@CheckForNull Map<String, String> properties) {
    List<String> quotedParts = new ArrayList<>();
    for (String part :
        readListProperty(
            properties,
            VIEW_CATALOG_AND_NAMESPACE_PARTS_KEY,
            VIEW_CATALOG_AND_NAMESPACE_PART_KEY_PREFIX)) {
      quotedParts.add(quoteIdentifierIfNeeded(part));
    }
    return String.join(".", quotedParts);
  }

  /**
   * Returns the names of the columns of a view query from the {@code view.query.out.*} properties
   * of the view, or an empty list if they are absent or incomplete.
   */
  @Nonnull
  static ImmutableList<String> parseViewQueryOutputColumns(
      @CheckForNull Map<String, String> properties) {
    return readListProperty(
        properties, VIEW_QUERY_OUTPUT_COLUMNS_KEY, VIEW_QUERY_OUTPUT_COLUMN_KEY_PREFIX);
  }

  /** Returns the {@code view.schemaMode} property of a view, or an empty string if absent. */
  @Nonnull
  static String formatViewSchemaMode(@CheckForNull Map<String, String> properties) {
    return properties == null ? "" : Strings.nullToEmpty(properties.get(VIEW_SCHEMA_MODE_KEY));
  }

  /**
   * Returns a list that Spark stores in properties as a count and one property per element, such as
   * {@code view.query.out.numCols=2}, {@code view.query.out.col.0=a} and {@code
   * view.query.out.col.1=b}. Returns an empty list if the count is absent or invalid, or an element
   * is missing.
   */
  @Nonnull
  private static ImmutableList<String> readListProperty(
      @CheckForNull Map<String, String> properties,
      @Nonnull String countKey,
      @Nonnull String elementKeyPrefix) {
    String countText = properties == null ? null : properties.get(countKey);
    if (countText == null) {
      return ImmutableList.of();
    }

    int count;
    try {
      count = Integer.parseInt(countText.trim());
    } catch (NumberFormatException e) {
      return ImmutableList.of();
    }

    ImmutableList.Builder<String> elements = ImmutableList.builder();
    for (int index = 0; index < count; index++) {
      String element = properties.get(elementKeyPrefix + index);
      if (element == null) {
        return ImmutableList.of();
      }
      elements.add(element);
    }
    return elements.build();
  }

  /** Returns the parameters of a function, or the columns of a table function, in order. */
  @Nonnull
  static ImmutableList<DatabricksFunctionParameter> toFunctionParameters(
      @CheckForNull FunctionParameterInfos parameterInfos) {
    if (parameterInfos == null) {
      return ImmutableList.of();
    }

    ImmutableList.Builder<DatabricksFunctionParameter> parameters = ImmutableList.builder();
    for (FunctionParameterInfo info :
        sortByPosition(parameterInfos.getParameters(), FunctionParameterInfo::getPosition)) {
      parameters.add(
          DatabricksFunctionParameter.newBuilder(
                  Strings.nullToEmpty(info.getName()), Strings.nullToEmpty(info.getTypeText()))
              .setMode(info.getParameterMode() == null ? null : info.getParameterMode().name())
              .setDefaultExpression(info.getParameterDefault())
              .setComment(info.getComment())
              .build());
    }
    return parameters.build();
  }

  /**
   * Returns the return type of a function, such as {@code INT}, or {@code TABLE(...)} with the
   * columns of a table function, as for {@code hive_metastore}.
   */
  @Nonnull
  static String formatFunctionDataType(
      @Nonnull FunctionInfo function, @Nonnull List<DatabricksFunctionParameter> returnColumns) {
    if (returnColumns.isEmpty()) {
      return Strings.nullToEmpty(function.getFullDataType());
    }

    List<String> columns = new ArrayList<>();
    for (DatabricksFunctionParameter column : returnColumns) {
      columns.add(column.toSqlDefinition());
    }
    return "TABLE(" + String.join(", ", columns) + ")";
  }

  /** Returns the language of the body of a function, such as {@code SQL} or {@code PYTHON}. */
  @Nonnull
  static String formatRoutineLanguage(@Nonnull FunctionInfo function) {
    if (function.getRoutineBody() == FunctionInfoRoutineBody.SQL) {
      return SQL_LANGUAGE;
    }

    String externalLanguage = function.getExternalLanguage();
    return externalLanguage == null ? "" : externalLanguage.toUpperCase(Locale.ROOT);
  }

  /**
   * Returns the details of a primary key as a JSON object: {@code columns} and, if present, {@code
   * rely} and {@code timeseriesColumns}.
   */
  @Nonnull
  static String formatPrimaryKeyDetails(@Nonnull PrimaryKeyConstraint primaryKey) {
    Map<String, Object> details = new LinkedHashMap<>();
    details.put("columns", copyWithoutNulls(primaryKey.getChildColumns()));
    if (primaryKey.getRely() != null) {
      details.put("rely", primaryKey.getRely());
    }
    ImmutableList<String> timeseriesColumns = copyWithoutNulls(primaryKey.getTimeseriesColumns());
    if (!timeseriesColumns.isEmpty()) {
      details.put("timeseriesColumns", timeseriesColumns);
    }
    return toJsonOrEmpty(details);
  }

  /**
   * Returns the details of a foreign key as a JSON object: {@code columns}, {@code
   * referencedTable}, {@code referencedColumns} and, if present, {@code rely}.
   */
  @Nonnull
  static String formatForeignKeyDetails(@Nonnull ForeignKeyConstraint foreignKey) {
    Map<String, Object> details = new LinkedHashMap<>();
    details.put("columns", copyWithoutNulls(foreignKey.getChildColumns()));
    details.put("referencedTable", Strings.nullToEmpty(foreignKey.getParentTable()));
    details.put("referencedColumns", copyWithoutNulls(foreignKey.getParentColumns()));
    if (foreignKey.getRely() != null) {
      details.put("rely", foreignKey.getRely());
    }
    return toJsonOrEmpty(details);
  }

  /** Returns the elements that are not null, or an empty list if the collection is null. */
  @Nonnull
  static <T> ImmutableList<T> copyWithoutNulls(@CheckForNull Collection<T> values) {
    if (values == null) {
      return ImmutableList.of();
    }

    ImmutableList.Builder<T> builder = ImmutableList.builder();
    for (T value : values) {
      if (value != null) {
        builder.add(value);
      }
    }
    return builder.build();
  }

  /**
   * Returns the elements in the order of their positions, keeping the order of the elements with
   * the same position or without one; the elements without a position go last.
   */
  @Nonnull
  private static <T> ImmutableList<T> sortByPosition(
      @CheckForNull Collection<T> values, @Nonnull Function<T, Long> position) {
    List<T> sorted = new ArrayList<>(copyWithoutNulls(values));
    sorted.sort(Comparator.comparing(position, Comparator.nullsLast(Comparator.naturalOrder())));
    return ImmutableList.copyOf(sorted);
  }
}
