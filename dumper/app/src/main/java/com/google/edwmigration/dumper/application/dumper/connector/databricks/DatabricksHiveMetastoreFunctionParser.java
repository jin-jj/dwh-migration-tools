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
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksSqlHelper.quoteIdentifier;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
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
 * Parses the {@code DESCRIBE FUNCTION EXTENDED} output of Databricks {@code hive_metastore}
 * functions.
 */
final class DatabricksHiveMetastoreFunctionParser {

  private static final Logger logger =
      LoggerFactory.getLogger(DatabricksHiveMetastoreFunctionParser.class);

  /** The format of {@code java.util.Date#toString}, which Spark uses for {@code Create Time}. */
  private static final String CREATED_TIME_PATTERN = "EEE MMM dd HH:mm:ss zzz yyyy";

  private static final DateTimeFormatter CREATED_TIME_FORMATTER =
      DateTimeFormatter.ofPattern(CREATED_TIME_PATTERN, Locale.US);

  /**
   * Matches {@code Key: value}. {@code DOTALL} lets the value span lines, because {@code DESCRIBE
   * FUNCTION EXTENDED} returns a multi-line {@code Body} as one cell with embedded newlines.
   */
  private static final Pattern KEY_VALUE_LINE_PATTERN =
      Pattern.compile("^([A-Za-z][A-Za-z0-9 _-]*):(?:\\s*(.*))?$", Pattern.DOTALL);

  private static final String INPUT_KEY = "input";
  private static final String RETURNS_KEY = "returns";
  private static final String BODY_KEY = "body";

  private static final String TABLE_FUNCTION_TYPE = "TABLE";

  private static final String SQL_LANGUAGE = "SQL";

  private static final String DEFAULT_KEYWORD = "DEFAULT";

  /**
   * The mode of every parameter of a {@code hive_metastore} function, as Unity Catalog reports it
   * for an input parameter. The columns that a table function returns have no mode.
   */
  private static final String INPUT_PARAMETER_MODE = "IN";

  /** Characters that leave a SQL expression waiting for another operand, as in {@code 'a' ||}. */
  private static final String OPERATOR_CHARACTERS = "|+-*/%=<>!&^~,(:";

  /** Keywords that leave a SQL expression waiting for another operand, as in {@code x LIKE}. */
  private static final ImmutableSet<String> OPERAND_EXPECTING_KEYWORDS =
      ImmutableSet.of(
          "AND", "BETWEEN", "CASE", "ELSE", "ESCAPE", "ILIKE", "IN", "IS", "LIKE", "NOT", "OR",
          "REGEXP", "RLIKE", "THEN", "WHEN");

  private DatabricksHiveMetastoreFunctionParser() {}

  /** A function parsed from its {@code DESCRIBE FUNCTION EXTENDED} output. */
  static final class ParsedFunction {
    final String functionCatalog;
    final String functionSchema;
    final String functionName;
    final boolean isTableFunction;
    final ImmutableList<DatabricksFunctionParameter> inputParams;

    /** The columns of the table that a table function returns. Empty for other functions. */
    final ImmutableList<DatabricksFunctionParameter> returnParams;

    /** The return type, such as {@code INT}, or {@code TABLE(...)} for a table function. */
    final String dataType;

    /** The body of a SQL or Python function. */
    final String routineDefinition;

    /** Such as {@code SQL} or {@code PYTHON}, for a function with a body. */
    final String routineLanguage;

    final String comment;
    final String owner;
    final String isDeterministic;

    /** Such as {@code CONTAINS SQL} or {@code READS SQL DATA}. */
    final String sqlDataAccess;

    final String createdAt;

    /** The class of a Hive function, as in {@code CREATE FUNCTION f AS 'com.acme.Udf'}. */
    final String className;

    /** The handler class of a Python table function. */
    final String handler;

    private ParsedFunction(
        @Nonnull String catalogName,
        @Nonnull String schemaName,
        @Nonnull String functionName,
        @Nonnull FunctionDescription description) {
      this.functionCatalog = catalogName;
      this.functionSchema = schemaName;
      this.functionName = functionName;
      this.handler = description.value("handler");
      this.isTableFunction =
          TABLE_FUNCTION_TYPE.equalsIgnoreCase(description.value("type")) || !handler.isEmpty();
      this.inputParams = parseSectionRows(description.inputRows, INPUT_PARAMETER_MODE);
      this.returnParams =
          isTableFunction ? parseSectionRows(description.returnRows, null) : ImmutableList.of();
      this.dataType = formatDataType(isTableFunction, description.returnRows, returnParams);
      String body = Strings.nullToEmpty(description.body);
      String language = description.value("language");
      this.routineDefinition = body;
      this.routineLanguage =
          body.isEmpty()
              ? ""
              : language.isEmpty() ? SQL_LANGUAGE : language.toUpperCase(Locale.ROOT);
      this.comment = description.value("comment");
      this.owner = description.value("owner");
      this.isDeterministic = description.value("deterministic").toLowerCase(Locale.ROOT);
      this.sqlDataAccess =
          description.value("data access").replaceAll("\\s+", " ").toUpperCase(Locale.ROOT);
      this.createdAt =
          formatCreatedTime(
              description.value("create time"),
              quoteIdentifier(schemaName) + "." + quoteIdentifier(functionName));
      this.className = description.value("class");
    }
  }

  /**
   * The values of {@code DESCRIBE FUNCTION EXTENDED} output: the single values by lower-case key,
   * the rows of the {@code Input} and {@code Returns} sections, and the body.
   */
  private static final class FunctionDescription {
    final Map<String, String> values = new HashMap<>();
    final List<String> inputRows = new ArrayList<>();
    final List<String> returnRows = new ArrayList<>();
    @CheckForNull String body;

    @Nonnull
    String value(@Nonnull String key) {
      return values.getOrDefault(key, "");
    }
  }

  @Nonnull
  static ParsedFunction parseDescribeFunctionExtended(
      @Nonnull String catalogName,
      @Nonnull String schemaName,
      @Nonnull String functionName,
      @Nonnull List<String> lines) {
    FunctionDescription description = new FunctionDescription();
    int index = 0;
    while (index < lines.size()) {
      String line = lines.get(index);
      Matcher keyValue = line == null ? null : KEY_VALUE_LINE_PATTERN.matcher(line);
      if (keyValue != null && keyValue.matches()) {
        String key = keyValue.group(1).trim().toLowerCase(Locale.ROOT);
        if (INPUT_KEY.equals(key)) {
          index = collectSectionRows(lines, index, description.inputRows);
        } else if (RETURNS_KEY.equals(key)) {
          index = collectSectionRows(lines, index, description.returnRows);
        } else if (BODY_KEY.equals(key)) {
          index = collectBody(lines, index, description);
        } else {
          description.values.put(key, keyValue.group(2) != null ? keyValue.group(2).trim() : "");
        }
      }
      index++;
    }
    return new ParsedFunction(catalogName, schemaName, functionName, description);
  }

  /**
   * Returns the return type of a scalar function, such as {@code INT}, or {@code TABLE(...)} with
   * the columns of a table function.
   */
  @Nonnull
  private static String formatDataType(
      boolean isTableFunction,
      @Nonnull List<String> returnRows,
      @Nonnull List<DatabricksFunctionParameter> returnParams) {
    if (returnRows.isEmpty()) {
      return "";
    }

    if (!isTableFunction) {
      return String.join(" ", trimmedSectionRows(returnRows));
    }

    List<String> columns = new ArrayList<>();
    for (DatabricksFunctionParameter column : returnParams) {
      columns.add(column.toSqlDefinition());
    }
    return "TABLE(" + String.join(", ", columns) + ")";
  }

  /**
   * Returns a time that Spark prints with {@code java.util.Date#toString}, such as the {@code
   * Create Time} {@code Sun Sep 27 21:03:11 UTC 2026}, as an ISO 8601 instant, such as {@code
   * 2026-09-27T21:03:11Z}. A time in another format is logged and returned as an empty string, so
   * that the {@code CreatedAt} column holds only instants, as for Unity Catalog.
   *
   * @param functionName the quoted name of the function, for the warning
   */
  @Nonnull
  private static String formatCreatedTime(
      @Nonnull String createdTime, @Nonnull String functionName) {
    if (createdTime.isEmpty()) {
      return "";
    }

    try {
      return ZonedDateTime.parse(createdTime, CREATED_TIME_FORMATTER).toInstant().toString();
    } catch (DateTimeParseException e) {
      logger.warn(
          "Writing {} without CreatedAt because its created time '{}' doesn't match '{}'.",
          functionName,
          createdTime,
          CREATED_TIME_PATTERN);
      return "";
    }
  }

  /**
   * Sets the body from the {@code Body:} line at {@code index} and its continuation lines.
   *
   * @return the index of the last line of the body
   */
  private static int collectBody(
      @Nonnull List<String> lines, int index, @Nonnull FunctionDescription description) {
    StringBuilder body = new StringBuilder(parseBodyFirstCell(lines.get(index)));
    int last = index;
    while (last + 1 < lines.size()) {
      String next = lines.get(last + 1);
      if (next == null || KEY_VALUE_LINE_PATTERN.matcher(next).matches()) {
        break;
      }
      if (body.length() > 0) {
        body.append("\n");
      }
      body.append(next);
      last++;
    }
    description.body = whitespace().trimTrailingFrom(body);
    return last;
  }

  /**
   * Extracts the body text from a {@code Body:} cell without stripping the indentation of its first
   * line.
   *
   * <p>Databricks prints {@code Body:} padded with spaces to the value column, followed by the
   * body. When a function is defined with {@code AS $$\n...\n$$}, the body starts on the line after
   * {@code Body:}, and its first line may be indented Python code.
   */
  @Nonnull
  private static String parseBodyFirstCell(@Nonnull String line) {
    int colon = line.indexOf(':');
    int start = colon + 1;
    while (start < line.length() && (line.charAt(start) == ' ' || line.charAt(start) == '\t')) {
      start++;
    }
    if (start < line.length() && line.charAt(start) == '\r') {
      start++;
    }
    if (start < line.length() && line.charAt(start) == '\n') {
      start++;
    }
    return whitespace().trimTrailingFrom(line.substring(start));
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
   * Parses the {@code Input} section, or the {@code Returns} section of a table function, into
   * parameters.
   *
   * <p>Each row reads {@code name type [DEFAULT expression] ['comment']}, with the name and type
   * columns padded to their widest value. Names and comments are printed without escaping, so a
   * name ends where the type column starts in every row, and the comment is the quoted text at the
   * end of a row. A name that contains a space is only recognized in a section with several rows.
   *
   * @param mode the mode of each parameter, such as {@code IN}, or null for none
   */
  @Nonnull
  private static ImmutableList<DatabricksFunctionParameter> parseSectionRows(
      @Nonnull List<String> rows, @CheckForNull String mode) {
    ImmutableList<String> aligned = alignSectionRows(rows);
    if (aligned.isEmpty() || (aligned.size() == 1 && aligned.get(0).trim().equals("()"))) {
      return ImmutableList.of();
    }

    int nameStart = Integer.MAX_VALUE;
    for (String row : aligned) {
      nameStart = Math.min(nameStart, indexOfNonWhitespace(row, 0));
    }
    int typeStart = findTypeColumn(aligned, nameStart);
    ImmutableList.Builder<DatabricksFunctionParameter> parameters = ImmutableList.builder();
    for (String row : aligned) {
      String name = typeStart < 0 ? "" : row.substring(nameStart, typeStart).trim();
      if (name.isEmpty()) {
        parameters.add(
            DatabricksFunctionParameter.newBuilder("", row.trim()).setMode(mode).build());
      } else {
        parameters.add(parseTypeDefaultAndComment(name, row.substring(typeStart).trim(), mode));
      }
    }
    return parameters.build();
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
   * Parses {@code type [DEFAULT expression] ['comment']}. If the text after the type is neither,
   * the whole text is kept as the type.
   */
  @Nonnull
  private static DatabricksFunctionParameter parseTypeDefaultAndComment(
      @Nonnull String name, @Nonnull String value, @CheckForNull String mode) {
    int typeEnd = findTypeEnd(value);
    String rest = value.substring(typeEnd).trim();
    DatabricksFunctionParameter.Builder parameter =
        DatabricksFunctionParameter.newBuilder(name, value.substring(0, typeEnd).trim())
            .setMode(mode);
    if (startsWithKeyword(rest, DEFAULT_KEYWORD)) {
      String expression = rest.substring(DEFAULT_KEYWORD.length()).trim();
      int commentStart = findTrailingComment(expression);
      if (commentStart < 0) {
        parameter.setDefaultExpression(expression);
      } else {
        parameter
            .setDefaultExpression(expression.substring(0, commentStart).trim())
            .setComment(expression.substring(commentStart + 1, expression.length() - 1));
      }
    } else if (isQuotedText(rest)) {
      parameter.setComment(rest.substring(1, rest.length() - 1));
    } else if (!rest.isEmpty()) {
      return DatabricksFunctionParameter.newBuilder(name, value).setMode(mode).build();
    }
    return parameter.build();
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
