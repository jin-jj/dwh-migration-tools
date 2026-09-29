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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

/**
 * Parses the {@code DESCRIBE FUNCTION EXTENDED} output of Databricks {@code hive_metastore}
 * functions and synthesizes their {@code CREATE FUNCTION} statements.
 */
final class DatabricksHiveMetastoreFunctionParser {

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

  private DatabricksHiveMetastoreFunctionParser() {}

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
