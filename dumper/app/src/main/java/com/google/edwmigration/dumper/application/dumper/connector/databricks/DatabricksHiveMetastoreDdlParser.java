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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * Parses the column clauses that only the {@code SHOW CREATE TABLE} output of a {@code
 * hive_metastore} table shows: {@code GENERATED ALWAYS AS (expression)} and {@code GENERATED
 * {ALWAYS | BY DEFAULT} AS IDENTITY (START WITH n INCREMENT BY m)}.
 *
 * <p>The statement is read as Databricks prints it: the column list follows the table name, a
 * column definition starts with the column name, and the clauses are outside of string literals,
 * quoted identifiers and parentheses.
 */
final class DatabricksHiveMetastoreDdlParser {

  /** Matches the start of a {@code CREATE TABLE} statement, up to the table name. */
  private static final Pattern CREATE_TABLE_PATTERN =
      Pattern.compile("\\s*CREATE\\s+(?:EXTERNAL\\s+)?TABLE\\s+", Pattern.CASE_INSENSITIVE);

  private static final Pattern IDENTITY_START_PATTERN =
      Pattern.compile("START\\s+WITH\\s+([-+]?\\d+)", Pattern.CASE_INSENSITIVE);

  private static final Pattern IDENTITY_INCREMENT_PATTERN =
      Pattern.compile("INCREMENT\\s+BY\\s+([-+]?\\d+)", Pattern.CASE_INSENSITIVE);

  private static final String IDENTITY_ALWAYS = "ALWAYS";
  private static final String IDENTITY_BY_DEFAULT = "BY DEFAULT";

  private DatabricksHiveMetastoreDdlParser() {}

  /** The generation and identity clauses of a column. A missing clause is empty. */
  static final class ParsedColumnClauses {

    /** The expression of {@code GENERATED ALWAYS AS (expression)}. */
    final String generationExpression;

    /** {@code ALWAYS} or {@code BY DEFAULT}, for an identity column. */
    final String identityGeneration;

    final String identityStart;
    final String identityIncrement;

    ParsedColumnClauses(
        @Nonnull String generationExpression,
        @Nonnull String identityGeneration,
        @Nonnull String identityStart,
        @Nonnull String identityIncrement) {
      this.generationExpression = generationExpression;
      this.identityGeneration = identityGeneration;
      this.identityStart = identityStart;
      this.identityIncrement = identityIncrement;
    }
  }

  /**
   * Returns the generation and identity clauses of the columns of a {@code CREATE TABLE} statement,
   * by lower-case column name. Columns without these clauses are left out, and so is every column
   * if the statement has no column list.
   */
  @Nonnull
  static ImmutableMap<String, ParsedColumnClauses> parseColumnClauses(@Nonnull String ddl) {
    int columnListStart = findColumnListStart(ddl);
    if (columnListStart < 0) {
      return ImmutableMap.of();
    }

    Map<String, ParsedColumnClauses> clauses = new LinkedHashMap<>();
    for (String definition : splitColumnDefinitions(ddl, columnListStart)) {
      int nameEnd = findNameEnd(definition, 0);
      String name = unquoteIdentifier(definition.substring(0, nameEnd));
      ParsedColumnClauses columnClauses = parseGeneratedClause(definition, nameEnd);
      if (columnClauses != null && !name.isEmpty()) {
        clauses.putIfAbsent(name.toLowerCase(Locale.ROOT), columnClauses);
      }
    }
    return ImmutableMap.copyOf(clauses);
  }

  /** Returns the index after the parenthesis that opens the column list, or -1 without one. */
  private static int findColumnListStart(@Nonnull String ddl) {
    Matcher createTable = CREATE_TABLE_PATTERN.matcher(ddl);
    if (!createTable.lookingAt()) {
      return -1;
    }

    int index = findNameEnd(ddl, createTable.end());
    while (index < ddl.length() && Character.isWhitespace(ddl.charAt(index))) {
      index++;
    }
    return index < ddl.length() && ddl.charAt(index) == '(' ? index + 1 : -1;
  }

  /**
   * Returns the column definitions of the column list that starts at {@code start}: the text
   * between the commas outside of literals, identifiers, parentheses and type brackets, such as the
   * brackets of {@code STRUCT<a: INT, b: INT>}.
   */
  @Nonnull
  private static ImmutableList<String> splitColumnDefinitions(@Nonnull String ddl, int start) {
    ImmutableList.Builder<String> definitions = ImmutableList.builder();
    Deque<Character> brackets = new ArrayDeque<>();
    int definitionStart = start;
    int index = start;
    while (index < ddl.length()) {
      char c = ddl.charAt(index);
      if (isQuote(c)) {
        index = skipQuoted(ddl, index);
        continue;
      }

      if (c == '(' || (c == '<' && followsTypeName(ddl, index))) {
        brackets.push(c);
      } else if (c == '>' && !brackets.isEmpty() && brackets.peek() == '<') {
        brackets.pop();
      } else if (c == ')') {
        if (brackets.isEmpty()) {
          addDefinition(definitions, ddl.substring(definitionStart, index));
          return definitions.build();
        }

        brackets.pop();
      } else if (c == ',' && brackets.isEmpty()) {
        addDefinition(definitions, ddl.substring(definitionStart, index));
        definitionStart = index + 1;
      }
      index++;
    }
    // Without the closing parenthesis, the statement is cut off, so its columns are unreliable.
    return ImmutableList.of();
  }

  private static void addDefinition(
      @Nonnull ImmutableList.Builder<String> definitions, @Nonnull String definition) {
    String trimmed = definition.trim();
    if (!trimmed.isEmpty()) {
      definitions.add(trimmed);
    }
  }

  /**
   * Returns the clauses of the {@code GENERATED} keyword in a column definition, from {@code
   * start}, or null if the definition has no such clause.
   */
  @CheckForNull
  private static ParsedColumnClauses parseGeneratedClause(@Nonnull String definition, int start) {
    int keyword = findTopLevelWord(definition, "GENERATED", start);
    while (keyword >= 0) {
      ParsedColumnClauses clauses =
          parseGeneratedClauseAt(definition, keyword + "GENERATED".length());
      if (clauses != null) {
        return clauses;
      }

      keyword = findTopLevelWord(definition, "GENERATED", keyword + 1);
    }
    return null;
  }

  /** Parses the words after {@code GENERATED}, or returns null if they aren't a clause. */
  @CheckForNull
  private static ParsedColumnClauses parseGeneratedClauseAt(@Nonnull String text, int start) {
    WordReader reader = new WordReader(text, start);
    String generation;
    String word = reader.nextWord();
    if (IDENTITY_ALWAYS.equalsIgnoreCase(word)) {
      generation = IDENTITY_ALWAYS;
    } else if ("BY".equalsIgnoreCase(word) && "DEFAULT".equalsIgnoreCase(reader.nextWord())) {
      generation = IDENTITY_BY_DEFAULT;
    } else {
      return null;
    }

    if (!"AS".equalsIgnoreCase(reader.nextWord())) {
      return null;
    }

    reader.skipWhitespace();
    if (reader.peek() == '(') {
      String expression = reader.nextParenthesized();
      if (expression == null || !IDENTITY_ALWAYS.equals(generation)) {
        return null;
      }

      return new ParsedColumnClauses(expression.trim(), "", "", "");
    }

    if (!"IDENTITY".equalsIgnoreCase(reader.nextWord())) {
      return null;
    }

    reader.skipWhitespace();
    String options = reader.peek() == '(' ? reader.nextParenthesized() : null;
    return new ParsedColumnClauses(
        "",
        generation,
        findNumber(IDENTITY_START_PATTERN, options),
        findNumber(IDENTITY_INCREMENT_PATTERN, options));
  }

  @Nonnull
  private static String findNumber(@Nonnull Pattern pattern, @CheckForNull String text) {
    if (text == null) {
      return "";
    }

    Matcher matcher = pattern.matcher(text);
    return matcher.find() ? matcher.group(1) : "";
  }

  /**
   * Returns the index of a word outside of literals, identifiers and brackets, from {@code start},
   * or -1.
   */
  private static int findTopLevelWord(@Nonnull String text, @Nonnull String word, int start) {
    int depth = 0;
    int index = start;
    while (index < text.length()) {
      char c = text.charAt(index);
      if (isQuote(c)) {
        index = skipQuoted(text, index);
        continue;
      }

      if (c == '(' || (c == '<' && followsTypeName(text, index))) {
        depth++;
      } else if ((c == ')' || c == '>') && depth > 0) {
        depth--;
      } else if (depth == 0
          && text.regionMatches(true, index, word, 0, word.length())
          && !isWordChar(text, index - 1)
          && !isWordChar(text, index + word.length())) {
        return index;
      }
      index++;
    }
    return -1;
  }

  /** Returns the index after an identifier or a qualified name that starts at {@code start}. */
  private static int findNameEnd(@Nonnull String text, int start) {
    int index = start;
    while (index < text.length()) {
      char c = text.charAt(index);
      if (c == '`') {
        index = skipQuoted(text, index);
      } else if (Character.isWhitespace(c) || c == '(') {
        return index;
      } else {
        index++;
      }
    }
    return index;
  }

  @Nonnull
  private static String unquoteIdentifier(@Nonnull String identifier) {
    if (identifier.length() >= 2 && identifier.startsWith("`") && identifier.endsWith("`")) {
      return identifier.substring(1, identifier.length() - 1).replace("``", "`");
    }
    return identifier;
  }

  private static boolean isQuote(char c) {
    return c == '\'' || c == '"' || c == '`';
  }

  /**
   * Returns the index after the literal or identifier that starts at {@code start}. A backslash
   * escapes the next character of a literal, and a doubled backtick is a backtick of an identifier.
   */
  private static int skipQuoted(@Nonnull String text, int start) {
    char quote = text.charAt(start);
    int index = start + 1;
    while (index < text.length()) {
      char c = text.charAt(index);
      if (c == '\\' && quote != '`') {
        index += 2;
      } else if (c == quote) {
        if (quote == '`' && index + 1 < text.length() && text.charAt(index + 1) == '`') {
          index += 2;
        } else {
          return index + 1;
        }
      } else {
        index++;
      }
    }
    return text.length();
  }

  /** Returns whether the {@code <} at {@code index} opens the parameters of a complex type. */
  private static boolean followsTypeName(@Nonnull String text, int index) {
    int end = index;
    while (end > 0 && Character.isWhitespace(text.charAt(end - 1))) {
      end--;
    }
    int begin = end;
    while (begin > 0 && isWordChar(text, begin - 1)) {
      begin--;
    }
    String word = text.substring(begin, end).toUpperCase(Locale.ROOT);
    return word.equals("ARRAY") || word.equals("MAP") || word.equals("STRUCT");
  }

  private static boolean isWordChar(@Nonnull String text, int index) {
    if (index < 0 || index >= text.length()) {
      return false;
    }

    char c = text.charAt(index);
    return Character.isLetterOrDigit(c) || c == '_';
  }

  /** Reads the words and parenthesized text of a clause. */
  private static final class WordReader {

    private final String text;
    private int index;

    WordReader(@Nonnull String text, int index) {
      this.text = text;
      this.index = index;
    }

    void skipWhitespace() {
      while (index < text.length() && Character.isWhitespace(text.charAt(index))) {
        index++;
      }
    }

    /** Returns the next character, or a space at the end of the text. */
    char peek() {
      return index < text.length() ? text.charAt(index) : ' ';
    }

    @Nonnull
    String nextWord() {
      skipWhitespace();
      int start = index;
      while (isWordChar(text, index)) {
        index++;
      }
      return text.substring(start, index);
    }

    /**
     * Returns the text inside the parentheses that open at the current index, or null if they don't
     * close.
     */
    @CheckForNull
    String nextParenthesized() {
      int start = index + 1;
      int depth = 0;
      while (index < text.length()) {
        char c = text.charAt(index);
        if (isQuote(c)) {
          index = skipQuoted(text, index);
          continue;
        }

        if (c == '(') {
          depth++;
        } else if (c == ')') {
          depth--;
          if (depth == 0) {
            index++;
            return text.substring(start, index - 1);
          }
        }
        index++;
      }
      return null;
    }
  }
}
