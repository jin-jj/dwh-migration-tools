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
 * Parses Databricks {@code SHOW TABLE EXTENDED}, {@code DESCRIBE SCHEMA EXTENDED}, and {@code
 * DESCRIBE FUNCTION EXTENDED} text outputs for {@code hive_metastore}.
 */
final class DatabricksHiveMetastoreParser {

  private static final Pattern TOP_LEVEL_SCHEMA_FIELD_PATTERN =
      Pattern.compile("^\\s\\|--\\s+([^:]+):\\s+(.+?)\\s*\\(nullable\\s*=\\s*(true|false)\\)\\s*$");

  /**
   * Matches {@code Key: value}. {@code DOTALL} lets the value span lines, because {@code DESCRIBE
   * FUNCTION EXTENDED} returns a multi-line {@code Body} as one cell with embedded newlines.
   */
  private static final Pattern KEY_VALUE_LINE_PATTERN =
      Pattern.compile("^([A-Za-z][A-Za-z0-9 _-]*):(?:\\s*(.*))?$", Pattern.DOTALL);

  private DatabricksHiveMetastoreParser() {}

  static final class ParsedColumn {
    final int ordinalPosition;
    final String columnName;
    final String dataType;
    final String isNullable;
    final String comment;
    final String partitionIndex;

    ParsedColumn(
        int ordinalPosition,
        String columnName,
        String dataType,
        String isNullable,
        String comment,
        String partitionIndex) {
      this.ordinalPosition = ordinalPosition;
      this.columnName = columnName;
      this.dataType = dataType;
      this.isNullable = isNullable;
      this.comment = comment;
      this.partitionIndex = partitionIndex;
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
    final String updatedAt;
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
        String updatedAt,
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
      this.updatedAt = updatedAt;
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

  @Nonnull
  static ParsedTableExtended parseShowTableExtended(
      @Nonnull String defaultSchema,
      @Nonnull String defaultTable,
      @CheckForNull String informationBlob) {
    String catalog = DatabricksFilter.HIVE_METASTORE_CATALOG;
    String schema = defaultSchema;
    String table = defaultTable;
    String type = "";
    String provider = "";
    String location = "";
    String comment = "";
    String owner = "";
    String createdTime = "";
    String viewText = null;
    List<String> partitionCols = new ArrayList<>();
    List<ParsedColumn> rawCols = new ArrayList<>();
    List<ParsedTableConstraint> constraints = new ArrayList<>();

    if (informationBlob != null && !informationBlob.isEmpty()) {
      String[] lines = informationBlob.split("\r?\n");
      int i = 0;
      while (i < lines.length) {
        String line = lines[i];
        if (line.trim().equals("Schema: root")) {
          i++;
          int ordinal = 0;
          while (i < lines.length) {
            String schemaLine = lines[i];
            if (!schemaLine.startsWith(" ")) {
              break;
            }
            Matcher colMatcher = TOP_LEVEL_SCHEMA_FIELD_PATTERN.matcher(schemaLine);
            if (colMatcher.matches()) {
              String colName = colMatcher.group(1).trim();
              String colType = colMatcher.group(2).trim();
              boolean nullable = Boolean.parseBoolean(colMatcher.group(3));
              rawCols.add(
                  new ParsedColumn(ordinal, colName, colType, nullable ? "YES" : "NO", "", ""));
              ordinal++;
            }
            i++;
          }
          continue;
        }

        Matcher kv = KEY_VALUE_LINE_PATTERN.matcher(line);
        if (kv.matches()) {
          String key = kv.group(1).trim();
          String val = kv.group(2) != null ? kv.group(2).trim() : "";
          if ("Catalog".equalsIgnoreCase(key) && !val.isEmpty()) {
            catalog = val;
          } else if (("Database".equalsIgnoreCase(key) || "Namespace".equalsIgnoreCase(key))
              && !val.isEmpty()) {
            schema = val;
          } else if ("Table".equalsIgnoreCase(key) && !val.isEmpty()) {
            table = val;
          } else if ("Type".equalsIgnoreCase(key)) {
            type = val;
          } else if ("Provider".equalsIgnoreCase(key)) {
            provider = val.toUpperCase(Locale.ROOT);
          } else if ("Location".equalsIgnoreCase(key)) {
            location = val;
          } else if ("Comment".equalsIgnoreCase(key)) {
            comment = val;
          } else if ("Owner".equalsIgnoreCase(key)) {
            owner = val;
          } else if ("Created Time".equalsIgnoreCase(key)) {
            createdTime = val;
          } else if ("Partition Columns".equalsIgnoreCase(key)) {
            partitionCols = parsePartitionColumnsList(val);
          } else if ("Table Properties".equalsIgnoreCase(key)) {
            parseDeltaCheckConstraintsFromProperties(val, constraints);
          } else if ("Table Constraints".equalsIgnoreCase(key)) {
            parseTableConstraintsList(val, constraints);
          } else if ("View Text".equalsIgnoreCase(key)) {
            StringBuilder viewSb = new StringBuilder(val);
            while (i + 1 < lines.length) {
              String nextLine = lines[i + 1];
              if (nextLine.trim().equals("Schema: root")
                  || KEY_VALUE_LINE_PATTERN.matcher(nextLine).matches()) {
                break;
              }
              viewSb.append("\n").append(nextLine);
              i++;
            }
            viewText = viewSb.toString();
          }
        }
        i++;
      }
    }

    Map<String, Integer> partitionIndexByLowerCol = new LinkedHashMap<>();
    for (int idx = 0; idx < partitionCols.size(); idx++) {
      partitionIndexByLowerCol.put(partitionCols.get(idx).toLowerCase(Locale.ROOT), idx);
    }

    ImmutableList.Builder<ParsedColumn> finalCols = ImmutableList.builder();
    for (ParsedColumn col : rawCols) {
      Integer pIdx = partitionIndexByLowerCol.get(col.columnName.toLowerCase(Locale.ROOT));
      finalCols.add(
          new ParsedColumn(
              col.ordinalPosition,
              col.columnName,
              col.dataType,
              col.isNullable,
              col.comment,
              pIdx != null ? String.valueOf(pIdx) : ""));
    }

    return new ParsedTableExtended(
        catalog,
        schema,
        table,
        type,
        provider,
        location,
        comment,
        owner,
        createdTime,
        "",
        viewText,
        finalCols.build(),
        ImmutableList.copyOf(constraints));
  }

  private static void parseDeltaCheckConstraintsFromProperties(
      String rawProps, List<ParsedTableConstraint> out) {
    if (rawProps == null) {
      return;
    }
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

  private static void parseTableConstraintsList(
      String rawConstraints, List<ParsedTableConstraint> out) {
    if (rawConstraints == null) {
      return;
    }
    String stripped = rawConstraints.trim();
    if (stripped.startsWith("[") && stripped.endsWith("]")) {
      stripped = stripped.substring(1, stripped.length() - 1);
    }
    if (stripped.isEmpty()) {
      return;
    }
    for (String item : splitTopLevelEntries(stripped)) {
      String trimmed = item.trim();
      if (trimmed.startsWith("(") && trimmed.endsWith(")") && trimmed.length() > 2) {
        trimmed = trimmed.substring(1, trimmed.length() - 1);
      }
      int commaIdx = trimmed.indexOf(',');
      if (commaIdx <= 0) {
        continue;
      }
      String name = trimmed.substring(0, commaIdx).trim();
      String def = trimmed.substring(commaIdx + 1).trim();
      String type = "CONSTRAINT";
      String upperDef = def.toUpperCase(Locale.ROOT);
      if (upperDef.startsWith("PRIMARY KEY")) {
        type = "PRIMARY KEY";
      } else if (upperDef.startsWith("FOREIGN KEY")) {
        type = "FOREIGN KEY";
      } else if (upperDef.startsWith("CHECK")) {
        type = "CHECK";
      }
      out.add(new ParsedTableConstraint(name, type, def));
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
    if (raw == null) {
      return result;
    }
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

  @Nonnull
  static ParsedFunctionExtended parseDescribeFunctionExtended(
      @Nonnull String catalogName,
      @Nonnull String schemaName,
      @Nonnull String functionName,
      @Nonnull List<String> lines) {
    String funcType = "";
    String dataType = "";
    String inputParams = "";
    String body = null;
    String className = null;
    String comment = "";
    String owner = "";

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
          StringBuilder inSb = new StringBuilder(val);
          while (i + 1 < lines.size()) {
            String next = lines.get(i + 1);
            if (next == null || KEY_VALUE_LINE_PATTERN.matcher(next).matches()) {
              break;
            }
            if (inSb.length() > 0) {
              inSb.append(", ");
            }
            inSb.append(next.trim());
            i++;
          }
          String rawInput = inSb.toString();
          inputParams = "()".equals(rawInput) ? "" : rawInput;
        } else if ("Returns".equalsIgnoreCase(key)) {
          StringBuilder retSb = new StringBuilder(val.replaceAll("\\s+", " ").trim());
          while (i + 1 < lines.size()) {
            String next = lines.get(i + 1);
            if (next == null || KEY_VALUE_LINE_PATTERN.matcher(next).matches()) {
              break;
            }
            String trimmedNext = next.replaceAll("\\s+", " ").trim();
            if (!trimmedNext.isEmpty()) {
              if (retSb.length() > 0) {
                retSb.append(", ");
              }
              retSb.append(trimmedNext);
            }
            i++;
          }
          dataType = retSb.toString();
        } else if ("Comment".equalsIgnoreCase(key)) {
          comment = val;
        } else if ("Owner".equalsIgnoreCase(key)) {
          owner = val;
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

    if ("TABLE".equalsIgnoreCase(funcType)
        && !dataType.isEmpty()
        && !dataType.toUpperCase(Locale.ROOT).startsWith("TABLE(")) {
      dataType = "TABLE(" + dataType + ")";
    }

    String routineDef = "";
    String routineLang = "";
    String ddl = "";
    if (body != null && !body.isEmpty()) {
      routineDef = body;
      routineLang = "SQL";
      StringBuilder ddlSb = new StringBuilder();
      ddlSb
          .append("CREATE FUNCTION ")
          .append(catalogName)
          .append(".")
          .append(schemaName)
          .append(".")
          .append(functionName)
          .append("(")
          .append(inputParams)
          .append(")");
      if (!dataType.isEmpty()) {
        ddlSb.append("\nRETURNS ").append(dataType);
      }
      if (!comment.isEmpty()) {
        ddlSb.append("\nCOMMENT '").append(comment.replace("'", "\\'")).append("'");
      }
      ddlSb.append("\nRETURN ").append(body);
      ddl = ddlSb.toString();
    } else if (className != null && !className.isEmpty()) {
      routineDef = className;
      routineLang = "JAVA";
      ddl =
          "CREATE FUNCTION "
              + catalogName
              + "."
              + schemaName
              + "."
              + functionName
              + " AS '"
              + className.replace("'", "\\'")
              + "'";
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
}
