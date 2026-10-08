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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksHiveMetastoreFunctionParser.ParsedFunction;
import javax.annotation.Nonnull;

/**
 * Builds the {@code CREATE FUNCTION} statements of {@code hive_metastore} functions from their
 * {@code DESCRIBE FUNCTION EXTENDED} values, because Databricks has no {@code SHOW CREATE
 * FUNCTION}.
 */
final class DatabricksHiveMetastoreFunctionDdlFormatter {

  private static final String SQL_LANGUAGE = "SQL";

  /** The SQL data access values that {@code CREATE FUNCTION} accepts as a clause. */
  private static final ImmutableSet<String> SQL_DATA_ACCESS_CLAUSES =
      ImmutableSet.of("CONTAINS SQL", "READS SQL DATA");

  /** The delimiter of a body in {@code AS $$ ... $$}. */
  private static final String BODY_DELIMITER = "$$";

  private DatabricksHiveMetastoreFunctionDdlFormatter() {}

  /**
   * Returns the {@code CREATE FUNCTION} statement of a function with a body or a class, or an empty
   * string for a function with neither.
   */
  @Nonnull
  static String formatCreateFunction(@Nonnull ParsedFunction function) {
    String qualifiedName =
        quoteIdentifier(function.functionCatalog)
            + "."
            + quoteIdentifier(function.functionSchema)
            + "."
            + quoteIdentifier(function.functionName);
    if (!function.routineDefinition.isEmpty()) {
      return formatCreateFunctionWithBody(qualifiedName, function);
    }

    if (!function.className.isEmpty()) {
      return "CREATE FUNCTION " + qualifiedName + " AS " + quoteStringLiteral(function.className);
    }

    return "";
  }

  @Nonnull
  private static String formatCreateFunctionWithBody(
      @Nonnull String qualifiedName, @Nonnull ParsedFunction function) {
    ImmutableList.Builder<String> parameters = ImmutableList.builder();
    for (DatabricksFunctionParameter parameter : function.inputParams) {
      parameters.add(parameter.toSqlDefinition());
    }

    StringBuilder ddl =
        new StringBuilder("CREATE FUNCTION ")
            .append(qualifiedName)
            .append('(')
            .append(String.join(", ", parameters.build()))
            .append(')');
    if (!function.dataType.isEmpty()) {
      ddl.append("\nRETURNS ").append(function.dataType);
    }

    boolean isSqlFunction = SQL_LANGUAGE.equals(function.routineLanguage);
    if (!isSqlFunction) {
      ddl.append("\nLANGUAGE ").append(function.routineLanguage);
    }

    if (Boolean.TRUE.toString().equals(function.isDeterministic)) {
      ddl.append("\nDETERMINISTIC");
    } else if (Boolean.FALSE.toString().equals(function.isDeterministic)) {
      ddl.append("\nNOT DETERMINISTIC");
    }

    if (SQL_DATA_ACCESS_CLAUSES.contains(function.sqlDataAccess)) {
      ddl.append('\n').append(function.sqlDataAccess);
    }

    if (!function.handler.isEmpty()) {
      ddl.append("\nHANDLER ").append(quoteStringLiteral(function.handler));
    }

    if (!function.comment.isEmpty()) {
      ddl.append("\nCOMMENT ").append(quoteStringLiteral(function.comment));
    }

    String body = function.routineDefinition;
    if (isSqlFunction) {
      ddl.append("\nRETURN ").append(body);
    } else if (body.contains(BODY_DELIMITER)) {
      ddl.append("\nAS ").append(quoteStringLiteral(body));
    } else {
      ddl.append("\nAS ")
          .append(BODY_DELIMITER)
          .append('\n')
          .append(body)
          .append('\n')
          .append(BODY_DELIMITER);
    }
    return ddl.toString();
  }
}
