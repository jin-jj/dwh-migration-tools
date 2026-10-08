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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.base.Preconditions;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * A parameter of a function, or a column of the table that a table function returns, as written to
 * the {@code InputParams} and {@code ReturnParams} JSON arrays of the functions files.
 *
 * <p>An absent {@code mode}, {@code default} or {@code comment} is left out of the JSON object. A
 * parameter whose text could not be split into its parts has an empty {@code name} and the whole
 * text as its {@code type}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"name", "type", "mode", "default", "comment"})
final class DatabricksFunctionParameter {

  @JsonProperty("name")
  final String name;

  @JsonProperty("type")
  final String type;

  /** Such as {@code IN}. */
  @JsonProperty("mode")
  @CheckForNull
  final String mode;

  /** The SQL expression of the {@code DEFAULT} clause. */
  @JsonProperty("default")
  @CheckForNull
  final String defaultExpression;

  @JsonProperty("comment")
  @CheckForNull
  final String comment;

  private DatabricksFunctionParameter(@Nonnull Builder builder) {
    this.name = builder.name;
    this.type = builder.type;
    this.mode = builder.mode;
    this.defaultExpression = builder.defaultExpression;
    this.comment = builder.comment;
  }

  @Nonnull
  static Builder newBuilder(@Nonnull String name, @Nonnull String type) {
    return new Builder(name, type);
  }

  /**
   * Returns the parameter as in {@code CREATE FUNCTION}, such as {@code `x` INT DEFAULT 1 COMMENT
   * 'the input'}, or the type alone if the parameter has no name.
   */
  @Nonnull
  String toSqlDefinition() {
    if (name.isEmpty()) {
      return type;
    }

    StringBuilder definition = new StringBuilder(quoteIdentifier(name)).append(' ').append(type);
    if (defaultExpression != null) {
      definition.append(" DEFAULT ").append(defaultExpression);
    }
    if (comment != null) {
      definition.append(" COMMENT ").append(quoteStringLiteral(comment));
    }
    return definition.toString();
  }

  static final class Builder {

    private final String name;
    private final String type;
    @CheckForNull private String mode;
    @CheckForNull private String defaultExpression;
    @CheckForNull private String comment;

    private Builder(@Nonnull String name, @Nonnull String type) {
      this.name = Preconditions.checkNotNull(name, "Parameter name was null.");
      this.type = Preconditions.checkNotNull(type, "Parameter type was null.");
    }

    /** Sets the mode, such as {@code IN}. A null mode leaves the mode absent. */
    @Nonnull
    Builder setMode(@CheckForNull String mode) {
      this.mode = mode;
      return this;
    }

    /** Sets the {@code DEFAULT} expression. A null expression leaves the default absent. */
    @Nonnull
    Builder setDefaultExpression(@CheckForNull String defaultExpression) {
      this.defaultExpression = defaultExpression;
      return this;
    }

    /** Sets the comment. A null comment leaves the comment absent. */
    @Nonnull
    Builder setComment(@CheckForNull String comment) {
      this.comment = comment;
      return this;
    }

    @Nonnull
    DatabricksFunctionParameter build() {
      return new DatabricksFunctionParameter(this);
    }
  }
}
