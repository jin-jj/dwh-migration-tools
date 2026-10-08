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

import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.copyWithoutNulls;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.formatForeignKeyDetails;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.formatFunctionDataType;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.formatMask;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.formatNullable;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.formatPrimaryKeyDetails;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.formatRoutineLanguage;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.formatSqlDataAccess;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.formatViewCatalogAndNamespace;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.parseColumnClauses;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.parseViewQueryOutputColumns;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.quoteIfNeeded;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.sortColumns;
import static com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.toFunctionParameters;
import static org.junit.Assert.assertEquals;

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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.edwmigration.dumper.application.dumper.connector.databricks.DatabricksUnityCatalogFormatter.ColumnClauses;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DatabricksUnityCatalogFormatterTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  @Test
  public void formatNullable_writesNoOnlyForColumnsThatAreNotNullable() {
    assertEquals("NO", formatNullable(false));
    assertEquals("YES", formatNullable(true));
    assertEquals("YES", formatNullable(null));
  }

  @Test
  public void formatSqlDataAccess_writesTheClauseOfCreateFunction() {
    assertEquals("READS SQL DATA", formatSqlDataAccess(FunctionInfoSqlDataAccess.READS_SQL_DATA));
    assertEquals("", formatSqlDataAccess(null));
  }

  @Test
  public void sortColumns_keepsTheOrderOfColumnsWithoutAPositionAndPutsThemLast() {
    List<ColumnInfo> columns =
        Arrays.asList(
            column("x", null),
            column("c", 2L),
            null,
            column("a", 0L),
            column("y", null),
            column("b", 1L));

    List<String> names = new ArrayList<>();
    for (ColumnInfo column : sortColumns(columns)) {
      names.add(column.getName());
    }

    assertEquals(ImmutableList.of("a", "b", "c", "x", "y"), names);
    assertEquals(ImmutableList.of(), sortColumns(null));
  }

  @Test
  public void parseColumnClauses_readsAnIdentityThatAllowsExplicitInserts() throws IOException {
    ColumnClauses clauses =
        parseColumnClauses(
            typeJson(
                ImmutableMap.of(
                    "delta.identity.start", 100,
                    "delta.identity.step", -1,
                    "delta.identity.allowExplicitInsert", true)));

    assertEquals("BY DEFAULT", clauses.identityGeneration);
    assertEquals("100", clauses.identityStart);
    assertEquals("-1", clauses.identityIncrement);
    assertEquals("", clauses.defaultExpression);
    assertEquals("", clauses.generationExpression);
  }

  @Test
  public void parseColumnClauses_returnsEmptyClausesForTypeJsonWithoutClauses() throws IOException {
    for (String typeJson :
        Arrays.asList(
            null,
            "",
            "not json",
            typeJson(ImmutableMap.of()),
            typeJson(ImmutableMap.of("CURRENT_DEFAULT", ImmutableMap.of("nested", "value"))),
            typeJson(ImmutableMap.of("delta.identity.step", 1)))) {
      ColumnClauses clauses = parseColumnClauses(typeJson);

      assertEquals(typeJson, "", clauses.defaultExpression);
      assertEquals(typeJson, "", clauses.generationExpression);
      assertEquals(typeJson, "", clauses.identityGeneration);
      assertEquals(typeJson, "", clauses.identityStart);
      assertEquals(typeJson, "", clauses.identityIncrement);
    }
  }

  @Test
  public void formatMask_writesTheArgumentsOfTheMaskFunction() throws IOException {
    ColumnMask mask =
        new ColumnMask()
            .setFunctionName("main.security.mask")
            .setUsingArguments(
                ImmutableList.of(
                    new PolicyFunctionArgument().setColumn("region"),
                    new PolicyFunctionArgument().setConstant("10")));

    assertEquals(
        ImmutableMap.of(
            "function",
            "main.security.mask",
            "usingColumns",
            ImmutableList.of(),
            "usingArguments",
            ImmutableList.of(
                ImmutableMap.of("column", "region"), ImmutableMap.of("constant", "10"))),
        parseJsonObject(formatMask(mask)));
    assertEquals("", formatMask(null));
  }

  @Test
  public void formatViewCatalogAndNamespace_quotesPartsThatAreNotPlainNames() {
    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            "view.catalogAndNamespace.numParts", "4",
            "view.catalogAndNamespace.part.0", "spark_catalog",
            "view.catalogAndNamespace.part.1", "2024",
            "view.catalogAndNamespace.part.2", "a`b",
            "view.catalogAndNamespace.part.3", "sales-eu");

    assertEquals(
        "spark_catalog.`2024`.`a``b`.`sales-eu`", formatViewCatalogAndNamespace(properties));
  }

  @Test
  public void formatViewCatalogAndNamespace_returnsEmptyForIncompleteParts() {
    assertEquals(
        "",
        formatViewCatalogAndNamespace(
            ImmutableMap.of(
                "view.catalogAndNamespace.numParts", "2",
                "view.catalogAndNamespace.part.0", "main")));
    assertEquals(
        "",
        formatViewCatalogAndNamespace(ImmutableMap.of("view.catalogAndNamespace.numParts", "x")));
    assertEquals("", formatViewCatalogAndNamespace(null));
  }

  @Test
  public void parseViewQueryOutputColumns_readsTheColumnsInOrder() {
    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            "view.query.out.col.1", "name",
            "view.query.out.numCols", "2",
            "view.query.out.col.0", "id");

    assertEquals(ImmutableList.of("id", "name"), parseViewQueryOutputColumns(properties));
    assertEquals(
        ImmutableList.of(),
        parseViewQueryOutputColumns(ImmutableMap.of("view.query.out.numCols", "1")));
  }

  @Test
  public void quoteIfNeeded_keepsPlainNamesUnquoted() {
    assertEquals("sales_2024", quoteIfNeeded("sales_2024"));
    assertEquals("`2024`", quoteIfNeeded("2024"));
    assertEquals("`my table`", quoteIfNeeded("my table"));
  }

  @Test
  public void toFunctionParameters_sortsTheParametersByPosition() {
    FunctionParameterInfos parameterInfos =
        new FunctionParameterInfos()
            .setParameters(
                ImmutableList.of(
                    new FunctionParameterInfo().setName("b").setTypeText("INT").setPosition(1L),
                    new FunctionParameterInfo()
                        .setName("a")
                        .setTypeText("STRING")
                        .setPosition(0L)));

    ImmutableList<DatabricksFunctionParameter> parameters = toFunctionParameters(parameterInfos);

    assertEquals(2, parameters.size());
    assertEquals("a", parameters.get(0).name);
    assertEquals("STRING", parameters.get(0).type);
    assertEquals("b", parameters.get(1).name);
    assertEquals(ImmutableList.of(), toFunctionParameters(null));
  }

  @Test
  public void formatFunctionDataType_writesTheFullDataTypeOfAScalarFunction() {
    FunctionInfo function = new FunctionInfo().setFullDataType("ARRAY<STRING>");

    assertEquals("ARRAY<STRING>", formatFunctionDataType(function, ImmutableList.of()));
    assertEquals("", formatFunctionDataType(new FunctionInfo(), ImmutableList.of()));
  }

  @Test
  public void formatRoutineLanguage_writesTheLanguageInUpperCase() {
    assertEquals(
        "SQL",
        formatRoutineLanguage(
            new FunctionInfo()
                .setRoutineBody(FunctionInfoRoutineBody.SQL)
                .setExternalLanguage("x")));
    assertEquals(
        "PYTHON",
        formatRoutineLanguage(
            new FunctionInfo()
                .setRoutineBody(FunctionInfoRoutineBody.EXTERNAL)
                .setExternalLanguage("Python")));
    assertEquals("", formatRoutineLanguage(new FunctionInfo()));
  }

  @Test
  public void formatPrimaryKeyDetails_writesTheTimeseriesColumns() throws IOException {
    PrimaryKeyConstraint primaryKey =
        new PrimaryKeyConstraint()
            .setName("events_pk")
            .setChildColumns(ImmutableList.of("device", "ts"))
            .setTimeseriesColumns(ImmutableList.of("ts"));

    assertEquals(
        ImmutableMap.of(
            "columns",
            ImmutableList.of("device", "ts"),
            "timeseriesColumns",
            ImmutableList.of("ts")),
        parseJsonObject(formatPrimaryKeyDetails(primaryKey)));
  }

  @Test
  public void formatForeignKeyDetails_writesRely() throws IOException {
    ForeignKeyConstraint foreignKey =
        new ForeignKeyConstraint()
            .setName("orders_customers_fk")
            .setChildColumns(ImmutableList.of("customer_id"))
            .setParentTable("main.sales.customers")
            .setParentColumns(ImmutableList.of("id"))
            .setRely(false);

    assertEquals(
        ImmutableMap.of(
            "columns",
            ImmutableList.of("customer_id"),
            "referencedTable",
            "main.sales.customers",
            "referencedColumns",
            ImmutableList.of("id"),
            "rely",
            false),
        parseJsonObject(formatForeignKeyDetails(foreignKey)));
  }

  @Test
  public void copyWithoutNulls_dropsNullElements() {
    assertEquals(ImmutableList.of("a", "b"), copyWithoutNulls(Arrays.asList("a", null, "b")));
    assertEquals(ImmutableList.of(), copyWithoutNulls(null));
  }

  private static ColumnInfo column(String name, Long position) {
    return new ColumnInfo().setName(name).setPosition(position);
  }

  /** Returns the {@code type_json} of a column with the metadata. */
  private static String typeJson(Map<String, ?> metadata) throws IOException {
    return JSON.writeValueAsString(
        ImmutableMap.of("name", "c", "type", "long", "nullable", true, "metadata", metadata));
  }

  private static Map<String, Object> parseJsonObject(String json) throws IOException {
    return JSON.readValue(json, new TypeReference<Map<String, Object>>() {});
  }
}
