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

/** Names of the catalogs Databricks provisions itself, which need special treatment. */
final class DatabricksCatalogNames {

  /** The legacy metastore. Not a Unity Catalog securable, and it has no information schema. */
  static final String HIVE_METASTORE = "hive_metastore";

  /** Databricks-owned sample datasets. Never part of a customer's workload. */
  static final String SAMPLES = "samples";

  /** Holds the metastore-wide system tables, including {@code system.information_schema}. */
  static final String SYSTEM = "system";

  private DatabricksCatalogNames() {}
}
