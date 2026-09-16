# BigQuery Migration Service Metadata and Log Dumper

This directory contains the Metadata and Log Dumper, a command line tool for
connecting to an existing database and generating an archive of DDL metadata or
logs. This tool generates archives in a format suitable for consumption by the
[BigQuery Migration Service's][BQMS] Assessment or Translation Service.

The Dumper is a Java tool. **[Download the latest cross-platform release zip `dwh-migration-tools-vX.X.X.zip`.](https://github.com/google/dwh-migration-tools/releases/latest)**

Compiling the Dumper from source requires `Java 8`, running the Dumper requires `Java 8` or higher. To check Java version run the command
`java -version` or refer to Java vendor documentation. Third party JDBC drivers
might impose additional restrictions on Java versions. Refer to the JDBC
driver's manual for details.

To get started using the Dumper, read
[the documentation](https://cloud.google.com/bigquery/docs/generate-metadata).



### Databricks Connector (SQL Warehouse)

The Databricks connector extracts metadata from Databricks Unity Catalog and legacy Hive Metastore (`hive_metastore`) using Databricks SQL Warehouse queries, avoiding HTTP 429 rate limiting on large environments.

#### Extraction Strategy

By default, the connector attempts to query global Unity Catalog system tables (`system.information_schema.*`) first for fast, single-query metadata extraction across all catalogs. If system tables are unavailable or permissions are insufficient, it automatically falls back to per-catalog `<catalog>.information_schema.*` queries.

You can configure the extraction strategy via `-Ddatabricks.metadata.strategy=<STRATEGY>` or by using dedicated connector names:

| Strategy / Connector | Description |
|---|---|
| `SYSTEM_THEN_CATALOG` (default) / `databricks` | Query `system.information_schema` first; fall back to per-catalog queries if system table queries fail. |
| `SYSTEM_ONLY` / `databricks-system-metadata` | Only query `system.information_schema` (fails if system tables are unavailable). |
| `CATALOG_ONLY` / `databricks-catalog-metadata` | Directly query per-catalog `<catalog>.information_schema` without checking `system.information_schema`. |

If per-catalog queries encounter catalogs where the user lacks `USE CATALOG` permission, the connector automatically skips them for subsequent entity queries to avoid repetitive permission errors.

#### Prerequisites
- Workspace URL (e.g. `https://<workspace-host>`).
- Personal Access Token (PAT) or OAuth token. Can be provided via `--password <token>`, standard Databricks environment variables (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`), or `~/.databrickscfg`.
- SQL Warehouse ID via `--warehouse <id>`.

#### Examples

Dump metadata using Databricks SQL Warehouse (default: system tables with fallback to per-catalog):
```bash
./bin/dwh-dumper --connector databricks --url https://<workspace-host> --password <token> --warehouse <warehouse-id>
```

Dump metadata using only system tables:
```bash
./bin/dwh-dumper --connector databricks-system-metadata --url https://<workspace-host> --warehouse <warehouse-id>
```

Dump metadata using only per-catalog queries:
```bash
./bin/dwh-dumper --connector databricks-catalog-metadata --url https://<workspace-host> --warehouse <warehouse-id>
```

Dump specific catalogs and schemas:
```bash
./bin/dwh-dumper --connector databricks --url https://<workspace-host> --warehouse <warehouse-id> --database catalog1,catalog2 --schema schema1,schema2
```

Dump metadata without a SQL warehouse, using only the Unity Catalog REST API:
```bash
./bin/dwh-dumper --connector databricks --url https://<workspace-host> -Ddatabricks.metadata.strategy=rest-only
```

Dump all Unity Catalog metadata, skipping legacy Hive Metastore:
```bash
./bin/dwh-dumper --connector databricks --url https://<workspace-host> --warehouse <warehouse-id> -Ddatabricks.skip-hive-metastore=true
```

#### Permissions

> **A principal that lacks a privilege sees fewer objects, not an error.** Every extraction path filters silently, so a dump taken with incomplete grants succeeds and looks exactly like a dump of a smaller workspace. Check the grants before trusting a small result.

For the SQL paths (the default), the principal needs **`CAN_USE`** on the SQL warehouse — set through the warehouse's Permissions UI or the permissions API; there is no `GRANT ... ON WAREHOUSE` statement.

Unlike operational system tables (`system.query.history`, `system.access.audit`), `information_schema` views require **no explicit `SELECT` grant on `information_schema` itself**. Instead, they implement automatic row-level filtering governed by Unity Catalog privileges on the underlying objects:
* **Tier 1 (`system.information_schema`, the default):** SQL name resolution checks traversal only on `system`. A **Metastore Admin** can read metadata for all catalogs across the metastore automatically, even on user-created catalogs where the admin has not explicitly granted themselves `USE CATALOG`.
* **Tier 2 (`<catalog>.information_schema` fallback) and non-admin principals:** SQL name resolution checks explicit traversal on each target catalog first. If `USE CATALOG` is missing on a catalog, `<catalog>.information_schema` fails immediately with `SQLSTATE: 42501` (which the connector catches, warns on, and skips). For non-admin principals (or when using `catalog-only`), grant per catalog:

```sql
GRANT USE CATALOG ON CATALOG `<catalog>`            TO `<principal>`;
GRANT USE SCHEMA  ON SCHEMA  `<catalog>`.`<schema>` TO `<principal>`;
GRANT SELECT      ON SCHEMA  `<catalog>`.`<schema>` TO `<principal>`;
```

For `rest-only`, no warehouse is needed, but the same Unity Catalog privileges apply — `tables/list` returns only tables the caller owns or has `SELECT` on, and `functions/list` only those it owns or can `EXECUTE`. The principal must also exist in the workspace, and the workspace must be attached to a Unity Catalog metastore.

The legacy `hive_metastore` catalog uses legacy table ACLs rather than Unity Catalog privileges. `READ_METADATA` is enough and is metadata-only:

```sql
GRANT USAGE         ON SCHEMA `hive_metastore`.`<database>` TO `<principal>`;
GRANT READ_METADATA ON SCHEMA `hive_metastore`.`<database>` TO `<principal>`;
```

Do not grant `ANY FILE`; it is not needed here and bypasses table ACLs.

#### Connector properties

| Property | Default | Meaning |
| --- | --- | --- |
| `databricks.metadata.strategy` | `system-then-catalog-then-rest` | Which extraction tiers to run, and in what order. Also accepts `system-then-catalog`, `system-only`, `catalog-only` and `rest-only`. Each tier runs only if the tiers before it failed. |
| `databricks.skip-hive-metastore` | `false` | Skip the legacy `hive_metastore` catalog. |
| `databricks.rest.requests-per-second` | `20` | Ceiling on the request rate of the REST tier. |

#### `--assessment` is not accepted

The connector rejects `--assessment` rather than ignoring it. This connector dumps metadata for migration purposes only and produces none of the assessment file set, so accepting the flag would yield a dump that is labelled as an assessment but cannot be ingested as one — a worse outcome than a clear refusal at startup.

#### Output

The dump contains one CSV per dataset: `catalogs.csv`, `schemata.csv`, `tables.csv`, `columns.csv`, `views.csv`, `table_constraints.csv` and `functions.csv`. Whichever tier succeeds writes the same file, so the columns do not depend on how the metadata was read.

The legacy `hive_metastore` catalog is dumped alongside, into `catalogs-hms.csv`, `schemata-hms.csv`, `tables-hms.csv`, `columns-hms.csv`, `views-hms.csv` and `functions-hms.csv`. It is kept separate because it is read by scraping `SHOW TABLE EXTENDED` rather than by querying an information schema, and the two sources do not agree field for field. `functions-hms.csv` in particular fills only the name and the implementing class: its functions are Hive UDFs, which have no SQL body and resolve their signature reflectively at each call site, so the type, parameter, comment and owner columns are written empty rather than guessed at.

When the REST tier runs, the dump also contains `tables-raw.jsonl`: the Unity Catalog `TableInfo` objects verbatim, one JSON document per line. The CSVs are a projection of these, so the raw entry carries what they drop — the column array, Delta table properties and features, `type_json`, and the securable kind — for consumers that want more than the assessment does. It costs nothing, because the REST tier walks the metastore once and shares the result; it is absent from dumps that never reach the REST tier, because no other tier has these objects to publish.

[BQMS]: https://cloud.google.com/bigquery/docs/migration-intro

