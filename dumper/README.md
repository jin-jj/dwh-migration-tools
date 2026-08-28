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

## Supported Connectors

### Databricks Connector

The Databricks connector extracts metadata from Databricks Unity Catalog and legacy Hive Metastore (`hive_metastore`).

#### Prerequisites
- Workspace URL (e.g. `https://<workspace-host>`).
- Personal Access Token (PAT) or OAuth token. Can be provided via `--password <token>`, standard Databricks environment variables (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`), or `~/.databrickscfg`.
- (Optional) SQL Warehouse ID via `--warehouse <id>` to extract legacy `hive_metastore` metadata.

#### Examples

Dump Unity Catalog metadata:
```bash
./bin/dwh-dumper --connector databricks --url https://<workspace-host> --password <token>
```

Dump specific catalogs and schemas:
```bash
./bin/dwh-dumper --connector databricks --url https://<workspace-host> --database catalog1,catalog2 --schema schema1,schema2
```

Dump both Unity Catalog and legacy Hive Metastore:
```bash
./bin/dwh-dumper --connector databricks --url https://<workspace-host> --warehouse <warehouse-id>
```

[BQMS]: https://cloud.google.com/bigquery/docs/migration-intro

