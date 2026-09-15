# Databricks metadata connector — design

**Status:** implemented, on branch `databricks-hardening` (12 commits off `main@8f6a2e88`) **Author:** jinjj@ **Tests:** 636 pass (77 Databricks-specific)

---

## 1\. Summary

A `dwh-migration-dumper` connector that extracts Databricks metadata — catalogs, schemas, tables, columns, views, constraints and functions — into the CSV bundle the downstream migration tooling consumes.

The central design problem is that **no single Databricks API returns all of this**. The connector therefore runs a three-tier fallback over two transports (SQL and REST), plus a dedicated path for the legacy `hive_metastore`, which neither tier can reach normally.

## 2\. Goals

- Extract translation-grade metadata: enough to rewrite DDL and queries against a target warehouse.  
- Work across the range of workspace configurations we cannot control — system schema disabled, under-privileged principals, no SQL warehouse, legacy Hive Metastore still in use.  
- Degrade **loudly**. A dump that silently produces empty files is worse than one that fails.  
- Stream to disk. Metastores are large and a dump may run for hours.

## 3\. Non-goals

- **Query history, billing and compute telemetry.** Covered today by the Databricks Discovery Notebook (see §10).  
- **Lineage.** The lineage API is rate-limited to 300 requests/hour account-wide, which does not scale to a metastore walk.  
- **Spark OSS / Thrift Server variants.** Possible later; the tier abstraction leaves room.

---

## 4\. Why three tiers

Neither transport is sufficient alone. This is the capability split that forced the design:

&nbsp;

&nbsp;

| Capability | SQL (`information_schema`) | UC REST API |
| :---- | :---- | :---- |
| **Whole metastore in one query** | **Yes —** `system.information_schema` | **No — requires catalog+schema per call** |
| **Table constraints** | **Yes** | **No — write-only endpoints; N+1 via per-table get** |
| `hive_metastore` | **Yes** | **No — not a UC securable** |
| **Works without a SQL warehouse** | **No** | **Yes** |
| **Works when system schema is disabled** | **Partly — per-catalog only** | **Yes** |
| `storage_location` | **No —** `storage_sub_directory` **is discontinued** | **Yes** |
| **Delta table properties / features** | **No** | **Yes** |

**Note —** The cost asymmetry is severe. On a metastore of 10 catalogs / 500 schemas / 25,000 tables, the REST tier needs roughly **27,500 requests (\~23 minutes)**, of which \~21 minutes is the constraints N+1. The SQL equivalent is **7 queries**. REST is a fallback, not a peer.

### Tier order

&nbsp;

&nbsp;

| Order | Tier | Source | Runs when |
| :---- | :---- | :---- | :---- |
| 1 | system | `system.information_schema` — one query, whole metastore | Always (default strategy) |
| 2 | catalog | `<catalog>.information_schema` — one query per catalog | Tier 1 failed |
| 3 | rest | Unity Catalog REST API — no warehouse needed | Tiers 1 and 2 both failed |
| — | hive\_metastore | `SHOW TABLE EXTENDED` per schema | Always, unless skipped |

Each tier writes **the same target path**, and tiers 2 and 3 are gated with `onlyIfFailed` / `onlyIfAllFailed`. At most one contributes. `hive_metastore` is orthogonal and always runs alongside (unless skipped), writing separate `*-hms.csv` entries.

This is expressed in `DatabricksInput`, which also exposes narrower strategies (`system-only`, `catalog-only`, `rest-only`) for operators who know their environment.

---

## 5\. Key design decisions

### 5.1 Failures propagate; they are never swallowed

The original prototype logged and continued on query failure. That produced an empty CSV *and* — because the task still reported success — **pinned the fallback chain shut**: `onlyIfFailed` never fired, so tier 2 never ran.

Every helper now throws. `DatabricksSqlHelper.executeQueryOrThrow` is named to make the contract impossible to misread. Tier 2 additionally reports **total** failure: if every catalog fails (e.g. the principal has no `SELECT` anywhere), the task fails rather than writing seven empty files and declaring victory.

**Important —** The task is the failure boundary. One task failing must not abort the dump, but it must be *visible* — otherwise the fallback design is inert.

### 5.2 Bulk results travel as JSON\_ARRAY, not CSV

Non-obvious, and I got it wrong once before fixing it. Two documented Databricks behaviours rule CSV out for the `EXTERNAL_LINKS` transport:

- *"Only the first chunk in the result would contain a header row with column names."* A reader that skips a header per chunk **loses one data row per chunk**; one that never skips **emits column names as data**. Both are silent corruption.  
- CSV encodes SQL `NULL` as the literal `null`, indistinguishable from the string `"null"`.

`JSON_ARRAY` is supported with `EXTERNAL_LINKS` (not INLINE-only, as is commonly assumed) and has neither problem.

### 5.3 Transport selection: INLINE vs EXTERNAL\_LINKS

`INLINE` is capped at 25 MiB, beyond which Databricks **cancels the statement**. Small `SHOW` commands use it; anything metastore-wide uses `EXTERNAL_LINKS`. The `hive_metastore` scan also uses `EXTERNAL_LINKS` despite returning few rows, because each row carries a ≥1 KB descriptor blob.

### 5.4 The REST tier walks the metastore once

Tables, columns, views and constraints all read identical `/tables/list` responses, differing only in projection — four full walks.

Sharing them collided with the framework's one-file-per-task model. A task *can* write several zip entries (`TaskRunContext.createOutputHandle` allows it), but fanning out would break the per-file condition gating the whole fallback design rests on.

**Resolution:** the walk is not a task. It is a temporary JSONL file, built lazily by whichever task runs first and owned by `DatabricksHandle`, which deletes it on close. `Handle` is already the connector's shared thread-safe object, so this is where shared state belongs. Columns are always requested, because fetching the union once beats a second walk.

Saves \~1,500 requests and three-quarters of the tier's listing time.

### 5.5 hive\_metastore is parsed from one blob per schema

`hive_metastore` has no `information_schema`. The prototype issued `DESCRIBE TABLE EXTENDED` per table (N+1) and classified views by searching the DDL text for the substring `VIEW`.

It now issues one `SHOW TABLE EXTENDED IN <schema> LIKE '*'` per schema and parses the returned descriptor blob — a newline-delimited document carrying `Type`, `Provider`, `Location`, `Owner`, `Created Time`, `Partition Columns`, a `Schema: root` tree with nested struct/array types, and `View Text`.

The parser is pinned by golden tests against **real production output** from a live workspace. That output settled three things documentation did not:

- Nullability *is* available (`nullable = false` appears in the schema tree).  
- Unpartitioned tables **omit** `Partition Columns:` entirely rather than emitting it empty.  
- `Created Time` uses English day/month names, so parsing requires `Locale.US` — `Locale.ROOT` fails.

### 5.6 Retry is layered, and the layers do different jobs

The SDK already retries 429 with `maxAttempts = 4`, honoring `Retry-After` and jittering when absent. Our outer loops **cannot** honor the header — `DatabricksError` exposes only a status and an error code — and do not need to; they exist to extend the budget across a long dump.

The genuine gap was the **pre-signed chunk download**, the only request the connector issues itself. It had *no retry at all*: one 503 from the object store discarded an extraction already minutes in. It now retries, and being our own HTTP, it is the one place `Retry-After` is visible, so it honors it (both delta-seconds and HTTP-date forms). Non-429 4xx still fails on the first attempt — a rejected pre-signed link will not start working.

All backoffs use **equal jitter** (`half + random(0, half)`). Full jitter decorrelates marginally better but can return a near-zero delay, which is the wrong response to an endpoint that just reported overload. Jitter matters because a dump runs tasks concurrently against one workspace, which throttles them together.

### 5.7 The filter is rendered twice, from one source

`--database` and `--schema` have to be applied in two different ways. A task querying an `information_schema` view should express them as a `WHERE` clause. A task reading the REST API, or scraping a `SHOW` command, has no clause available and must test each name as it arrives.

`DatabricksFilter` holds the requested names once and renders both forms. Holding them in one object is the point: `whereClause()` and `matchesCatalog()` are two renderings of the same sets, so the three tiers cannot disagree about what belongs in the dump.

**Case folding.** Unity Catalog stores catalog, schema and table names **lowercased**, converting on creation, while accepting any case everywhere else. The shared `ConnectorArguments.toPredicate` uses `Predicates.in(...)` — exact `String.equals`. So `--database MyCatalog` matched nothing, deterministically, and produced a *successful* dump of header-only CSVs. The fix is local, because `getDatabasePredicate()` is shared by every connector and changing it would alter Snowflake, Oracle and Redshift behaviour.

**Note —** The codebase already contained a partial workaround — `hive_metastore` task selection scanned raw arguments with `equalsIgnoreCase` beside the case-sensitive predicate, fixing one catalog name and leaving the rest broken. That workaround is now removed.

**Pushdown.** Every system-tier statement previously selected the whole metastore and discarded unwanted rows in Java. The statements are ordered, and an ordered result cannot stream, so a one-catalog-in-ten dump made the warehouse read and globally sort ten times the rows it needed before shipping the first byte. At 25,000 tables that is \~500,000 column rows pulled to keep \~50,000.

The reason it was written that way is structural, not semantic: a task only ever saw a `Predicate<String>`, so the names had been erased by the time they reached the code that builds the SQL. `ConnectorArguments.toPredicate` is plain set membership — there are no globs to translate.

This follows the **Snowflake connector**, which has always pushed down. `SnowflakeMetadataConnector` reads `arguments.getDatabases()` directly, renders `column IN ('A','B')`, and substitutes it into a two-slot statement template. It solved the same case problem in the same place, in the opposite direction — Snowflake uppercases unquoted identifiers, Unity Catalog lowercases them.

Three details are worth recording:

* The clause emits `lower(col) IN (...)`, not a bare literal comparison. Spark SQL compares *strings* case-sensitively even though it resolves *identifiers* case-insensitively, and folding the column makes the clause an exact translation of the Java comparison rather than merely an equivalent one.  
* Case folding was a **prerequisite**. Pushing the old case-sensitive filter into SQL would have silently started returning rows the Java side dropped, so the same command would have produced different output depending on which tier ran.  
* Filtering is still applied in Java after the rows arrive. That costs a set lookup per row and means a statement later edited without its placeholder still produces a correctly scoped dump.

The per-catalog tier restricts only the schema, because its loop has already fixed the catalog. `samples` and `system` are now excluded by the statement rather than the row handler, which matters because `system` holds the metastore's own observability tables and is large. The REST tier keeps the predicate, as does the `hive_metastore` tier — `SHOW TABLE EXTENDED` takes no `WHERE`, which is exactly the case where Snowflake also keeps a row predicate (`SHOW EXTERNAL TABLES`).

### 5.8 The dump uses the connector-wide CSV dialect

The three Databricks base tasks each redeclared `FORMAT = CSVFormat.DEFAULT`, shadowing `AbstractTask.FORMAT`. Databricks was the only connector in the repository doing this, so its dump was the only one written with CRLF line endings and no escape character while every other dump used LF and a backslash — a silent divergence in a published format, reaching exactly the fields most likely to contain a quote or newline (`view_definition`, `routine_definition`).

The redeclarations are gone. This was found while surveying the Hive connector for shareable code (§10): it turned out to be the one thing genuinely shared, and the connector had accidentally opted out of it.

### 5.9 The REST tier publishes its raw objects alongside the CSVs

The CSVs are a *projection*. `tables.csv` keeps ten fields; the `TableInfo` the REST API returned carries the full column array, the Delta table properties and features, `type_json`, `securable_kind`, the data-source format details and the storage location. Everything not in the ten columns was being thrown away — after we had already paid for it.

That is tolerable when the CSV is the whole product. It is not tolerable here, because the raw objects were already sitting on disk: §5.4's shared walk spills them as JSONL and then **deletes the file on handle close**. We were fetching full-fidelity metadata, serializing it, reading it back, projecting it, and discarding the original.

**Decision:** when the REST tier runs, the dump also contains `tables-raw.jsonl` — that same listing, copied out verbatim. A consumer that wants more than the assessment needs can read it instead of re-walking the metastore, which for a large workspace is \~25,000 tables' worth of requests it does not have to issue.

Three things had to be true for this to be worth doing:

* **It must be free.** The task extends `AbstractDatabricksRestTask` and asks for the same shared listing, so it either reuses the walk another REST task performed or performs the walk they then reuse. A test asserts that adding it does not produce a second `/tables/list` call.  
* **It must not run when the REST tier does not.** This is the sharp edge. Gated wrong, a normal dump — where tier 1 succeeds and no REST task runs at all — would trigger a full REST metastore walk for this entry alone. `DatabricksInput` therefore gained a second **abstract** method, `restTierTasks`, which each strategy answers for itself: the three strategies that never reach REST return nothing, and the two that do return the task under exactly the condition `tasks()` would have given it.

**\[\!NOTE\] The obvious shortcut — call** `tasks()` **a second time and see whether the REST task comes back — is wrong.** `onlyIfFailed` **and** `onlyIfAllFailed` ***mutate* the task and return** `this`**, so a second call appends a duplicate condition to the per-catalog task. Making the method abstract also means a new strategy cannot be added without answering the question.**

* **It must be honest.** The walk previously set `omit_properties=true`, since no CSV had a column for properties. Publishing an object labelled "raw" with a field silently missing is worse than not publishing it, so the walk now requests properties. Table properties are also among the few things REST can see that SQL cannot, so this closes a real gap rather than merely padding the file. Cost is roughly \+10 MB on a 25,000-table metastore, on the tier that is already the slow one.

The entry is scoped like every other output: it honours `--database` and `--schema`, and excludes `hive_metastore`, which is not a Unity Catalog securable and has no `TableInfo`.

The name follows the Hive connector, which writes its raw Thrift objects to `tables-raw.jsonl` too. That is the §10 conclusion applied: borrow the vocabulary, not the code. One difference — Hive does not declare its raw entries in its format SPI, and ours is declared, because an undeclared zip entry is one no consumer can discover.

Only tables get this treatment for now. Catalogs and schemas are small and their CSVs are nearly lossless; functions could follow if a consumer asks.

---

## 6\. Output contract

&nbsp;

&nbsp;

| Entry | Unity Catalog | Hive Metastore |
| :---- | :---- | :---- |
| `catalogs.csv` / `catalogs-hms.csv` | Yes | Yes |
| `schemata.csv` / `schemata-hms.csv` | Yes | Yes |
| `tables.csv` / `tables-hms.csv` | Yes | Yes |
| `columns.csv` / `columns-hms.csv` | Yes | Yes |
| `views.csv` / `views-hms.csv` | Yes | Yes |
| `table_constraints.csv` | Yes | — |
| `functions.csv` | Yes | — |
| `tables-raw.jsonl` | REST tier only | — |

Plus the framework's `compilerworks-metadata.yaml` and format marker.

All tiers normalize to the same headers. Notable normalizations: `information_schema`'s `YES`/`NO` becomes `true`/`false`; REST's 0-based column `position` becomes a 1-based `OrdinalPosition` to match `information_schema`.

`tables-raw.jsonl` is the one entry that is **not** tier-independent, and deliberately so: it is the SDK's `TableInfo` verbatim, which only the REST tier has. Its shape is the SDK's, not ours, so it is versioned by the SDK rather than by this connector — a consumer should tolerate unknown fields.

## 7\. Configuration

&nbsp;

&nbsp;

| Property | Default | Purpose |
| :---- | :---- | :---- |
| `databricks.metadata.strategy` | `system-then-catalog-then-rest` | Which tiers run and in what order |
| `databricks.rest.requests-per-second` | `20` | REST tier self-throttle |
| `databricks.skip-hive-metastore` | `false` | Skip the legacy path |

Registered connector names: **`databricks`** (primary), plus `databricks-sql`, `databricks-system-metadata` and `databricks-catalog-metadata` as pinned-strategy aliases.

**Important —** The primary name must remain exactly `databricks`: BQMS reserves that string, and `DumperConnector.METADATA_DATABRICKS` is registered `virtual = true`, meaning "does not exist in the dumper yet."

## 8\. Testing

100 tests, no live workspace required. The full suite is 659\.

- **WireMock** for all HTTP: the external-links download path, gzip sniffing, retry and `Retry-After`.  
- **Mockito** against the SDK service interfaces for the REST tier, including a test proving three tasks sharing a handle produce exactly one `/tables/list` call.  
- **Golden tests** over real production `SHOW TABLE EXTENDED` output.  
* The generated `WHERE` clause is pinned, and asserted to **agree with the in-process filter** — a disagreement would not fail loudly, it would produce a dump whose contents depend on which tier ran.  
* Two tests capture the statement actually sent and assert the restriction reaches it and precedes the `ORDER BY`.  
* The raw listing's **gating** is tested directly, not just its content: that it carries the same conditions as the REST tables task, that the SQL tiers are not gated twice by the second `DatabricksInput` call, and that no strategy which skips the REST tier emits it.  
* New regression tests are verified against the **unfixed** code to confirm they actually fail. This has paid for itself twice: once when a workaround was masking the bug under test, and once when the CSV-dialect divergence turned out to be invisible to every existing assertion.

---

## 9\. Known limitations

1. Rows are read **positionally**, not by column name. Mitigated by a guarded accessor and the fact that every SELECT list is ours; the risk is drift, not breakage.  
2. `databricks-sdk-java` is pinned at **0.54.0**; 0.153.0 is current.  
3. No integration test in CI — the Databricks kokoro job is still a notebook-driven stub.  
1. `ConnectorArguments.toPredicate` remains case-sensitive for **every other connector**. This connector routes around it rather than fixing it, which is the right call for a single connector's change but leaves the trap in place.  
2. The REST tier cannot restrict server-side at all: `/catalogs/list` and `/schemas/list` take no filter, so a narrow dump still lists the whole metastore before discarding. Only the SQL tiers benefit from §5.7.

   ## 10\. Relationship to the existing Hive connector

   The repository already dumps a Hive Metastore, via the `hiveql` connector and the generated Thrift module in `dumper/lib-ext-hive-metastore/`. Since this connector also dumps a Hive Metastore, the overlap was surveyed deliberately rather than assumed away.  
   **Decision: share nothing beyond what is already shared.** The two are different products that happen to share a name.

   ### Why the transport cannot be shared

   Databricks exposes no Thrift HMS endpoint to external clients, so `hive_metastore` has to be reached over SQL (`SHOW DATABASES`, `SHOW TABLE EXTENDED`). That much was a given. What the survey settled is that the Thrift client is not merely *a* transport but is baked into the abstraction: `HiveMetastoreThriftClient.Builder.build()` opens the socket itself before choosing an implementation, and **4 of its 11 abstract methods return `org.apache.thrift.TBase`**. `Table.getRawThriftObject()` is `@Nonnull`, which a SQL-backed implementation cannot honour. Its `getTable(db, table)` is also per-table, which is precisely the N+1 shape this connector rejected (§5.5).

   ### Why the output contract cannot be shared

   The two formats are structurally disjoint, not subset and superset:  
   &nbsp;  
   &nbsp;

|  | `hiveql.dump.zip` | `databricks.dump.zip` |
| :---- | :---- | :---- |
| Live table output | `tables.jsonl`, one **nested** object per table | `tables-hms.csv`, **flat** |
| Flat CSV equivalents | `tables.csv`, `columns.csv` — both **`@Deprecated`** | the live path |
| Table type vocabulary | Thrift's `MANAGED_TABLE` / `EXTERNAL_TABLE` / `VIRTUAL_VIEW` | Spark's `MANAGED` / `EXTERNAL` / `VIEW` |
| Timestamps | `createTime`, epoch **seconds** | `CreatedAt`, epoch **milliseconds** |
| Partition columns | `IsPartitionKey`, a **boolean** | `PartitionIndex`, a 1-based **ordinal** |
| Nullability | absent — HMS does not carry it | present, parsed from the schema tree |

   Unifying the headers would be a breaking change to a published format, in service of aesthetics.

   ### Why a common data model is not worth extracting

   Of the 19 scalar fields on the Hive `Table` façade, **7** have a counterpart in `DatabricksHiveMetastoreTable`, and three of those seven disagree on encoding (the last three rows above). A shared model would therefore be mostly nullable and would still need a mapping layer on each side — an indirection added while keeping both mappings.

   There is also nothing to share in the parsing. The Hive side does **no** type parsing at all: Thrift `FieldSchema.getType()` is passed through verbatim. `DatabricksHiveMetastoreTable`'s type rendering exists precisely because Thrift already gives Hive the structure that a `SHOW` command does not. Symmetrically, no other connector in the repository scrapes `SHOW TABLE EXTENDED` or `DESCRIBE FORMATTED`, so there is no prior art to converge on and no third consumer for any abstraction extracted here.

   ### What the survey did change

   One thing, and it was a real bug: the Databricks tasks had accidentally opted out of `AbstractTask.FORMAT`, the one thing genuinely shared across every connector. Fixed in §5.8.

   Going forward the convention is to **borrow vocabulary, not code**: where this connector names something Hive already names (`location`, `owner`, `viewText`, the serde/input/output format triple), it uses the Hive spelling so a reader can line the two up by eye. That is a review convention, costs nothing, and delivers most of what a shared model would.

   **Important —** This analysis inverts if `tables-hms.csv` is ever expected to feed the **same** downstream importer as `hiveql.dump.zip`. In that case the right move is for these tasks to emit `HiveMetadataDumpFormat.TableMetadata` directly, with most fields null, and share nothing else. Confirm which importer consumes the `-hms` entries before this is settled.

   &nbsp;

## 

   ## 11\. Open strategic question: overlap with the Discovery Notebook

The Databricks Discovery Notebook ships today and its ZIP is classified by BQMS exactly like a dumper ZIP. It produces 16 CSVs and 11 JSONLs covering clusters, warehouses, jobs, pipelines, billing and telemetry.

**This connector's outputs overlap with none of them.** A dumper ZIP would classify correctly and load nothing.

The proposed split is that these are complementary rather than competing:

* **Notebook** → *scoping* metadata: assessment, TCO, cluster inventory.  
* **Dumper** → *task-oriented* metadata: DDL and queries for translation and agentic migration.

If that split is accepted, BQMS needs ingestion for the dumper's seven entries. If it is not, the connector needs to grow toward the notebook's file set instead. **This should be settled before the connector is advertised to customers.**

## 12\. Future work

Ranked:

1. Column-name-based row access.  
1. Share the catalog/schema listing (the table walk is already shared; \~45 requests remain).  
2. Fix `toPredicate` upstream, with the other connector owners.  
3. Bump the SDK.  
4. Integration tests in CI.  
5. A separate `databricks-logs` connector for query history, modelled on `SnowflakeLogsConnector`.

&nbsp;