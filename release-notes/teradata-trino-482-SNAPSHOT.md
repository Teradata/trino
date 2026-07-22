# Teradata connector for Trino 482-SNAPSHOT

The Teradata connector lets Trino query and create tables in an external Teradata database —
enabling federation between Teradata and other systems (Hive, object storage, other Teradata
instances, and more) from a single SQL interface.

This is the initial release of the connector; the notes below summarize its capabilities.

## Highlights

- Query and create tables in Teradata over JDBC, and join Teradata data with other Trino catalogs.
- Full Trino **view management** for Teradata.
- **`MERGE`** support (non-transactional).
- **`query(...)` table function** for pushing native Teradata SQL (e.g. `QUALIFY`) straight to the database.
- Cost-based optimization via Teradata **table/column statistics**, plus join, limit, top-N, and
  aggregate **pushdown**.

## Connectivity & configuration

- Connects via the Teradata JDBC driver using a `connection-url` (default port `1025`), with
  `connection-user` / `connection-password`.
- **TLS** supported by appending `SSLMODE` to the JDBC URL.
- Multiple Teradata catalogs supported (one properties file per catalog).

## SQL support

In addition to Trino's globally-available and read statements, the connector supports:

- `CREATE SCHEMA`, `DROP SCHEMA`
- `CREATE TABLE`, `CREATE TABLE AS`, `DROP TABLE`
- Schema and table management
- View management (see below)
- `MERGE` (non-transactional)

### View management

- Full create / replace / drop / rename / show-definition / comment on views and view columns.
- `CREATE OR REPLACE VIEW` maps to Teradata's native `REPLACE VIEW`.
- View definitions are stored by Trino in a metadata table (`trino_views`) inside a dedicated
  database (default `trino_metadata`, configurable via `teradata.view-metadata-schema`) — not as
  native Teradata views.

### Procedures

- `system.flush_metadata_cache()` — flush the connector's JDBC metadata caches.
- `system.execute('...')` — run a statement directly in Teradata.

### Table functions

- `query(varchar) -> table` — full query pass-through to Teradata, useful for native features like
  the `QUALIFY` clause or for performance.

## Type mapping

Maps Teradata types to their Trino equivalents (and back for writes), including integer types
(`TINYINT`–`BIGINT`), `DECIMAL`/`NUMBER`/`NUMERIC`, `FLOAT`/`DOUBLE`, and character types
(`CHAR`/`VARCHAR`). See the connector documentation for the complete mapping table.

## Performance

- **Table statistics** from Teradata's data dictionary feed Trino's cost-based optimizer.
- **Pushdown** for join, limit, and top-N.
- **Aggregate pushdown** for `avg`, `count`, `max`, `min`, `sum`, `stddev`(+`_pop`/`_samp`),
  `variance`/`var_pop`/`var_samp`, `covar_pop`/`covar_samp`, `corr`, `regr_intercept`, `regr_slope`.
- **Predicate pushdown** for most types, including temporal and `UUID`.

## Known limitations

- **String range predicates are not pushed down.** Range comparisons (`>`, `<`, `BETWEEN`) on
  `CHAR`/`VARCHAR` columns run in Trino to preserve correct ordering; equality/inequality (`=`,
  `IN`, `!=`) on text columns are pushed down.
- **`MERGE` is non-transactional.**
- **Views are not native Teradata objects** — they are stored in the `trino_views` metadata table,
  so tools like BTEQ or Teradata Studio will not show them as database views.

## Artifacts

- `trino-teradata-482-SNAPSHOT.zip` — the Teradata connector plugin. Unzip it into your Trino
  deployment's `plugin/` directory and restart the coordinator and workers.

## Based on

- Upstream Trino **482** (this build tracks the `482-SNAPSHOT` development line).
- Full source is attached below as **Source code (zip / tar.gz)**.
