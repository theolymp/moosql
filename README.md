# moo

[![CI](https://github.com/theolymp/moosql/actions/workflows/ci.yml/badge.svg)](https://github.com/theolymp/moosql/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org/)

Copy-on-Write proxy for MariaDB/MySQL — make changes freely, original stays untouched.

## What it does

`moo` sits in front of any MariaDB/MySQL database and intercepts every query. All writes (INSERT, UPDATE, DELETE, DDL) are redirected to a local SQLite overlay — the upstream database is never modified. When a SELECT touches a table that has overlay data, the proxy rewrites the query to merge the base and overlay results via temporary-table injection, so the client sees a consistent merged view.

Think of it as OverlayFS for databases: reads come from the real database, writes go to a local layer, and clients see both merged transparently. When you are done experimenting, run `moo reset` and the overlay is gone, leaving the upstream exactly as it was. Or run `moo apply` to write the overlay changes back into the upstream.

## Quick Start

```bash
# 1. Build
cargo build --release
# Binary: target/release/moo

# 2. Start MariaDB (if not already running)
docker run -d -p 3306:3306 -e MYSQL_ROOT_PASSWORD=secret mariadb:latest

# 3. Start the proxy (listens on :3307, reads upstream on :3306)
./target/release/moo start \
  --upstream=localhost:3306 \
  --listen=localhost:3307 \
  --user=root \
  --password=secret

# 4. Connect through the proxy — identical to connecting to MariaDB directly
mysql -h 127.0.0.1 -P 3307 -u root mydb
```

### Pre-built container image

```bash
# Pull pre-built image from GHCR
docker pull ghcr.io/theolymp/moo:latest

# Run against an upstream MariaDB
docker run --rm -p 3307:3307 \
  ghcr.io/theolymp/moo:latest \
  start --upstream=host.docker.internal:3306 --user=root --password=secret
```

Available tags:
- `:latest` — latest released version
- `:0.1.0`, `:0.1`, `:0` — specific versions and aliases (semver)
- `:edge` — latest master commit
- `:master` — same as `:edge`

The upstream user needs `SELECT` and `CREATE TEMPORARY TABLES` — no write privileges are required:

```sql
GRANT SELECT, CREATE TEMPORARY TABLES ON mydb.* TO 'cow_user'@'%';
```

## CLI Reference

### start

Start the proxy. CLI flags override config file values.

```bash
moo start \
  --upstream=localhost:3306 \
  --listen=localhost:3307 \
  --overlay=./dev-overlay \
  --overlay-name=default \
  --user=root \
  --password=secret \
  --config=cow.toml

# Enable live query logging to stdout
moo start --upstream=localhost:3306 --watch

# Filter watch output to specific operations or table names
moo start --upstream=localhost:3306 --watch --watch-filter=INSERT
moo start --upstream=localhost:3306 --watch --watch-filter=users
```

| Flag | Default | Description |
|------|---------|-------------|
| `--upstream` | `localhost:3306` | Upstream MariaDB/MySQL address |
| `--listen` | `localhost:3307` | Address the proxy listens on |
| `--overlay` | `./dev-overlay` | Path to the overlay base directory |
| `--overlay-name` | `default` | Named overlay to use within the base directory |
| `--user` | — | Database user |
| `--password` | — | Database password |
| `--config` | — | Path to TOML config file |
| `--watch` | off | Enable live query logging to stdout |
| `--watch-filter` | — | Filter watch output (e.g. `INSERT`, `SELECT`, or a table name) |

### status

Show overlay size and dirty table count.

```bash
moo status
moo status --overlay=./dev-overlay
```

### reset

Wipe the overlay. Resets all tables or a single named table.

```bash
moo reset                          # wipe everything
moo reset users                    # wipe only the users table
moo reset --overlay=./dev-overlay users
```

### tables

List every table that has overlay data, with flags indicating whether schema or row data is present.

```bash
moo tables
moo tables --overlay=./dev-overlay
```

### diff

Show what changed in the overlay compared to the base database.

```bash
# Table-level summary (default)
moo diff

# Row-level details
moo diff --verbose

# Output as SQL statements
moo diff --format=sql

# Show old->new values for UPDATEs (requires upstream access)
moo diff --full \
  --upstream=localhost:3306 \
  --user=root \
  --password=secret

# Filter to a specific table or database
moo diff --table=users
moo diff --db=mydb --verbose

moo diff --overlay=./dev-overlay --format=sql
```

| Flag | Description |
|------|-------------|
| `--format` | Output format: `text` (default) or `sql` |
| `--verbose` | Show row-level details instead of table summary |
| `--full` | Fetch base rows and show old→new diff for UPDATEs |
| `--upstream` | Upstream address (required with `--full`) |
| `--user` | Upstream user (required with `--full`) |
| `--password` | Upstream password (required with `--full`) |
| `--table` | Filter to a specific table |
| `--db` | Filter to a specific database |

### apply

Apply overlay changes to the upstream database. Requires write access to the upstream.

```bash
# Preview what would be applied
moo apply \
  --upstream=localhost:3306 \
  --user=root \
  --password=secret \
  --dry-run

# Apply with confirmation prompt
moo apply \
  --upstream=localhost:3306 \
  --user=root \
  --password=secret

# Apply without prompt, then reset the overlay
moo apply \
  --upstream=localhost:3306 \
  --user=root \
  --password=secret \
  --yes \
  --reset

# Apply only a specific database or table
moo apply --upstream=localhost:3306 --user=root --db=mydb
moo apply --upstream=localhost:3306 --user=root --table=users
```

| Flag | Description |
|------|-------------|
| `--upstream` | Upstream MariaDB address (required) |
| `--user` | Upstream database user (required) |
| `--password` | Upstream database password |
| `--dry-run` | Show what would be applied without executing |
| `--yes` | Skip confirmation prompt |
| `--reset` | Reset the overlay after a successful apply |
| `--db` | Apply only this database |
| `--table` | Apply only this table |

### snapshot / restore / snapshots

Save and restore named snapshots of the overlay state.

```bash
# Save current overlay as a snapshot
moo snapshot before-migration
moo snapshot before-migration --force   # overwrite existing

# Restore a snapshot
moo restore before-migration

# List all saved snapshots
moo snapshots

# With a custom overlay path
moo snapshot my-snap --overlay=./dev-overlay
moo restore my-snap --overlay=./dev-overlay
moo snapshots --overlay=./dev-overlay
```

### overlay

Manage named overlays. Each overlay is an independent SQLite layer stored under the base directory. This lets you maintain multiple independent change sets against the same upstream.

```bash
# Create a new empty overlay
moo overlay create feature-x

# List all overlays (active one is marked)
moo overlay list

# Show which overlay is currently active
moo overlay active

# Switch to a different overlay (restart proxy to take effect)
moo overlay switch feature-x

# Copy an overlay as the basis for a new one
moo overlay branch feature-x feature-x-v2

# Merge source overlay into target (reports conflicts, does not auto-resolve)
moo overlay merge feature-x main

# Delete an overlay
moo overlay delete feature-x

# All overlay commands accept --base to override the overlay directory
moo overlay list --base=./dev-overlay
```

### diff-overlays

Compare two overlay directories, like `git diff branch-a branch-b`.

```bash
moo diff-overlays ./dev-overlay/feature-x ./dev-overlay/main

# Filter to a specific database or table
moo diff-overlays ./overlay-a ./overlay-b --db=mydb
moo diff-overlays ./overlay-a ./overlay-b --table=users
```

## Config File

All fields are optional. CLI flags take priority over the config file.

```toml
# cow.toml

[upstream]
host     = "localhost"
port     = 3306
user     = "root"
password = "secret"

[proxy]
listen = "localhost:3307"

[overlay]
path = "./dev-overlay/"
```

Pass the config file with `--config=cow.toml` on the `start` command.

## How It Works

The proxy speaks the MySQL wire protocol (`opensrv-mysql`) so any MySQL-compatible client connects to it unchanged.

**Query routing**

1. Every incoming query is parsed with `sqlparser-rs`.
2. SELECTs that touch only clean (unmodified) tables are passed straight through to the upstream — zero overhead.
3. SELECTs that touch a dirty table are rewritten to merge the base and overlay via a `UNION ALL` with a `NOT IN` filter. Two session-scoped temporary tables are injected into the upstream connection per dirty table:
   - `_cow_meta_<table>` — all overlay PKs (used to exclude overridden base rows)
   - `_cow_temp_<table>` — non-tombstone data rows (inserts + updates)
4. INSERT / UPDATE / DELETE are intercepted and written to SQLite. The upstream never receives them.
5. DDL (CREATE, ALTER, DROP, TRUNCATE) is tracked in SQLite; the upstream schema is unchanged.
6. Stored procedure bodies are fetched, rewritten to use the merged views, and executed as ad-hoc SQL.

**Overlay storage**

One SQLite file per database lives under the overlay directory (`./dev-overlay/default/mydb.db`). Overlay rows use auto-decrement IDs starting from `2^63-1` to avoid collisions with base DB IDs. Named overlays are stored as subdirectories of the base overlay path.

**Session model**

Each client connection gets a dedicated upstream connection. Temporary tables are session-scoped in MariaDB, so isolation between clients is automatic. Multiple `moo` instances can point at the same upstream simultaneously.

```
Client A  -->  moo (:3307)  -->  upstream conn 1  (temp tables for A)
Client B  -->  moo (:3307)  -->  upstream conn 2  (temp tables for B)
```

## Docker

A multi-stage Dockerfile is included. The final image is based on `debian:bookworm-slim` and contains only the binary.

```bash
# Build the image
docker build -t moo .

# Run the proxy container, pointing at an upstream MariaDB
docker run -d \
  -p 3307:3307 \
  -v $(pwd)/dev-overlay:/overlay \
  -e UPSTREAM=mydb-host:3306 \
  moo start \
    --upstream=mydb-host:3306 \
    --listen=0.0.0.0:3307 \
    --overlay=/overlay \
    --user=root \
    --password=secret
```

The default `CMD` is `start --listen=0.0.0.0:3307`. Override it by appending arguments after the image name.

## Supported Operations

| Operation | Status |
|-----------|--------|
| SELECT passthrough (clean tables, zero overhead) | Supported |
| INSERT / UPDATE / DELETE to overlay | Supported |
| JOINs across base + overlay tables | Supported |
| Subqueries, correlated subqueries, UNION, derived tables | Supported |
| GROUP BY, HAVING, aggregates (COUNT, SUM, AVG) on dirty tables | Supported |
| EXISTS / NOT EXISTS | Supported |
| CASE, COALESCE, IFNULL | Supported |
| LIMIT / OFFSET on dirty tables | Supported |
| Composite primary keys | Supported |
| Self-JOINs | Supported |
| DDL tracking (CREATE / ALTER / DROP / TRUNCATE TABLE) | Supported |
| Column DEFAULT values on overlay INSERT | Supported |
| LAST_INSERT_ID() tracking | Supported |
| Stored procedure body rewriting | Supported |
| Prepared statement forwarding | Supported |
| Multi-database support (USE db) | Supported |
| Foreign key constraint enforcement (RESTRICT, CASCADE, SET NULL) | Supported |
| Named overlays (create / list / switch / branch / merge) | Supported |
| Snapshots (save / restore / list) | Supported |
| Apply overlay to upstream (with dry-run) | Supported |
| Cross-overlay diff | Supported |
| Live query watch with filter | Supported |

## Known Limitations

- **No automatic merge conflict resolution.** `overlay merge` reports conflicts but does not auto-resolve them.
- **No LOAD DATA INFILE.** Bulk load statements are not intercepted.
- **No replication support.** The proxy is not a replication replica.
- **Prepared statements on dirty tables** read base data at PREPARE time rather than the merged view. Rewriting at PREPARE time is planned.
- **Complex stored procedures** with cursors, dynamic SQL (`EXECUTE`), or deeply nested calls may not rewrite correctly.
- **Foreign key UPDATE constraints** (when a PK value changes) are not enforced.
- **No connection pooling.** Each client gets a dedicated upstream connection to keep temporary tables isolated. A pooled mode is planned but not yet implemented.

## Testing

**Unit tests**

```bash
cargo test
```

**Integration tests**

Integration tests require a running MariaDB instance. The test suite is in `tests/integration.rs` and uses Docker to start MariaDB automatically.

```bash
# Run integration tests (Docker must be available)
cargo test --test integration

# Run with output visible
cargo test --test integration -- --nocapture
```

The integration tests spin up a MariaDB container, run the proxy against it, execute a suite of SQL operations, and verify the overlay behaves correctly — all without touching the upstream data.

## Building

```bash
# Release build
cargo build --release
# Binary: target/release/moo

# Debug build (faster compile, slower runtime)
cargo build
```

Requires Rust 1.70+. SQLite is bundled via `rusqlite` — no system SQLite dependency needed.
