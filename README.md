# mariadb-cow

Copy-on-Write proxy for MariaDB/MySQL — make changes freely, original stays untouched.

## What it does

`moo` sits in front of any MariaDB/MySQL database and intercepts every query. All writes (INSERT, UPDATE, DELETE, DDL) are redirected to a local SQLite overlay — the upstream database is never modified. When a SELECT touches a table that has overlay data, the proxy rewrites the query to merge the base and overlay results via temporary-table injection, so the client sees a consistent merged view. When you are done, run `moo reset` and the overlay is gone, leaving the upstream exactly as it was.

Think of it as OverlayFS for databases: read from the real database, write to a local layer, reads see both merged transparently.

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

The upstream user needs `SELECT` and `CREATE TEMPORARY TABLES` — no write privileges are required:

```sql
GRANT SELECT, CREATE TEMPORARY TABLES ON mydb.* TO 'moo_user'@'%';
```

## Usage

### start

Start the proxy. CLI flags override config file values.

```bash
moo start \
  --upstream=localhost:3306 \
  --listen=localhost:3307 \
  --overlay=./dev-overlay \
  --user=root \
  --password=secret \
  --config=moo.toml
```

Defaults: `upstream=localhost:3306`, `listen=localhost:3307`, `overlay=./dev-overlay`.

### status

Show overlay size and dirty table count.

```bash
moo status
moo status --overlay=./dev-overlay
```

### reset

Wipe the overlay. Resets all tables or a single named table.

```bash
moo reset                   # wipe everything
moo reset users             # wipe only the users table
moo reset --overlay=./dev-overlay users
```

### tables

List every table that has overlay data, with flags indicating whether schema or row data is present.

```bash
moo tables
moo tables --overlay=./dev-overlay
```

## Config File

All fields are optional. CLI flags take priority over the config file.

```toml
# moo.toml

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

Pass the config file with `--config=moo.toml` on the `start` command.

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

One SQLite file per database lives under the overlay directory (`./dev-overlay/mydb.db`). Overlay rows use auto-decrement IDs starting from `2^63-1` to avoid collisions with base DB IDs.

**Session model**

Each client connection gets a dedicated upstream connection. Temporary tables are session-scoped in MariaDB, so isolation between clients is automatic. Multiple `moo` instances can point at the same upstream simultaneously.

```
Client A  -->  moo (:3307)  -->  upstream conn 1  (temp tables for A)
Client B  -->  moo (:3307)  -->  upstream conn 2  (temp tables for B)
```

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

## Known Limitations

- **No commit-back.** Overlay changes cannot be merged into the upstream database. By design.
- **No LOAD DATA INFILE.** Bulk load statements are not intercepted.
- **No replication support.** The proxy is not a replication replica.
- **Prepared statements on dirty tables** read base data at PREPARE time rather than the merged view. Rewriting at PREPARE time is planned.
- **Complex stored procedures** with cursors, dynamic SQL (`EXECUTE`), or deeply nested calls may not rewrite correctly.
- **Foreign key UPDATE constraints** (when a PK value changes) are not enforced.
- **No connection pooling.** Each client gets a dedicated upstream connection to keep temporary tables isolated. A pooled mode (rebuild temps on checkout, clean up on checkin) is planned but not yet implemented.

## Building

```bash
cargo build --release
```

Requires Rust 1.70+. SQLite is bundled via `rusqlite` — no system SQLite dependency needed.

```bash
# Debug build (faster compile, slower runtime)
cargo build

# Run tests
cargo test
```

The release binary is at `target/release/moo`.
