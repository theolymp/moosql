# mariadb-cow diff Command

## Overview

A CLI subcommand that shows what changed in the overlay compared to the base database. Works offline by reading SQLite overlay files directly. Optionally connects to upstream for full UPDATE diffs.

## CLI Interface

```
mariadb-cow diff [OPTIONS]

Options:
  --overlay <PATH>     Overlay directory (default: ./dev-overlay)
  --format <FORMAT>    Output format: text (default), sql
  --verbose            Show row-level details (default: table summary only)
  --full               For UPDATEs: fetch base rows and show old->new diff
  --upstream <ADDR>    Upstream address (required with --full)
  --user <USER>        Upstream user (required with --full)
  --password <PASS>    Upstream password (required with --full)
  --table <NAME>       Filter to a specific table
  --db <NAME>          Filter to a specific database (default: all)
```

## Output Modes

### Summary (default)

```
testdb:
  users:     +2 inserted, ~1 updated, -1 deleted
  orders:    +1 inserted
  products:  (schema changed: ALTER TABLE)
  temp_data: (created in overlay)
```

### Verbose (--verbose)

```
testdb.users:
  + INSERT id=9223372036854775807 name='David' email='david@test.com' active=1
  + INSERT id=9223372036854775806 name='Eve' email='eve@test.com' active=1
  ~ UPDATE id=1 name='Alice' email='alice-new@test.com' active=1
  - DELETE id=3
```

### Verbose + Full (--verbose --full)

Shows old-to-new comparison for UPDATEs by fetching base rows from upstream:

```
testdb.users:
  ~ UPDATE id=1:
      email: 'alice@example.com' -> 'alice-new@test.com'
```

Requires `--upstream`, `--user`, `--password`.

### SQL (--format=sql)

Generates executable SQL statements representing the overlay changes:

```sql
-- testdb.users
INSERT INTO `users` (`id`, `name`, `email`, `active`) VALUES (9223372036854775807, 'David', 'david@test.com', 1);
UPDATE `users` SET `email` = 'alice-new@test.com' WHERE `id` = 1;
DELETE FROM `users` WHERE `id` = 3;
```

This doubles as an export feature: `mariadb-cow diff --format=sql > changes.sql`.

## Implementation

### Data Flow

1. Scan `overlay_dir` for `.db` files (one per database)
2. For each DB: read `_cow_tables` for dirty/truncated/schema flags
3. For each dirty table: read `_cow_data_<table>` shadow table rows
4. Group rows by `_cow_op` (INSERT/UPDATE/DELETE)
5. Apply `--table` and `--db` filters
6. Format output according to `--format` and `--verbose`/`--full` flags

### Full Diff Mode (--full)

When `--full` is specified:
1. Connect to upstream via `mysql_async`
2. For each UPDATE row: fetch the base row by PK (`SELECT * FROM table WHERE pk = ?`)
3. Compare columns between base and overlay
4. Show only changed columns in the diff

Error handling: if upstream is unreachable, print warning and fall back to showing overlay values only.

### File Structure

- `src/cli/diff.rs` — diff command logic: read overlay, format output
- `src/config.rs` — add `Diff` subcommand to CLI args
- `src/main.rs` — wire up diff command

### Truncated Tables

If a table has `truncated=1` in `_cow_tables`:
- Summary: show `(truncated)` or `(truncated, +N inserted after)`
- Verbose: show all overlay rows as INSERTs (they're the only data)
- SQL: generate `TRUNCATE TABLE` followed by INSERTs

### Schema-Only Changes

Tables with `has_schema=1` but `has_data=0`:
- Summary: show the overlay_schema text (e.g., "schema changed: ALTER TABLE ...")
- SQL: output the DDL statement as stored in `overlay_schema`

## Testing

Unit tests in `diff.rs`:
- `test_summary_format` — verify summary output for known overlay state
- `test_verbose_format` — verify row-level output
- `test_sql_format` — verify SQL generation
- `test_filter_by_table` — verify --table filter
- `test_truncated_table_output` — verify truncated table handling
