use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;

use crate::overlay::registry::Registry;
use crate::overlay::store::OverlayStore;

/// Branch: copy `<base>/<source>/` to `<base>/<new_name>/` by copying every .db file.
pub fn branch_overlay(base: &Path, source: &str, new_name: &str) -> Result<()> {
    let src_dir = base.join(source);
    let dst_dir = base.join(new_name);

    anyhow::ensure!(
        src_dir.exists(),
        "Source overlay '{}' does not exist at {}",
        source,
        src_dir.display()
    );
    anyhow::ensure!(
        !dst_dir.exists(),
        "Target overlay '{}' already exists at {}",
        new_name,
        dst_dir.display()
    );

    std::fs::create_dir_all(&dst_dir)
        .with_context(|| format!("Failed to create branch directory: {}", dst_dir.display()))?;

    // Copy every file in the source directory (SQLite db files + WAL/SHM if present)
    for entry in std::fs::read_dir(&src_dir)
        .with_context(|| format!("Failed to read source directory: {}", src_dir.display()))?
    {
        let entry = entry?;
        let file_name = entry.file_name();
        let src_file = entry.path();
        let dst_file = dst_dir.join(&file_name);
        std::fs::copy(&src_file, &dst_file).with_context(|| {
            format!(
                "Failed to copy {} -> {}",
                src_file.display(),
                dst_file.display()
            )
        })?;
    }

    Ok(())
}

/// Conflict report entry: a PK that was modified in both source and target.
#[derive(Debug)]
pub struct MergeConflict {
    pub db_name: String,
    pub table_name: String,
    pub pk: String,
}

/// Merge: copy non-conflicting changes from `<base>/<source>/` into `<base>/<target>/`.
/// Returns a list of conflicts (same `_cow_pk` modified in both). Does not auto-resolve.
pub fn merge_overlays(base: &Path, source: &str, target: &str) -> Result<Vec<MergeConflict>> {
    let src_dir = base.join(source);
    let tgt_dir = base.join(target);

    anyhow::ensure!(
        src_dir.exists(),
        "Source overlay '{}' does not exist at {}",
        source,
        src_dir.display()
    );
    anyhow::ensure!(
        tgt_dir.exists(),
        "Target overlay '{}' does not exist at {}",
        target,
        tgt_dir.display()
    );

    let mut all_conflicts = Vec::new();

    // Iterate over every .db file in the source directory
    for entry in std::fs::read_dir(&src_dir)
        .with_context(|| format!("Failed to read source directory: {}", src_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("db") {
            continue;
        }

        let db_name = match path.file_stem().and_then(|s| s.to_str()) {
            Some(n) => n.to_owned(),
            None => continue,
        };

        let src_store = OverlayStore::open(&src_dir, &db_name)?;
        let tgt_store = OverlayStore::open(&tgt_dir, &db_name)?;

        let src_reg = Registry::new(&src_store.conn);
        let tgt_reg = Registry::new(&tgt_store.conn);

        let src_dirty = src_reg.list_dirty()?;

        for src_table in src_dirty {
            let table = &src_table.table_name;
            let tgt_is_dirty = tgt_reg.is_dirty(table)?;

            if !tgt_is_dirty {
                // Safe to copy everything: shadow table + registry entry
                copy_shadow_table(&src_store.conn, &tgt_store.conn, table)
                    .with_context(|| {
                        format!(
                            "Failed to copy shadow table '{}' from '{}' to '{}'",
                            table, source, target
                        )
                    })?;
                // Mark table dirty in target registry
                let kind = match (src_table.has_schema, src_table.has_data) {
                    (true, true) => crate::overlay::registry::DirtyKind::Both,
                    (true, false) => crate::overlay::registry::DirtyKind::Schema,
                    _ => crate::overlay::registry::DirtyKind::Data,
                };
                tgt_reg.mark_dirty(table, kind)?;
            } else {
                // Both sides dirty: check for PK conflicts
                let src_pks = get_shadow_pks(&src_store.conn, table)?;
                let tgt_pks = get_shadow_pks(&tgt_store.conn, table)?;

                let mut conflicts: Vec<MergeConflict> = src_pks
                    .iter()
                    .filter(|pk| tgt_pks.contains(pk))
                    .map(|pk| MergeConflict {
                        db_name: db_name.clone(),
                        table_name: table.clone(),
                        pk: pk.clone(),
                    })
                    .collect();

                all_conflicts.append(&mut conflicts);

                // Copy non-conflicting source rows into target
                let conflict_pks: std::collections::HashSet<&String> =
                    all_conflicts.iter().map(|c| &c.pk).collect();

                copy_shadow_rows_excluding(
                    &src_store.conn,
                    &tgt_store.conn,
                    table,
                    &conflict_pks,
                )
                .with_context(|| {
                    format!(
                        "Failed to copy non-conflicting rows for table '{}' from '{}' to '{}'",
                        table, source, target
                    )
                })?;
            }
        }
    }

    Ok(all_conflicts)
}

/// Collect all `_cow_pk` values from `_cow_data_<table>` in `conn`.
/// Returns empty vec if the shadow table does not exist.
fn get_shadow_pks(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let shadow = format!("_cow_data_{table}");

    let exists: bool = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
        rusqlite::params![shadow],
        |row| row.get::<_, i64>(0),
    )? > 0;

    if !exists {
        return Ok(Vec::new());
    }

    let mut stmt = conn.prepare(&format!("SELECT _cow_pk FROM \"{shadow}\""))?;
    let pks = stmt.query_map([], |row| row.get::<_, String>(0))?;
    Ok(pks.collect::<rusqlite::Result<Vec<_>>>()?)
}

/// Copy all rows from `src._cow_data_<table>` into `dst._cow_data_<table>`.
/// The destination shadow table is created if it doesn't exist yet.
fn copy_shadow_table(src: &Connection, dst: &Connection, table: &str) -> Result<()> {
    let shadow = format!("_cow_data_{table}");

    // Get the CREATE TABLE DDL from the source
    let ddl: Option<String> = src
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?1",
            rusqlite::params![shadow],
            |row| row.get(0),
        )
        .optional()?;

    let ddl = match ddl {
        Some(d) => d,
        None => return Ok(()), // source shadow table doesn't exist yet
    };

    // Create the shadow table in destination (replace CREATE TABLE with CREATE TABLE IF NOT EXISTS)
    let create_if_not_exists = ddl.replacen("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1);
    dst.execute_batch(&create_if_not_exists)?;

    // Read all rows from source
    let mut stmt = src.prepare(&format!("SELECT * FROM \"{shadow}\""))?;
    let col_names: Vec<String> = stmt
        .column_names()
        .into_iter()
        .map(|s| s.to_owned())
        .collect();

    let placeholders = (1..=col_names.len())
        .map(|i| format!("?{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let quoted_cols = col_names
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let insert_sql = format!(
        "INSERT OR REPLACE INTO \"{shadow}\" ({quoted_cols}) VALUES ({placeholders})"
    );

    let rows: Vec<Vec<rusqlite::types::Value>> = stmt
        .query_map([], |row| {
            let mut vals = Vec::new();
            for i in 0..col_names.len() {
                vals.push(row.get::<_, rusqlite::types::Value>(i)?);
            }
            Ok(vals)
        })?
        .collect::<rusqlite::Result<_>>()?;

    for row in rows {
        let params = rusqlite::params_from_iter(row.iter());
        dst.execute(&insert_sql, params)?;
    }

    Ok(())
}

/// Copy rows from `src._cow_data_<table>` into `dst._cow_data_<table>`,
/// skipping rows whose `_cow_pk` is in `exclude_pks`.
fn copy_shadow_rows_excluding(
    src: &Connection,
    dst: &Connection,
    table: &str,
    exclude_pks: &std::collections::HashSet<&String>,
) -> Result<()> {
    let shadow = format!("_cow_data_{table}");

    let ddl: Option<String> = src
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?1",
            rusqlite::params![shadow],
            |row| row.get(0),
        )
        .optional()?;

    let ddl = match ddl {
        Some(d) => d,
        None => return Ok(()),
    };

    let create_if_not_exists = ddl.replacen("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1);
    dst.execute_batch(&create_if_not_exists)?;

    let mut stmt = src.prepare(&format!("SELECT * FROM \"{shadow}\""))?;
    let col_names: Vec<String> = stmt
        .column_names()
        .into_iter()
        .map(|s| s.to_owned())
        .collect();

    // Find index of _cow_pk column
    let pk_col_idx = col_names
        .iter()
        .position(|c| c == "_cow_pk")
        .unwrap_or(0);

    let placeholders = (1..=col_names.len())
        .map(|i| format!("?{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let quoted_cols = col_names
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let insert_sql = format!(
        "INSERT OR IGNORE INTO \"{shadow}\" ({quoted_cols}) VALUES ({placeholders})"
    );

    let rows: Vec<Vec<rusqlite::types::Value>> = stmt
        .query_map([], |row| {
            let mut vals = Vec::new();
            for i in 0..col_names.len() {
                vals.push(row.get::<_, rusqlite::types::Value>(i)?);
            }
            Ok(vals)
        })?
        .collect::<rusqlite::Result<_>>()?;

    for row in rows {
        let pk_val = match &row[pk_col_idx] {
            rusqlite::types::Value::Text(s) => s.clone(),
            _ => continue,
        };

        if exclude_pks.contains(&pk_val) {
            continue;
        }

        let params = rusqlite::params_from_iter(row.iter());
        dst.execute(&insert_sql, params)?;
    }

    Ok(())
}

// Extension trait for optional query
trait OptionalExt<T> {
    fn optional(self) -> rusqlite::Result<Option<T>>;
}

impl<T> OptionalExt<T> for rusqlite::Result<T> {
    fn optional(self) -> rusqlite::Result<Option<T>> {
        match self {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::overlay::registry::{DirtyKind, Registry};
    use crate::overlay::store::OverlayStore;
    use tempfile::TempDir;

    fn setup_overlay_with_data(base: &Path, name: &str) -> OverlayStore {
        let dir = base.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        let store = OverlayStore::open(&dir, "testdb").unwrap();
        let reg = Registry::new(&store.conn);

        // Create a shadow table and add some rows
        store
            .conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS \"_cow_data_users\" (
                    _cow_pk TEXT NOT NULL,
                    _cow_op TEXT NOT NULL,
                    name TEXT
                );
                INSERT INTO \"_cow_data_users\" VALUES ('1', 'INSERT', 'Alice');
                INSERT INTO \"_cow_data_users\" VALUES ('2', 'UPDATE', 'Bob');",
            )
            .unwrap();
        reg.mark_dirty("users", DirtyKind::Data).unwrap();

        store
    }

    #[test]
    fn test_branch_overlay() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path();

        // Set up source overlay
        let _src = setup_overlay_with_data(base, "main");

        // Branch it
        branch_overlay(base, "main", "feature").unwrap();

        // Verify the branch exists and has the same data
        let branch_dir = base.join("feature");
        assert!(branch_dir.exists());

        let branch_store = OverlayStore::open(&branch_dir, "testdb").unwrap();
        let reg = Registry::new(&branch_store.conn);
        let dirty = reg.list_dirty().unwrap();
        assert_eq!(dirty.len(), 1);
        assert_eq!(dirty[0].table_name, "users");

        // Verify the actual row data was copied
        let count: i64 = branch_store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM \"_cow_data_users\"",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_merge_no_conflicts() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path();

        // Source: has changes to "orders"
        {
            let dir = base.join("source");
            std::fs::create_dir_all(&dir).unwrap();
            let store = OverlayStore::open(&dir, "testdb").unwrap();
            let reg = Registry::new(&store.conn);
            store
                .conn
                .execute_batch(
                    "CREATE TABLE IF NOT EXISTS \"_cow_data_orders\" (
                        _cow_pk TEXT NOT NULL,
                        _cow_op TEXT NOT NULL,
                        amount TEXT
                    );
                    INSERT INTO \"_cow_data_orders\" VALUES ('10', 'INSERT', '99.99');",
                )
                .unwrap();
            reg.mark_dirty("orders", DirtyKind::Data).unwrap();
        }

        // Target: has changes to "users" (different table — no conflict possible)
        {
            let dir = base.join("target");
            std::fs::create_dir_all(&dir).unwrap();
            let store = OverlayStore::open(&dir, "testdb").unwrap();
            let reg = Registry::new(&store.conn);
            store
                .conn
                .execute_batch(
                    "CREATE TABLE IF NOT EXISTS \"_cow_data_users\" (
                        _cow_pk TEXT NOT NULL,
                        _cow_op TEXT NOT NULL,
                        name TEXT
                    );
                    INSERT INTO \"_cow_data_users\" VALUES ('1', 'INSERT', 'Alice');",
                )
                .unwrap();
            reg.mark_dirty("users", DirtyKind::Data).unwrap();
        }

        let conflicts = merge_overlays(base, "source", "target").unwrap();
        assert!(conflicts.is_empty(), "Expected no conflicts, got: {:?}", conflicts);

        // Verify target now also has the orders data
        let tgt_dir = base.join("target");
        let tgt_store = OverlayStore::open(&tgt_dir, "testdb").unwrap();
        let count: i64 = tgt_store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM \"_cow_data_orders\"",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_merge_with_conflicts() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path();

        // Source: modified user pk=1
        {
            let dir = base.join("source");
            std::fs::create_dir_all(&dir).unwrap();
            let store = OverlayStore::open(&dir, "testdb").unwrap();
            let reg = Registry::new(&store.conn);
            store
                .conn
                .execute_batch(
                    "CREATE TABLE IF NOT EXISTS \"_cow_data_users\" (
                        _cow_pk TEXT NOT NULL,
                        _cow_op TEXT NOT NULL,
                        name TEXT
                    );
                    INSERT INTO \"_cow_data_users\" VALUES ('1', 'UPDATE', 'Alice-src');
                    INSERT INTO \"_cow_data_users\" VALUES ('3', 'INSERT', 'Charlie');",
                )
                .unwrap();
            reg.mark_dirty("users", DirtyKind::Data).unwrap();
        }

        // Target: also modified user pk=1 (conflict!) and pk=2
        {
            let dir = base.join("target");
            std::fs::create_dir_all(&dir).unwrap();
            let store = OverlayStore::open(&dir, "testdb").unwrap();
            let reg = Registry::new(&store.conn);
            store
                .conn
                .execute_batch(
                    "CREATE TABLE IF NOT EXISTS \"_cow_data_users\" (
                        _cow_pk TEXT NOT NULL,
                        _cow_op TEXT NOT NULL,
                        name TEXT
                    );
                    INSERT INTO \"_cow_data_users\" VALUES ('1', 'UPDATE', 'Alice-tgt');
                    INSERT INTO \"_cow_data_users\" VALUES ('2', 'DELETE', 'Bob');",
                )
                .unwrap();
            reg.mark_dirty("users", DirtyKind::Data).unwrap();
        }

        let conflicts = merge_overlays(base, "source", "target").unwrap();

        // pk=1 should be a conflict
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].table_name, "users");
        assert_eq!(conflicts[0].pk, "1");

        // pk=3 (non-conflicting from source) should now be in target
        let tgt_dir = base.join("target");
        let tgt_store = OverlayStore::open(&tgt_dir, "testdb").unwrap();
        let count: i64 = tgt_store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM \"_cow_data_users\" WHERE _cow_pk='3'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "Non-conflicting row pk=3 should be in target");

        // pk=1 in target should still be Alice-tgt (not overwritten)
        let name: String = tgt_store
            .conn
            .query_row(
                "SELECT name FROM \"_cow_data_users\" WHERE _cow_pk='1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(name, "Alice-tgt", "Conflicting row should not be overwritten");
    }
}
