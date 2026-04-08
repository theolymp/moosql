use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::Result;

use crate::overlay::registry::Registry;
use crate::overlay::row_store::RowStore;
use crate::overlay::store::OverlayStore;

/// Summary of changes for one table in one overlay.
#[derive(Debug, Default, PartialEq)]
pub struct TableChangeSummary {
    pub inserted: usize,
    pub updated: usize,
    pub deleted: usize,
}

impl TableChangeSummary {
    fn is_empty(&self) -> bool {
        self.inserted == 0 && self.updated == 0 && self.deleted == 0
    }

    fn format(&self) -> String {
        let mut parts = Vec::new();
        if self.inserted > 0 {
            parts.push(format!("+{} inserted", self.inserted));
        }
        if self.updated > 0 {
            parts.push(format!("~{} updated", self.updated));
        }
        if self.deleted > 0 {
            parts.push(format!("-{} deleted", self.deleted));
        }
        if parts.is_empty() {
            "(no changes)".to_string()
        } else {
            parts.join(", ")
        }
    }
}

/// Summarize the rows in a shadow table by counting INSERTs, UPDATEs, DELETEs.
fn summarize_shadow_table(store: &OverlayStore, table: &str) -> Result<TableChangeSummary> {
    let rs = RowStore::new(&store.conn);
    let rows = rs.get_all_overlay_data(table)?;

    let mut summary = TableChangeSummary::default();
    for row in &rows {
        let op = row
            .iter()
            .find(|(k, _)| k == "_cow_op")
            .map(|(_, v)| v.as_str())
            .unwrap_or("");
        match op {
            "INSERT" => summary.inserted += 1,
            "UPDATE" => summary.updated += 1,
            "DELETE" => summary.deleted += 1,
            _ => {}
        }
    }
    Ok(summary)
}

/// Read all rows from the shadow table, indexed by `_cow_pk`.
/// Returns a map of pk -> serialised row string (all columns joined).
fn index_shadow_rows(store: &OverlayStore, table: &str) -> Result<HashMap<String, String>> {
    let rs = RowStore::new(&store.conn);
    let rows = rs.get_all_overlay_data(table)?;

    let mut map = HashMap::new();
    for row in rows {
        let pk = row
            .iter()
            .find(|(k, _)| k == "_cow_pk")
            .map(|(_, v)| v.clone())
            .unwrap_or_default();
        // Serialise all column values (sorted by key for determinism) as comparison key.
        let mut sorted = row.clone();
        sorted.sort_by(|a, b| a.0.cmp(&b.0));
        let val: String = sorted
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(";");
        map.insert(pk, val);
    }
    Ok(map)
}

/// Collect all database names (.db files) present in an overlay directory.
fn scan_db_names(overlay_dir: &Path) -> Result<HashSet<String>> {
    let mut names = HashSet::new();
    if !overlay_dir.exists() {
        return Ok(names);
    }
    for entry in std::fs::read_dir(overlay_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("db") {
            continue;
        }
        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
            names.insert(stem.to_owned());
        }
    }
    Ok(names)
}

pub fn run_diff_overlays(
    path_a: &Path,
    path_b: &Path,
    db_filter: Option<&str>,
    table_filter: Option<&str>,
) -> Result<()> {
    let dbs_a = scan_db_names(path_a)?;
    let dbs_b = scan_db_names(path_b)?;

    // Union of all database names present in either overlay.
    let all_dbs: HashSet<String> = dbs_a.union(&dbs_b).cloned().collect();

    // Apply optional db filter.
    let dbs_to_check: Vec<String> = if let Some(f) = db_filter {
        all_dbs.into_iter().filter(|d| d == f).collect()
    } else {
        let mut v: Vec<String> = all_dbs.into_iter().collect();
        v.sort();
        v
    };

    // Results grouped by category.
    let mut only_in_a: Vec<(String, String, TableChangeSummary)> = Vec::new(); // (db, table, summary_a)
    let mut only_in_b: Vec<(String, String, TableChangeSummary)> = Vec::new(); // (db, table, summary_b)
    let mut in_both: Vec<(String, String, TableChangeSummary, TableChangeSummary)> = Vec::new(); // (db, table, summary_a, summary_b)
    let mut identical: Vec<(String, String)> = Vec::new();

    for db in &dbs_to_check {
        let in_a = dbs_a.contains(db.as_str());
        let in_b = dbs_b.contains(db.as_str());

        // Open stores only for dbs that actually exist on each side.
        let store_a = if in_a {
            Some(OverlayStore::open(path_a, db)?)
        } else {
            None
        };
        let store_b = if in_b {
            Some(OverlayStore::open(path_b, db)?)
        } else {
            None
        };

        // Collect dirty tables from each side.
        let dirty_a: HashMap<String, _> = if let Some(ref s) = store_a {
            let reg = Registry::new(&s.conn);
            reg.list_dirty()?
                .into_iter()
                .map(|i| (i.table_name.clone(), i))
                .collect()
        } else {
            HashMap::new()
        };

        let dirty_b: HashMap<String, _> = if let Some(ref s) = store_b {
            let reg = Registry::new(&s.conn);
            reg.list_dirty()?
                .into_iter()
                .map(|i| (i.table_name.clone(), i))
                .collect()
        } else {
            HashMap::new()
        };

        let all_tables: HashSet<String> = dirty_a
            .keys()
            .chain(dirty_b.keys())
            .cloned()
            .collect();

        // Apply optional table filter.
        let tables_to_check: Vec<String> = if let Some(f) = table_filter {
            all_tables.into_iter().filter(|t| t == f).collect()
        } else {
            let mut v: Vec<String> = all_tables.into_iter().collect();
            v.sort();
            v
        };

        for table in &tables_to_check {
            let has_a = dirty_a.contains_key(table.as_str());
            let has_b = dirty_b.contains_key(table.as_str());

            match (has_a, has_b) {
                (true, false) => {
                    let summary = summarize_shadow_table(store_a.as_ref().unwrap(), table)?;
                    only_in_a.push((db.clone(), table.clone(), summary));
                }
                (false, true) => {
                    let summary = summarize_shadow_table(store_b.as_ref().unwrap(), table)?;
                    only_in_b.push((db.clone(), table.clone(), summary));
                }
                (true, true) => {
                    // Compare rows by pk.
                    let rows_a =
                        index_shadow_rows(store_a.as_ref().unwrap(), table)?;
                    let rows_b =
                        index_shadow_rows(store_b.as_ref().unwrap(), table)?;

                    if rows_a == rows_b {
                        identical.push((db.clone(), table.clone()));
                    } else {
                        let summary_a =
                            summarize_shadow_table(store_a.as_ref().unwrap(), table)?;
                        let summary_b =
                            summarize_shadow_table(store_b.as_ref().unwrap(), table)?;
                        in_both.push((db.clone(), table.clone(), summary_a, summary_b));
                    }
                }
                (false, false) => {} // shouldn't happen given union construction
            }
        }
    }

    // ---- Output ----
    let has_output = !only_in_a.is_empty() || !only_in_b.is_empty() || !in_both.is_empty();

    if !has_output && identical.is_empty() {
        println!("Overlays are identical (no dirty tables in either).");
        return Ok(());
    }

    if !has_output {
        println!("Overlays are identical.");
        for (db, table) in &identical {
            println!("  {db}.{table}: (same)");
        }
        return Ok(());
    }

    if !only_in_a.is_empty() {
        println!("Only in {}:", path_a.display());
        for (db, table, summary) in &only_in_a {
            if summary.is_empty() {
                println!("  {db}.{table}: (marked dirty, no row data)");
            } else {
                println!("  {db}.{table}: {}", summary.format());
            }
        }
    }

    if !only_in_b.is_empty() {
        println!("Only in {}:", path_b.display());
        for (db, table, summary) in &only_in_b {
            if summary.is_empty() {
                println!("  {db}.{table}: (marked dirty, no row data)");
            } else {
                println!("  {db}.{table}: {}", summary.format());
            }
        }
    }

    if !in_both.is_empty() {
        println!("Different in both:");
        for (db, table, summary_a, summary_b) in &in_both {
            println!("  {db}.{table}:");
            println!("    {}: {}", path_a.display(), summary_a.format());
            println!("    {}: {}", path_b.display(), summary_b.format());
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::overlay::registry::DirtyKind;
    use crate::overlay::row_store::RowStore;
    use tempfile::TempDir;

    /// Helper: open a store and insert some rows into the shadow table.
    fn insert_rows(
        store: &OverlayStore,
        table: &str,
        ops: &[(&str, &str)], // (pk, op)
    ) -> Result<()> {
        let rs = RowStore::new(&store.conn);
        rs.ensure_shadow_table(table, &[])?;

        let reg = Registry::new(&store.conn);
        reg.mark_dirty(table, DirtyKind::Data)?;

        for (pk, op) in ops {
            store.conn.execute(
                &format!(
                    "INSERT INTO \"_cow_data_{table}\" (_cow_pk, _cow_op) VALUES (?1, ?2)"
                ),
                rusqlite::params![pk, op],
            )?;
        }
        Ok(())
    }

    #[test]
    fn test_diff_identical_overlays() {
        let dir_a = TempDir::new().unwrap();
        let dir_b = TempDir::new().unwrap();

        // Same data in both
        {
            let store_a = OverlayStore::open(dir_a.path(), "testdb").unwrap();
            insert_rows(&store_a, "users", &[("1", "INSERT"), ("2", "INSERT")]).unwrap();
        }
        {
            let store_b = OverlayStore::open(dir_b.path(), "testdb").unwrap();
            insert_rows(&store_b, "users", &[("1", "INSERT"), ("2", "INSERT")]).unwrap();
        }

        // Should produce no entries in only_in_a, only_in_b, in_both.
        // We test by calling run_diff_overlays and verifying it doesn't return an error;
        // for behavioral correctness we test the helper functions directly.
        run_diff_overlays(dir_a.path(), dir_b.path(), None, None).unwrap();

        let rows_a = index_shadow_rows(
            &OverlayStore::open(dir_a.path(), "testdb").unwrap(),
            "users",
        )
        .unwrap();
        let rows_b = index_shadow_rows(
            &OverlayStore::open(dir_b.path(), "testdb").unwrap(),
            "users",
        )
        .unwrap();
        assert_eq!(rows_a, rows_b, "Identical overlays should have equal row indices");
    }

    #[test]
    fn test_diff_only_in_a() {
        let dir_a = TempDir::new().unwrap();
        let dir_b = TempDir::new().unwrap();

        // Only overlay-a has a dirty table
        {
            let store_a = OverlayStore::open(dir_a.path(), "testdb").unwrap();
            insert_rows(
                &store_a,
                "users",
                &[("1", "INSERT"), ("2", "INSERT"), ("3", "INSERT")],
            )
            .unwrap();
        }

        // dir_b has the same db file but no dirty tables in it
        {
            let _store_b = OverlayStore::open(dir_b.path(), "testdb").unwrap();
            // No dirty tables added
        }

        // run should succeed
        run_diff_overlays(dir_a.path(), dir_b.path(), None, None).unwrap();

        // Verify summary calculation
        let store_a = OverlayStore::open(dir_a.path(), "testdb").unwrap();
        let summary = summarize_shadow_table(&store_a, "users").unwrap();
        assert_eq!(summary.inserted, 3);
        assert_eq!(summary.updated, 0);
        assert_eq!(summary.deleted, 0);
    }

    #[test]
    fn test_diff_both_different() {
        let dir_a = TempDir::new().unwrap();
        let dir_b = TempDir::new().unwrap();

        {
            let store_a = OverlayStore::open(dir_a.path(), "testdb").unwrap();
            insert_rows(
                &store_a,
                "products",
                &[("1", "INSERT"), ("2", "INSERT")],
            )
            .unwrap();
        }
        {
            let store_b = OverlayStore::open(dir_b.path(), "testdb").unwrap();
            insert_rows(
                &store_b,
                "products",
                &[("1", "INSERT"), ("3", "DELETE")],
            )
            .unwrap();
        }

        run_diff_overlays(dir_a.path(), dir_b.path(), None, None).unwrap();

        let store_a = OverlayStore::open(dir_a.path(), "testdb").unwrap();
        let store_b = OverlayStore::open(dir_b.path(), "testdb").unwrap();

        let summary_a = summarize_shadow_table(&store_a, "products").unwrap();
        let summary_b = summarize_shadow_table(&store_b, "products").unwrap();

        assert_eq!(summary_a.inserted, 2);
        assert_eq!(summary_a.deleted, 0);

        assert_eq!(summary_b.inserted, 1);
        assert_eq!(summary_b.deleted, 1);

        // Rows should differ
        let rows_a = index_shadow_rows(&store_a, "products").unwrap();
        let rows_b = index_shadow_rows(&store_b, "products").unwrap();
        assert_ne!(rows_a, rows_b, "Different overlays should have different row indices");
    }
}
