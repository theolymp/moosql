use std::path::Path;

use crate::overlay::registry::{DirtyTableInfo, Registry};
#[cfg(test)]
use crate::overlay::registry::DirtyKind;
use crate::overlay::store::OverlayStore;

pub struct OverlayStatus {
    pub dirty_tables: usize,
    #[allow(dead_code)]
    pub total_rows: usize,
    pub databases: Vec<String>,
}

pub fn get_overlay_status(overlay_dir: &Path) -> anyhow::Result<OverlayStatus> {
    if !overlay_dir.exists() {
        return Ok(OverlayStatus {
            dirty_tables: 0,
            total_rows: 0,
            databases: vec![],
        });
    }

    let mut dirty_tables = 0usize;
    let mut databases = Vec::new();

    for entry in std::fs::read_dir(overlay_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("db") {
            continue;
        }

        let db_name = match path.file_stem().and_then(|s| s.to_str()) {
            Some(n) => n.to_owned(),
            None => continue,
        };

        let store = OverlayStore::open(overlay_dir, &db_name)?;
        let reg = Registry::new(&store.conn);
        let dirty = reg.list_dirty()?;

        if !dirty.is_empty() {
            dirty_tables += dirty.len();
            databases.push(db_name);
        }
    }

    Ok(OverlayStatus {
        dirty_tables,
        total_rows: 0,
        databases,
    })
}

pub fn list_dirty_tables(overlay_dir: &Path) -> anyhow::Result<Vec<(String, DirtyTableInfo)>> {
    if !overlay_dir.exists() {
        return Ok(vec![]);
    }

    let mut result = Vec::new();

    for entry in std::fs::read_dir(overlay_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("db") {
            continue;
        }

        let db_name = match path.file_stem().and_then(|s| s.to_str()) {
            Some(n) => n.to_owned(),
            None => continue,
        };

        let store = OverlayStore::open(overlay_dir, &db_name)?;
        let reg = Registry::new(&store.conn);
        let dirty = reg.list_dirty()?;

        for info in dirty {
            result.push((db_name.clone(), info));
        }
    }

    Ok(result)
}

pub fn reset_overlay(overlay_dir: &Path, table: Option<&str>) -> anyhow::Result<()> {
    if !overlay_dir.exists() {
        return Ok(());
    }

    for entry in std::fs::read_dir(overlay_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("db") {
            continue;
        }

        let db_name = match path.file_stem().and_then(|s| s.to_str()) {
            Some(n) => n.to_owned(),
            None => continue,
        };

        let store = OverlayStore::open(overlay_dir, &db_name)?;
        let reg = Registry::new(&store.conn);

        match table {
            Some(t) => reg.reset_table(t)?,
            None => reg.reset_all()?,
        }
    }

    Ok(())
}

pub fn print_status(overlay_dir: &Path) -> anyhow::Result<()> {
    let status = get_overlay_status(overlay_dir)?;

    println!("Overlay directory: {}", overlay_dir.display());
    println!("Dirty tables:      {}", status.dirty_tables);
    if status.databases.is_empty() {
        println!("Databases:         (none)");
    } else {
        println!("Databases:         {}", status.databases.join(", "));
    }

    Ok(())
}

pub fn print_tables(overlay_dir: &Path) -> anyhow::Result<()> {
    let tables = list_dirty_tables(overlay_dir)?;

    if tables.is_empty() {
        println!("No dirty tables in overlay.");
        return Ok(());
    }

    // Column widths
    let db_w = tables
        .iter()
        .map(|(db, _)| db.len())
        .max()
        .unwrap_or(8)
        .max(8);
    let tbl_w = tables
        .iter()
        .map(|(_, info)| info.table_name.len())
        .max()
        .unwrap_or(5)
        .max(5);

    println!(
        "{:<db_w$}  {:<tbl_w$}  {:<6}  {:<4}",
        "DATABASE", "TABLE", "SCHEMA", "DATA",
        db_w = db_w,
        tbl_w = tbl_w,
    );
    println!("{}", "-".repeat(db_w + tbl_w + 16));

    for (db, info) in &tables {
        println!(
            "{:<db_w$}  {:<tbl_w$}  {:<6}  {:<4}",
            db,
            info.table_name,
            if info.has_schema { "yes" } else { "no" },
            if info.has_data { "yes" } else { "no" },
            db_w = db_w,
            tbl_w = tbl_w,
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_status_empty_overlay() {
        let dir = TempDir::new().unwrap();
        let status = get_overlay_status(dir.path()).unwrap();
        assert_eq!(status.dirty_tables, 0);
    }

    #[test]
    fn test_status_with_dirty_table() {
        let dir = TempDir::new().unwrap();
        let store = OverlayStore::open(dir.path(), "testdb").unwrap();
        let reg = Registry::new(&store.conn);
        reg.mark_dirty("users", DirtyKind::Data).unwrap();
        drop(store);
        let status = get_overlay_status(dir.path()).unwrap();
        assert_eq!(status.dirty_tables, 1);
    }

    #[test]
    fn test_reset_all() {
        let dir = TempDir::new().unwrap();
        let store = OverlayStore::open(dir.path(), "testdb").unwrap();
        let reg = Registry::new(&store.conn);
        reg.mark_dirty("users", DirtyKind::Data).unwrap();
        drop(store);
        reset_overlay(dir.path(), None).unwrap();
        let status = get_overlay_status(dir.path()).unwrap();
        assert_eq!(status.dirty_tables, 0);
    }
}
