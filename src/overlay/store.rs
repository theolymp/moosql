use rusqlite::{Connection, Result as SqlResult};
use std::path::{Path, PathBuf};

pub struct OverlayStore {
    pub conn: Connection,
    pub db_name: String,
    pub path: PathBuf,
}

impl OverlayStore {
    pub fn open(overlay_dir: &Path, db_name: &str) -> SqlResult<Self> {
        std::fs::create_dir_all(overlay_dir).map_err(|e| {
            rusqlite::Error::InvalidPath(
                format!("Failed to create overlay dir: {e}").into(),
            )
        })?;

        let path = overlay_dir.join(format!("{db_name}.db"));
        let conn = Connection::open(&path)?;

        // Pragmas
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;

        // Meta tables
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS _cow_tables (
                table_name   TEXT PRIMARY KEY,
                has_schema   INTEGER DEFAULT 0,
                has_data     INTEGER DEFAULT 0,
                base_schema  TEXT,
                overlay_schema TEXT,
                truncated    INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS _cow_sequences (
                table_name TEXT PRIMARY KEY,
                next_value INTEGER DEFAULT 9223372036854775807
            );",
        )?;

        // Migrate existing databases: add truncated column if it doesn't exist yet.
        // SQLite returns an error if the column already exists; we safely ignore it.
        let _ = conn.execute_batch(
            "ALTER TABLE _cow_tables ADD COLUMN truncated INTEGER DEFAULT 0;",
        );

        Ok(Self {
            conn,
            db_name: db_name.to_owned(),
            path,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_open_creates_db_file() {
        let dir = TempDir::new().unwrap();
        let store = OverlayStore::open(dir.path(), "testdb").unwrap();
        assert!(store.path.exists());
        assert_eq!(store.path.file_name().unwrap(), "testdb.db");
    }

    #[test]
    fn test_open_creates_meta_tables() {
        let dir = TempDir::new().unwrap();
        let store = OverlayStore::open(dir.path(), "testdb").unwrap();

        let tables_exist: bool = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('_cow_tables','_cow_sequences')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
            == 2;

        assert!(tables_exist, "_cow_tables and _cow_sequences should exist");
    }
}
