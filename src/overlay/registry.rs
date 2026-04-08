use rusqlite::{Connection, Result as SqlResult};

#[derive(Debug, Clone, PartialEq)]
pub enum DirtyKind {
    Data,
    Schema,
    #[allow(dead_code)]
    Both,
}

#[derive(Debug, Clone)]
pub struct DirtyTableInfo {
    pub table_name: String,
    pub has_schema: bool,
    pub has_data: bool,
}

pub struct Registry<'a> {
    conn: &'a Connection,
}

impl<'a> Registry<'a> {
    pub fn new(conn: &'a Connection) -> Self {
        Self { conn }
    }

    #[allow(dead_code)]
    pub fn is_dirty(&self, table: &str) -> SqlResult<bool> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM _cow_tables WHERE table_name = ?1",
            [table],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    pub fn mark_dirty(&self, table: &str, kind: DirtyKind) -> SqlResult<()> {
        let (has_schema, has_data) = match kind {
            DirtyKind::Data => (0i64, 1i64),
            DirtyKind::Schema => (1i64, 0i64),
            DirtyKind::Both => (1i64, 1i64),
        };

        self.conn.execute(
            "INSERT INTO _cow_tables (table_name, has_schema, has_data)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(table_name) DO UPDATE SET
               has_schema = has_schema | excluded.has_schema,
               has_data   = has_data   | excluded.has_data",
            rusqlite::params![table, has_schema, has_data],
        )?;
        Ok(())
    }

    pub fn list_dirty(&self) -> SqlResult<Vec<DirtyTableInfo>> {
        let mut stmt = self.conn.prepare(
            "SELECT table_name, has_schema, has_data FROM _cow_tables",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(DirtyTableInfo {
                table_name: row.get(0)?,
                has_schema: row.get::<_, i64>(1)? != 0,
                has_data: row.get::<_, i64>(2)? != 0,
            })
        })?;
        rows.collect()
    }

    pub fn is_truncated(&self, table: &str) -> SqlResult<bool> {
        let result: rusqlite::Result<i64> = self.conn.query_row(
            "SELECT truncated FROM _cow_tables WHERE table_name = ?1",
            [table],
            |row| row.get(0),
        );
        match result {
            Ok(v) => Ok(v != 0),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(false),
            Err(e) => Err(e),
        }
    }

    pub fn reset_table(&self, table: &str) -> SqlResult<()> {
        // Drop shadow table if it exists
        let shadow = format!("_cow_data_{table}");
        self.conn.execute_batch(&format!(
            "DROP TABLE IF EXISTS \"{shadow}\";"
        ))?;

        self.conn.execute(
            "DELETE FROM _cow_tables WHERE table_name = ?1",
            [table],
        )?;
        self.conn.execute(
            "DELETE FROM _cow_sequences WHERE table_name = ?1",
            [table],
        )?;
        Ok(())
    }

    pub fn reset_all(&self) -> SqlResult<()> {
        // Collect all table names first, then drop each shadow table
        let dirty = self.list_dirty()?;
        for info in &dirty {
            let shadow = format!("_cow_data_{}", info.table_name);
            self.conn.execute_batch(&format!(
                "DROP TABLE IF EXISTS \"{shadow}\";"
            ))?;
        }
        self.conn.execute_batch(
            "DELETE FROM _cow_tables; DELETE FROM _cow_sequences;",
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::overlay::store::OverlayStore;
    use tempfile::TempDir;

    fn open_store() -> (TempDir, OverlayStore) {
        let dir = TempDir::new().unwrap();
        let store = OverlayStore::open(dir.path(), "test").unwrap();
        (dir, store)
    }

    #[test]
    fn test_mark_dirty_and_is_dirty() {
        let (_dir, store) = open_store();
        let reg = Registry::new(&store.conn);

        assert!(!reg.is_dirty("orders").unwrap());
        reg.mark_dirty("orders", DirtyKind::Data).unwrap();
        assert!(reg.is_dirty("orders").unwrap());
    }

    #[test]
    fn test_list_dirty_tables() {
        let (_dir, store) = open_store();
        let reg = Registry::new(&store.conn);

        reg.mark_dirty("orders", DirtyKind::Data).unwrap();
        reg.mark_dirty("users", DirtyKind::Schema).unwrap();
        reg.mark_dirty("products", DirtyKind::Both).unwrap();

        let mut dirty = reg.list_dirty().unwrap();
        dirty.sort_by(|a, b| a.table_name.cmp(&b.table_name));

        assert_eq!(dirty.len(), 3);

        let orders = dirty.iter().find(|t| t.table_name == "orders").unwrap();
        assert!(orders.has_data);
        assert!(!orders.has_schema);

        let users = dirty.iter().find(|t| t.table_name == "users").unwrap();
        assert!(!users.has_data);
        assert!(users.has_schema);

        let products = dirty.iter().find(|t| t.table_name == "products").unwrap();
        assert!(products.has_data);
        assert!(products.has_schema);
    }

    #[test]
    fn test_reset_table() {
        let (_dir, store) = open_store();
        let reg = Registry::new(&store.conn);

        reg.mark_dirty("orders", DirtyKind::Data).unwrap();
        reg.mark_dirty("users", DirtyKind::Schema).unwrap();

        assert!(reg.is_dirty("orders").unwrap());
        reg.reset_table("orders").unwrap();
        assert!(!reg.is_dirty("orders").unwrap());
        // users should still be dirty
        assert!(reg.is_dirty("users").unwrap());
    }

    #[test]
    fn test_is_truncated() {
        let (_dir, store) = open_store();
        let reg = Registry::new(&store.conn);

        // Not truncated by default
        assert!(!reg.is_truncated("users").unwrap());

        // Simulate TRUNCATE: mark dirty + set truncated=1
        store.conn.execute(
            "INSERT INTO _cow_tables (table_name, has_data, truncated) VALUES ('users', 1, 1)
             ON CONFLICT(table_name) DO UPDATE SET has_data=1, truncated=1",
            [],
        ).unwrap();

        assert!(reg.is_truncated("users").unwrap());

        // Verify it shows up in list_dirty
        let dirty = reg.list_dirty().unwrap();
        assert_eq!(dirty.len(), 1);
        assert_eq!(dirty[0].table_name, "users");
    }

    #[test]
    fn test_truncated_visible_after_reopen() {
        let dir = TempDir::new().unwrap();

        // Connection 1: TRUNCATE
        {
            let store = OverlayStore::open(dir.path(), "test").unwrap();
            store.conn.execute(
                "INSERT INTO _cow_tables (table_name, has_data, truncated) VALUES ('users', 1, 1)
                 ON CONFLICT(table_name) DO UPDATE SET has_data=1, truncated=1",
                [],
            ).unwrap();
        }

        // Connection 2: read dirty + truncated
        {
            let store = OverlayStore::open(dir.path(), "test").unwrap();
            let reg = Registry::new(&store.conn);
            let dirty = reg.list_dirty().unwrap();
            assert_eq!(dirty.len(), 1, "Should find 1 dirty table after reopen");
            assert_eq!(dirty[0].table_name, "users");
            assert!(reg.is_truncated("users").unwrap(), "Should be truncated after reopen");
        }
    }
}
