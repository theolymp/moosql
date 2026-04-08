use rusqlite::{Connection, OptionalExtension, Result as SqlResult};

pub struct SequenceTracker<'a> {
    conn: &'a Connection,
}

impl<'a> SequenceTracker<'a> {
    pub fn new(conn: &'a Connection) -> Self {
        Self { conn }
    }

    /// Returns the next ID for the given table.
    /// IDs start at i64::MAX and count downward.
    pub fn next_id(&self, table: &str) -> SqlResult<i64> {
        // Seed if not present
        self.conn.execute(
            "INSERT OR IGNORE INTO _cow_sequences (table_name, next_value) VALUES (?1, ?2)",
            rusqlite::params![table, i64::MAX],
        )?;

        // Read current
        let current: i64 = self.conn.query_row(
            "SELECT next_value FROM _cow_sequences WHERE table_name = ?1",
            [table],
            |row| row.get(0),
        )?;

        // Decrement for next call
        self.conn.execute(
            "UPDATE _cow_sequences SET next_value = next_value - 1 WHERE table_name = ?1",
            [table],
        )?;

        Ok(current)
    }

    /// Returns the most recently issued ID (next_value + 1), or None if no ID has been issued yet.
    #[allow(dead_code)]
    pub fn last_id(&self, table: &str) -> SqlResult<Option<i64>> {
        let result: Option<i64> = self
            .conn
            .query_row(
                "SELECT next_value FROM _cow_sequences WHERE table_name = ?1",
                [table],
                |row| row.get(0),
            )
            .optional()?;

        match result {
            // If next_value is still at MAX, no ID has been issued yet
            Some(v) if v == i64::MAX => Ok(None),
            Some(v) => Ok(Some(v + 1)),
            None => Ok(None),
        }
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
    fn test_next_id_counts_down() {
        let (_dir, store) = open_store();
        let seq = SequenceTracker::new(&store.conn);

        let id1 = seq.next_id("orders").unwrap();
        let id2 = seq.next_id("orders").unwrap();

        assert_eq!(id1, i64::MAX);
        assert_eq!(id2, i64::MAX - 1);
    }

    #[test]
    fn test_separate_sequences_per_table() {
        let (_dir, store) = open_store();
        let seq = SequenceTracker::new(&store.conn);

        let orders_id1 = seq.next_id("orders").unwrap();
        let users_id1 = seq.next_id("users").unwrap();
        let orders_id2 = seq.next_id("orders").unwrap();

        assert_eq!(orders_id1, i64::MAX);
        assert_eq!(users_id1, i64::MAX);
        assert_eq!(orders_id2, i64::MAX - 1);
    }
}
