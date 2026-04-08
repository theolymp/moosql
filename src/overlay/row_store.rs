use rusqlite::{Connection, Result as SqlResult};

pub struct RowStore<'a> {
    conn: &'a Connection,
}

#[allow(dead_code)]
pub struct OverlayRow {
    pub pk: String,
    pub op: String,
}

impl<'a> RowStore<'a> {
    pub fn new(conn: &'a Connection) -> Self {
        Self { conn }
    }

    /// Creates the shadow table `_cow_data_{table}` with the given columns if it does not exist.
    /// `columns` is a slice of `(column_name, column_type)` pairs.
    pub fn ensure_shadow_table(
        &self,
        table: &str,
        columns: &[(&str, &str)],
    ) -> SqlResult<()> {
        let shadow = format!("_cow_data_{table}");

        let col_defs: String = columns
            .iter()
            .map(|(name, ty)| format!("    \"{name}\" {ty}"))
            .collect::<Vec<_>>()
            .join(",\n");

        let sep = if col_defs.is_empty() { "" } else { ",\n" };

        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS \"{shadow}\" (\n\
             _cow_pk  TEXT NOT NULL,\n\
             _cow_op  TEXT NOT NULL CHECK(_cow_op IN ('INSERT','UPDATE','DELETE')){sep}{col_defs}\n\
             );"
        );

        self.conn.execute_batch(&ddl)?;
        Ok(())
    }

    /// Returns all overlay rows for the given table.
    #[allow(dead_code)]
    pub fn get_overlay_rows(&self, table: &str) -> SqlResult<Vec<OverlayRow>> {
        let shadow = format!("_cow_data_{table}");
        let mut stmt = self
            .conn
            .prepare(&format!("SELECT _cow_pk, _cow_op FROM \"{shadow}\""))?;
        let rows = stmt.query_map([], |row| {
            Ok(OverlayRow {
                pk: row.get(0)?,
                op: row.get(1)?,
            })
        })?;
        rows.collect()
    }

    /// Reads all columns from `_cow_data_<table>` and returns each row as a list
    /// of (column_name, value) pairs (including `_cow_pk` and `_cow_op`).
    /// Returns an empty Vec if the shadow table does not exist yet.
    pub fn get_all_overlay_data(&self, table: &str) -> SqlResult<Vec<Vec<(String, String)>>> {
        let shadow = format!("_cow_data_{table}");

        // Check whether the shadow table exists at all.
        let exists: bool = self.conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
            rusqlite::params![shadow],
            |row| row.get::<_, i64>(0),
        )? > 0;

        if !exists {
            return Ok(Vec::new());
        }

        let mut stmt = self
            .conn
            .prepare(&format!("SELECT * FROM \"{shadow}\""))?;

        // Collect column names from the statement metadata.
        let column_names: Vec<String> = stmt
            .column_names()
            .into_iter()
            .map(|s| s.to_owned())
            .collect();

        let rows = stmt.query_map([], |row| {
            let mut pairs = Vec::new();
            for (i, name) in column_names.iter().enumerate() {
                let val: rusqlite::types::Value = row.get(i)?;
                let s = match val {
                    rusqlite::types::Value::Null => "NULL".to_string(),
                    rusqlite::types::Value::Integer(n) => n.to_string(),
                    rusqlite::types::Value::Real(f) => f.to_string(),
                    rusqlite::types::Value::Text(t) => t,
                    rusqlite::types::Value::Blob(b) => {
                        String::from_utf8_lossy(&b).into_owned()
                    }
                };
                pairs.push((name.clone(), s));
            }
            Ok(pairs)
        })?;

        rows.collect()
    }

    /// Returns the primary keys of rows that have been tombstoned (DELETE operation).
    #[allow(dead_code)]
    pub fn get_tombstone_pks(&self, table: &str) -> SqlResult<Vec<String>> {
        let shadow = format!("_cow_data_{table}");
        let mut stmt = self.conn.prepare(&format!(
            "SELECT _cow_pk FROM \"{shadow}\" WHERE _cow_op = 'DELETE'"
        ))?;
        let pks = stmt.query_map([], |row| row.get(0))?;
        pks.collect()
    }
}
