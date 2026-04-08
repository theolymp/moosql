use rusqlite::{Connection, OptionalExtension, Result as SqlResult};

pub struct SchemaTracker<'a> {
    conn: &'a Connection,
}

impl<'a> SchemaTracker<'a> {
    pub fn new(conn: &'a Connection) -> Self {
        Self { conn }
    }

    pub fn store_base_schema(&self, table: &str, create_sql: &str) -> SqlResult<()> {
        self.conn.execute(
            "INSERT INTO _cow_tables (table_name, base_schema)
             VALUES (?1, ?2)
             ON CONFLICT(table_name) DO UPDATE SET base_schema = excluded.base_schema",
            rusqlite::params![table, create_sql],
        )?;
        Ok(())
    }

    pub fn update_overlay_schema(&self, table: &str, create_sql: &str) -> SqlResult<()> {
        self.conn.execute(
            "INSERT INTO _cow_tables (table_name, overlay_schema)
             VALUES (?1, ?2)
             ON CONFLICT(table_name) DO UPDATE SET overlay_schema = excluded.overlay_schema",
            rusqlite::params![table, create_sql],
        )?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn get_overlay_schema(&self, table: &str) -> SqlResult<Option<String>> {
        self.conn
            .query_row(
                "SELECT overlay_schema FROM _cow_tables WHERE table_name = ?1",
                [table],
                |row| row.get(0),
            )
            .optional()
            .map(|opt| opt.flatten())
    }

    #[allow(dead_code)]
    pub fn get_base_schema(&self, table: &str) -> SqlResult<Option<String>> {
        self.conn
            .query_row(
                "SELECT base_schema FROM _cow_tables WHERE table_name = ?1",
                [table],
                |row| row.get(0),
            )
            .optional()
            .map(|opt| opt.flatten())
    }
}
