use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, OptsBuilder, Params as MysqlParams, Row, Statement as MysqlStatement, Value as MysqlValue};
use opensrv_mysql::{
    AsyncMysqlShim, Column, ColumnFlags, ColumnType, ErrorKind, InitWriter, OkResponse,
    ParamParser, QueryResultWriter, StatementMetaWriter, ValueInner,
};
use tokio::io::AsyncWrite;
use tracing::{debug, info, warn};

use crate::bridge::temp_tables::TempTableManager;
use crate::overlay::registry::Registry;
use crate::overlay::store::OverlayStore;
use crate::overlay::transaction::TransactionBuffer;
use crate::overlay::writer;
use crate::protocol::query_handler::{self, QueryAction};
use crate::proxy::schema_cache::{CachedColumn, CachedTableSchema, FkAction, ForeignKeyInfo, SchemaCache};
use crate::sql::parser::TransactionOp;

static CONNECTION_ID: AtomicU32 = AtomicU32::new(1);

/// Result of FK constraint enforcement for a DELETE operation.
struct FkResult {
    /// Child tables whose rows should be cascade-deleted: (child_table, child_pks).
    cascaded_deletes: Vec<(String, Vec<String>)>,
    /// Child tables whose FK column should be set to NULL: (child_table, fk_column, affected_child_pks).
    set_null_updates: Vec<(String, String, Vec<String>)>,
}

/// Main handler implementing the opensrv-mysql AsyncMysqlShim trait.
/// Each client connection gets its own CowHandler instance.
pub struct CowHandler {
    /// Connection to upstream MariaDB/MySQL server (established after auth).
    upstream: Option<Conn>,
    /// Upstream address for connecting.
    upstream_addr: String,
    /// Upstream username for authenticating with the real server.
    upstream_user: String,
    /// Upstream password for authenticating with the real server.
    upstream_password: String,
    /// Path to overlay storage directory.
    overlay_dir: PathBuf,
    /// Current database context.
    current_db: Option<String>,
    /// Temp table manager for creating CoW temp tables on upstream.
    temp_tables: TempTableManager,
    /// Transaction buffer for overlay writes.
    tx_buffer: TransactionBuffer,
    /// Unique connection ID for this session.
    conn_id: u32,
    /// Cached list of dirty table names (refreshed as needed).
    dirty_tables: Vec<String>,
    /// Cached list of truncated table names (subset of dirty_tables, refreshed alongside).
    truncated_tables: Vec<String>,
    /// Cached map of table name -> primary-key column name (refreshed alongside dirty_tables).
    pk_columns: HashMap<String, String>,
    /// Upstream prepared statements keyed by the statement ID sent to the client.
    prepared_stmts: HashMap<u32, MysqlStatement>,
    /// Last insert ID produced by an overlay INSERT (returned for SELECT LAST_INSERT_ID()).
    last_insert_id: Option<i64>,
    /// Per-session schema cache: avoids repeated SHOW COLUMNS round-trips.
    schema_cache: SchemaCache,
}

impl CowHandler {
    pub fn new(
        upstream_addr: String,
        upstream_user: String,
        upstream_password: String,
        overlay_dir: PathBuf,
    ) -> Self {
        Self {
            upstream: None,
            upstream_addr,
            upstream_user,
            upstream_password,
            overlay_dir,
            current_db: None,
            temp_tables: TempTableManager::new(),
            tx_buffer: TransactionBuffer::new(),
            conn_id: CONNECTION_ID.fetch_add(1, Ordering::Relaxed),
            dirty_tables: Vec::new(),
            truncated_tables: Vec::new(),
            pk_columns: HashMap::new(),
            prepared_stmts: HashMap::new(),
            last_insert_id: None,
            schema_cache: SchemaCache::new(),
        }
    }

    /// Connect to the upstream MariaDB server with given credentials.
    async fn connect_upstream(
        &mut self,
        user: &str,
        password: &str,
        db: Option<&str>,
    ) -> Result<(), mysql_async::Error> {
        let (host, port) = if let Some(colon_pos) = self.upstream_addr.rfind(':') {
            let host = &self.upstream_addr[..colon_pos];
            let port_str = &self.upstream_addr[colon_pos + 1..];
            let port: u16 = port_str.parse().unwrap_or(3306);
            (host.to_string(), port)
        } else {
            (self.upstream_addr.clone(), 3306u16)
        };

        let mut builder = OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port)
            .user(Some(user))
            .pass(Some(password));

        if let Some(db_name) = db {
            builder = builder.db_name(Some(db_name));
        }

        let opts = Opts::from(builder);
        let conn = Conn::new(opts).await?;
        self.upstream = Some(conn);

        if let Some(db_name) = db {
            self.current_db = Some(db_name.to_string());
        }

        Ok(())
    }

    /// Refresh the dirty tables list from the overlay store.
    /// Also refreshes the truncated_tables list (tables that have been TRUNCATEd).
    fn refresh_dirty_tables(&mut self) {
        if let Some(ref db) = self.current_db {
            match OverlayStore::open(&self.overlay_dir, db) {
                Ok(store) => {
                    let reg = Registry::new(&store.conn);
                    match reg.list_dirty() {
                        Ok(dirty) => {
                            let table_names: Vec<String> =
                                dirty.iter().map(|d| d.table_name.clone()).collect();
                            // Check which of the dirty tables are also truncated.
                            let mut truncated = Vec::new();
                            for name in &table_names {
                                match reg.is_truncated(name) {
                                    Ok(true) => {
                                        info!("Table {} is truncated in overlay", name);
                                        truncated.push(name.clone());
                                    }
                                    Ok(false) => {}
                                    Err(e) => {
                                        warn!("Failed to check truncated flag for {}: {}", name, e);
                                    }
                                }
                            }
                            self.dirty_tables = table_names;
                            self.truncated_tables = truncated;
                        }
                        Err(e) => {
                            warn!("Failed to list dirty tables: {}", e);
                            self.dirty_tables.clear();
                            self.truncated_tables.clear();
                        }
                    }
                }
                Err(e) => {
                    debug!("No overlay store for db {}: {}", db, e);
                    self.dirty_tables.clear();
                    self.truncated_tables.clear();
                }
            }
        } else {
            self.dirty_tables.clear();
            self.truncated_tables.clear();
        }
    }

    /// Refresh the pk_columns map for all currently-dirty tables.
    ///
    /// Reads from the schema cache (populated lazily by `get_or_fetch_schema`).
    /// Tables not yet in the cache are skipped here; they will be fetched on demand
    /// when the first write against them arrives.
    fn refresh_pk_columns(&mut self) {
        let dirty = self.dirty_tables.clone();
        let mut map = HashMap::new();
        for table in &dirty {
            if let Some(cached) = self.schema_cache.get(table) {
                let pk = cached.pk_column();
                // Only store if the schema actually has a PK column; otherwise let
                // the rewriter fall back to "id".
                if cached.columns.iter().any(|c| c.is_pk) {
                    map.insert(table.clone(), pk);
                }
            }
            // If not cached yet, skip — the cache will be populated on the first
            // write, and refresh_pk_columns is called again at the start of each query.
        }
        self.pk_columns = map;
    }

    /// Return the cached schema for `table`, fetching it from upstream via a single
    /// `SHOW COLUMNS` call if not yet cached.
    async fn get_or_fetch_schema(&mut self, table: &str) -> io::Result<CachedTableSchema> {
        // Cache hit — clone so the borrow ends before we potentially mutate.
        if let Some(cached) = self.schema_cache.get(table) {
            return Ok(cached.clone());
        }

        // Cache miss — fetch from upstream.
        let conn = match self.upstream.as_mut() {
            Some(c) => c,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "No upstream connection for schema fetch",
                ))
            }
        };

        let sql = format!("SHOW COLUMNS FROM `{}`", table);
        let rows: Vec<mysql_async::Row> = conn
            .query(&sql)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("SHOW COLUMNS failed: {e}")))?;

        let mut columns = Vec::with_capacity(rows.len());
        for row in &rows {
            // Field(0), Type(1), Null(2), Key(3), Default(4), Extra(5)
            let name: String = row
                .as_ref(0)
                .and_then(|v| match v {
                    mysql_async::Value::Bytes(b) => Some(String::from_utf8_lossy(b).to_string()),
                    _ => None,
                })
                .unwrap_or_default();
            let col_type: String = row
                .as_ref(1)
                .and_then(|v| match v {
                    mysql_async::Value::Bytes(b) => Some(String::from_utf8_lossy(b).to_string()),
                    _ => None,
                })
                .unwrap_or_default();
            let is_pk: bool = row
                .as_ref(3)
                .and_then(|v| match v {
                    mysql_async::Value::Bytes(b) => Some(String::from_utf8_lossy(b).to_string()),
                    _ => None,
                })
                .map(|k| k == "PRI")
                .unwrap_or(false);
            let default: Option<String> = row
                .as_ref(4)
                .and_then(|v| match v {
                    mysql_async::Value::NULL => None,
                    mysql_async::Value::Bytes(b) => Some(String::from_utf8_lossy(b).to_string()),
                    mysql_async::Value::Int(n) => Some(n.to_string()),
                    mysql_async::Value::UInt(n) => Some(n.to_string()),
                    other => Some(format!("{:?}", other)),
                });
            columns.push(CachedColumn { name, col_type, default, is_pk });
        }

        let schema = CachedTableSchema { columns };
        self.schema_cache.insert(table.to_string(), schema.clone());
        Ok(schema)
    }

    /// Load FK relations from INFORMATION_SCHEMA if not already cached.
    async fn ensure_fk_relations_loaded(&mut self) -> io::Result<()> {
        if self.schema_cache.fk_relations_loaded() {
            return Ok(());
        }

        let db = match &self.current_db {
            Some(db) => db.clone(),
            None => return Ok(()),
        };

        let conn = match self.upstream.as_mut() {
            Some(c) => c,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "No upstream connection for FK metadata fetch",
                ));
            }
        };

        let sql = format!(
            "SELECT kcu.TABLE_NAME, kcu.COLUMN_NAME, kcu.REFERENCED_TABLE_NAME, \
             kcu.REFERENCED_COLUMN_NAME, rc.DELETE_RULE, rc.UPDATE_RULE \
             FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc \
             JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu \
                 ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME \
                 AND rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA \
             WHERE rc.CONSTRAINT_SCHEMA = '{}'",
            db
        );

        let rows: Vec<Row> = conn
            .query(&sql)
            .await
            .map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("FK metadata query failed: {e}"))
            })?;

        let mut relations = Vec::new();
        for row in &rows {
            let extract = |idx: usize| -> String {
                row.as_ref(idx)
                    .and_then(|v| match v {
                        mysql_async::Value::Bytes(b) => {
                            Some(String::from_utf8_lossy(b).to_string())
                        }
                        _ => None,
                    })
                    .unwrap_or_default()
            };

            let child_table = extract(0);
            let child_column = extract(1);
            let parent_table = extract(2);
            let parent_column = extract(3);
            let delete_rule = extract(4);
            let update_rule = extract(5);

            if !child_table.is_empty() && !parent_table.is_empty() {
                relations.push(ForeignKeyInfo {
                    child_table,
                    child_column,
                    parent_table,
                    parent_column,
                    on_delete: FkAction::from_str(&delete_rule),
                    on_update: FkAction::from_str(&update_rule),
                });
            }
        }

        debug!(
            conn_id = self.conn_id,
            count = relations.len(),
            "Loaded FK relations from INFORMATION_SCHEMA"
        );
        self.schema_cache.set_fk_relations(relations);
        Ok(())
    }

    /// Enforce FK constraints for a DELETE on the given parent table.
    ///
    /// Returns `FkResult` containing cascaded deletes and set-null updates to apply,
    /// or an error if a RESTRICT/NO ACTION constraint is violated.
    async fn enforce_fk_delete(
        &mut self,
        _parent_table: &str,
        deleted_pks: &[String],
        child_fks: Vec<ForeignKeyInfo>,
    ) -> io::Result<FkResult> {
        let mut result = FkResult {
            cascaded_deletes: Vec::new(),
            set_null_updates: Vec::new(),
        };

        if deleted_pks.is_empty() {
            return Ok(result);
        }

        for fk in &child_fks {
            // Fetch the child table schema to get its PK column
            let child_schema = self.get_or_fetch_schema(&fk.child_table).await?;
            let child_pk_col = child_schema.pk_column();

            // Build the IN clause for the parent PKs
            let in_values: Vec<String> = deleted_pks
                .iter()
                .map(|pk| format!("'{}'", pk.replace('\'', "''")))
                .collect();
            let in_clause = in_values.join(",");

            // Query upstream to find child rows referencing the deleted parent rows
            let select_sql = format!(
                "SELECT `{}` FROM `{}` WHERE `{}` IN ({})",
                child_pk_col, fk.child_table, fk.child_column, in_clause
            );

            let child_pks: Vec<String> = if let Some(ref mut conn) = self.upstream {
                let rows: Vec<Row> = conn.query(&select_sql).await.map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!("FK check query failed on {}: {e}", fk.child_table),
                    )
                })?;

                rows.iter()
                    .filter_map(|row| {
                        row.as_ref(0).map(|v| match v {
                            mysql_async::Value::NULL => "NULL".to_string(),
                            mysql_async::Value::Bytes(b) => {
                                String::from_utf8_lossy(b).to_string()
                            }
                            mysql_async::Value::Int(n) => n.to_string(),
                            mysql_async::Value::UInt(n) => n.to_string(),
                            _ => format!("{:?}", v),
                        })
                    })
                    .collect()
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "No upstream connection for FK check",
                ));
            };

            if child_pks.is_empty() {
                continue;
            }

            match fk.on_delete {
                FkAction::Restrict | FkAction::NoAction => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "Cannot delete or update a parent row: a foreign key constraint fails \
                             (`{}`.`{}`, CONSTRAINT FOREIGN KEY (`{}`) REFERENCES `{}` (`{}`))",
                            fk.child_table,
                            fk.child_column,
                            fk.child_column,
                            fk.parent_table,
                            fk.parent_column
                        ),
                    ));
                }
                FkAction::Cascade => {
                    result
                        .cascaded_deletes
                        .push((fk.child_table.clone(), child_pks));
                }
                FkAction::SetNull => {
                    result.set_null_updates.push((
                        fk.child_table.clone(),
                        fk.child_column.clone(),
                        child_pks,
                    ));
                }
                FkAction::SetDefault => {
                    // SET DEFAULT is rare and not well-supported by MySQL/MariaDB InnoDB.
                    // Treat as RESTRICT for safety.
                    if !child_pks.is_empty() {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "Cannot delete or update a parent row: a foreign key constraint fails \
                                 (SET DEFAULT not supported, `{}`.`{}`)",
                                fk.child_table, fk.child_column
                            ),
                        ));
                    }
                }
            }
        }

        Ok(result)
    }

    /// Recursively enforce FK constraints for a DELETE, applying cascaded deletes
    /// and SET NULL updates through the overlay.
    fn enforce_fk_delete_recursive<'a>(
        &'a mut self,
        table: &'a str,
        pks: &'a [String],
        depth: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + 'a>> {
        Box::pin(async move {
        if depth > 10 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "FK cascade depth exceeded (max 10)",
            ));
        }

        if pks.is_empty() {
            return Ok(());
        }

        // Clone FK data before async operations to avoid borrow issues
        let child_fks: Vec<ForeignKeyInfo> = self
            .schema_cache
            .get_child_fks(table)
            .into_iter()
            .cloned()
            .collect();

        if child_fks.is_empty() {
            return Ok(());
        }

        let fk_result = self.enforce_fk_delete(table, pks, child_fks).await?;

        // Apply SET NULL updates
        for (child_table, fk_col, child_pks) in &fk_result.set_null_updates {
            let child_schema = self.get_or_fetch_schema(child_table).await?;
            let schema_pairs = child_schema.schema_pairs();
            let child_pk_col = child_schema.pk_column();

            let db = match &self.current_db {
                Some(db) => db.clone(),
                None => continue,
            };

            // Fetch full rows from upstream for the affected children so we can
            // write proper overlay UPDATE rows.
            let in_values: Vec<String> = child_pks
                .iter()
                .map(|pk| format!("'{}'", pk.replace('\'', "''")))
                .collect();
            let in_clause = in_values.join(",");

            let col_names: Vec<String> = schema_pairs.iter().map(|(n, _)| format!("`{}`", n)).collect();
            let select_sql = format!(
                "SELECT {} FROM `{}` WHERE `{}` IN ({})",
                col_names.join(", "),
                child_table,
                child_pk_col,
                in_clause
            );

            let upstream_rows: Vec<Vec<(String, String)>> = if let Some(ref mut conn) = self.upstream {
                let rows: Vec<Row> = conn.query(&select_sql).await.map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!("FK SET NULL fetch failed on {}: {e}", child_table),
                    )
                })?;

                rows.iter()
                    .map(|row| {
                        schema_pairs
                            .iter()
                            .enumerate()
                            .map(|(i, (col_name, _))| {
                                let val = row
                                    .as_ref(i)
                                    .map(|v| match v {
                                        mysql_async::Value::NULL => "NULL".to_string(),
                                        mysql_async::Value::Bytes(b) => {
                                            String::from_utf8_lossy(b).to_string()
                                        }
                                        mysql_async::Value::Int(n) => n.to_string(),
                                        mysql_async::Value::UInt(n) => n.to_string(),
                                        _ => format!("{:?}", v),
                                    })
                                    .unwrap_or_else(|| "NULL".to_string());
                                (col_name.clone(), val)
                            })
                            .collect()
                    })
                    .collect()
            } else {
                continue;
            };

            // Open overlay store and write SET NULL updates
            // (OverlayStore uses rusqlite which is !Send — open, use, drop before .await)
            {
                let store = OverlayStore::open(&self.overlay_dir, &db).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!("Failed to open overlay for FK SET NULL: {e}"),
                    )
                })?;

                let row_store = crate::overlay::row_store::RowStore::new(&store.conn);
                let col_refs: Vec<(&str, &str)> = schema_pairs
                    .iter()
                    .map(|(n, t)| (n.as_str(), t.as_str()))
                    .collect();
                row_store.ensure_shadow_table(child_table, &col_refs).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!("Failed to ensure shadow table for FK SET NULL: {e}"),
                    )
                })?;

                let shadow = format!("_cow_data_{}", child_table);

                for upstream_row in &upstream_rows {
                    // Build the row with the FK column set to NULL
                    let cow_pk = upstream_row
                        .iter()
                        .find(|(c, _)| c.eq_ignore_ascii_case(&child_pk_col))
                        .map(|(_, v)| v.clone())
                        .unwrap_or_else(|| "unknown".to_string());

                    // Check if row already exists in overlay
                    let existing_op: Option<String> = store
                        .conn
                        .query_row(
                            &format!("SELECT _cow_op FROM \"{}\" WHERE _cow_pk = ?1", shadow),
                            rusqlite::params![cow_pk],
                            |r| r.get(0),
                        )
                        .ok();

                    // Build column names and values for the upsert
                    let mut col_names_sql = vec!["_cow_pk".to_string(), "_cow_op".to_string()];
                    let mut placeholders = vec!["?1".to_string(), "'UPDATE'".to_string()];
                    let mut values: Vec<String> = vec![cow_pk.clone()];

                    let mut param_idx = 2;
                    for (col_name, col_val) in upstream_row {
                        col_names_sql.push(format!("\"{}\"", col_name));
                        if col_name.eq_ignore_ascii_case(fk_col) {
                            placeholders.push("NULL".to_string());
                        } else {
                            param_idx += 1;
                            placeholders.push(format!("?{}", param_idx - 1));
                            values.push(col_val.clone());
                        }
                    }

                    if existing_op.is_some() {
                        // Update existing overlay row — set the FK column to NULL
                        let set_clauses: Vec<String> = upstream_row
                            .iter()
                            .map(|(col_name, _)| {
                                if col_name.eq_ignore_ascii_case(fk_col) {
                                    format!("\"{}\" = NULL", col_name)
                                } else {
                                    // Keep existing value
                                    format!("\"{}\" = \"{}\"", col_name, col_name)
                                }
                            })
                            .collect();
                        let update_sql = format!(
                            "UPDATE \"{}\" SET _cow_op = 'UPDATE', {} WHERE _cow_pk = ?1",
                            shadow,
                            set_clauses.join(", ")
                        );
                        store
                            .conn
                            .execute(&update_sql, rusqlite::params![cow_pk])
                            .map_err(|e| {
                                io::Error::new(
                                    io::ErrorKind::Other,
                                    format!("FK SET NULL overlay update failed: {e}"),
                                )
                            })?;
                    } else {
                        // Insert new overlay row with FK column as NULL
                        let insert_sql = format!(
                            "INSERT INTO \"{}\" ({}) VALUES ({})",
                            shadow,
                            col_names_sql.join(", "),
                            placeholders.join(", ")
                        );
                        let params: Vec<&dyn rusqlite::types::ToSql> =
                            values.iter().map(|v| v as &dyn rusqlite::types::ToSql).collect();
                        store.conn.execute(&insert_sql, params.as_slice()).map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::Other,
                                format!("FK SET NULL overlay insert failed: {e}"),
                            )
                        })?;
                    }
                }

                // Mark child table as dirty
                let reg = crate::overlay::registry::Registry::new(&store.conn);
                reg.mark_dirty(child_table, crate::overlay::registry::DirtyKind::Data)
                    .map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!("Failed to mark {} as dirty: {e}", child_table),
                        )
                    })?;
            }

            // Invalidate temp table cache for the child table
            self.temp_tables.invalidate(child_table);
        }

        // Apply cascaded deletes recursively
        for (child_table, child_pks) in &fk_result.cascaded_deletes {
            // Fetch child schema for the delete
            let child_schema = self.get_or_fetch_schema(child_table).await?;
            let schema_pairs = child_schema.schema_pairs();

            let db = match &self.current_db {
                Some(db) => db.clone(),
                None => continue,
            };

            // Open overlay store, write tombstones for the child rows
            {
                let store = OverlayStore::open(&self.overlay_dir, &db).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!("Failed to open overlay for FK CASCADE: {e}"),
                    )
                })?;

                let row_store = crate::overlay::row_store::RowStore::new(&store.conn);
                let col_refs: Vec<(&str, &str)> = schema_pairs
                    .iter()
                    .map(|(n, t)| (n.as_str(), t.as_str()))
                    .collect();
                row_store.ensure_shadow_table(child_table, &col_refs).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!("Failed to ensure shadow table for FK CASCADE: {e}"),
                    )
                })?;

                let shadow = format!("_cow_data_{}", child_table);

                for pk in child_pks {
                    let existing_op: Option<String> = store
                        .conn
                        .query_row(
                            &format!("SELECT _cow_op FROM \"{}\" WHERE _cow_pk = ?1", shadow),
                            rusqlite::params![pk],
                            |r| r.get(0),
                        )
                        .ok();

                    match existing_op.as_deref() {
                        Some("INSERT") => {
                            // Overlay-only row — just delete it
                            store
                                .conn
                                .execute(
                                    &format!("DELETE FROM \"{}\" WHERE _cow_pk = ?1", shadow),
                                    rusqlite::params![pk],
                                )
                                .map_err(|e| {
                                    io::Error::new(
                                        io::ErrorKind::Other,
                                        format!("FK CASCADE delete overlay-only row failed: {e}"),
                                    )
                                })?;
                        }
                        Some(_) => {
                            // Convert to tombstone
                            store
                                .conn
                                .execute(
                                    &format!(
                                        "UPDATE \"{}\" SET _cow_op = 'DELETE' WHERE _cow_pk = ?1",
                                        shadow
                                    ),
                                    rusqlite::params![pk],
                                )
                                .map_err(|e| {
                                    io::Error::new(
                                        io::ErrorKind::Other,
                                        format!("FK CASCADE tombstone update failed: {e}"),
                                    )
                                })?;
                        }
                        None => {
                            // Insert tombstone
                            store
                                .conn
                                .execute(
                                    &format!(
                                        "INSERT INTO \"{}\" (_cow_pk, _cow_op) VALUES (?1, 'DELETE')",
                                        shadow
                                    ),
                                    rusqlite::params![pk],
                                )
                                .map_err(|e| {
                                    io::Error::new(
                                        io::ErrorKind::Other,
                                        format!("FK CASCADE tombstone insert failed: {e}"),
                                    )
                                })?;
                        }
                    }
                }

                // Mark child table as dirty
                let reg = crate::overlay::registry::Registry::new(&store.conn);
                reg.mark_dirty(child_table, crate::overlay::registry::DirtyKind::Data)
                    .map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!("Failed to mark {} as dirty: {e}", child_table),
                        )
                    })?;
            }

            // Invalidate temp table cache for the child table
            self.temp_tables.invalidate(child_table);

            // Recurse: this child table might also have children with CASCADE
            self.enforce_fk_delete_recursive(child_table, child_pks, depth + 1)
                .await?;
        }

        Ok(())
        }) // end async move
    }

    /// Populate temp tables for all supplied dirty tables by reading overlay rows
    /// from SQLite and inserting them into upstream-session temporary tables.
    async fn populate_temp_tables(&mut self, dirty_tables: &[String]) -> io::Result<()> {
        let db = match self.current_db.clone() {
            Some(db) => db,
            None => return Ok(()), // No database selected — nothing to do.
        };

        let store = match crate::overlay::store::OverlayStore::open(&self.overlay_dir, &db) {
            Ok(s) => s,
            Err(e) => {
                debug!(conn_id = self.conn_id, error = %e, "No overlay store; skipping temp-table population");
                return Ok(());
            }
        };

        for table in dirty_tables {
            // Skip if temp table is already current for this session.
            if self.temp_tables.is_current(table) {
                debug!(conn_id = self.conn_id, table = %table, "Temp table already current; skipping recreation");
                continue;
            }

            // Fetch (or retrieve from cache) the table schema.
            let cached_schema = match self.get_or_fetch_schema(table).await {
                Ok(s) => s,
                Err(e) => {
                    warn!(conn_id = self.conn_id, table = %table, error = %e, "Could not fetch schema for temp-table population; skipping table");
                    continue;
                }
            };
            let schema: Vec<(String, String)> = cached_schema.schema_pairs();

            // Read all overlay rows (with data columns) from SQLite.
            // The RowStore borrows store.conn — drop it immediately after use so it
            // does not live across the .await below (rustc non-Send borrow check).
            let overlay_data = {
                let row_store = crate::overlay::row_store::RowStore::new(&store.conn);
                match row_store.get_all_overlay_data(table) {
                    Ok(d) => d,
                    Err(e) => {
                        warn!(conn_id = self.conn_id, table = %table, error = %e, "Failed to read overlay data; skipping table");
                        continue;
                    }
                }
            };

            // Always create temp tables for dirty tables (even if empty),
            // so the rewritten queries can reference them without errors.

            // Build the column definitions for ensure_temp_table (exclude _cow_pk / _cow_op —
            // those are added automatically by TempTableManager).
            let col_defs: Vec<(String, String)> = schema
                .iter()
                .map(|(name, ty)| (name.clone(), ty.clone()))
                .collect();
            let col_defs_refs: Vec<(&str, &str)> = col_defs
                .iter()
                .map(|(n, t)| (n.as_str(), t.as_str()))
                .collect();

            // Convert SQLite rows into TempRow structs.
            let temp_rows: Vec<crate::bridge::temp_tables::TempRow> = overlay_data
                .iter()
                .map(|pairs| {
                    let pk = pairs
                        .iter()
                        .find(|(k, _)| k == "_cow_pk")
                        .map(|(_, v)| v.clone())
                        .unwrap_or_default();
                    let op = pairs
                        .iter()
                        .find(|(k, _)| k == "_cow_op")
                        .map(|(_, v)| v.clone())
                        .unwrap_or_default();

                    // Values in schema column order.
                    let values: Vec<String> = col_defs
                        .iter()
                        .map(|(col_name, _)| {
                            pairs
                                .iter()
                                .find(|(k, _)| k == col_name)
                                .map(|(_, v)| v.clone())
                                .unwrap_or_else(|| "NULL".to_string())
                        })
                        .collect();

                    crate::bridge::temp_tables::TempRow { pk, op, values }
                })
                .collect();

            // Create or refresh the temp table in the upstream session.
            let conn = match self.upstream.as_mut() {
                Some(c) => c,
                None => return Err(io::Error::new(io::ErrorKind::NotConnected, "No upstream connection")),
            };

            if let Err(e) = self
                .temp_tables
                .ensure_temp_table(conn, table, &col_defs_refs, &temp_rows)
                .await
            {
                warn!(conn_id = self.conn_id, table = %table, error = %e, "Failed to create/populate temp table");
            } else {
                debug!(conn_id = self.conn_id, table = %table, rows = temp_rows.len(), "Populated temp table");
            }
        }

        Ok(())
    }

    /// Forward a query to upstream and relay the result set back to the client.
    async fn forward_query<W: AsyncWrite + Unpin + Send>(
        &mut self,
        sql: &str,
        results: QueryResultWriter<'_, W>,
    ) -> io::Result<()> {
        if self.upstream.is_none() {
            results
                .error(
                    ErrorKind::ER_UNKNOWN_ERROR,
                    b"No upstream connection established",
                )
                .await?;
            return Ok(());
        }

        // Execute query on upstream; on connection error, attempt one reconnect.
        let query_result = {
            let conn = self.upstream.as_mut().unwrap();
            conn.query_iter(sql).await
        };

        // If the first attempt returned a connection error, reconnect and retry once.
        let query_result = match query_result {
            Err(ref e) if is_connection_error(e) => {
                warn!(
                    conn_id = self.conn_id,
                    error = %e,
                    "Upstream connection error — attempting reconnect"
                );
                let user = self.upstream_user.clone();
                let pass = self.upstream_password.clone();
                let db = self.current_db.clone();
                match self.connect_upstream(&user, &pass, db.as_deref()).await {
                    Ok(()) => {
                        info!(conn_id = self.conn_id, "Reconnected to upstream after connection drop");
                        let conn = self.upstream.as_mut().unwrap();
                        conn.query_iter(sql).await
                    }
                    Err(reconnect_err) => {
                        let msg = format!("Upstream connection lost and reconnect failed: {}", reconnect_err);
                        results.error(ErrorKind::ER_UNKNOWN_ERROR, msg.as_bytes()).await?;
                        return Ok(());
                    }
                }
            }
            other => other,
        };
        match query_result {
            Ok(mut result) => {
                // Get column metadata before consuming rows
                let columns_arc = result.columns();

                match columns_arc {
                    Some(cols) if !cols.is_empty() => {
                        // Build column definitions for opensrv-mysql
                        let opensrv_cols: Vec<Column> = cols
                            .iter()
                            .map(|c| Column {
                                table: c.table_str().to_string(),
                                column: c.name_str().to_string(),
                                coltype: mysql_col_type_to_opensrv(c.column_type()),
                                colflags: mysql_flags_to_opensrv(c.flags()),
                            })
                            .collect();

                        let num_cols = opensrv_cols.len();

                        // Collect all rows
                        let rows: Vec<Row> = result.collect().await.map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::Other,
                                format!("upstream error: {}", e),
                            )
                        })?;

                        // Drop the result to release the connection
                        drop(result);

                        // Start writing result set
                        let mut row_writer = results.start(&opensrv_cols).await?;

                        for row in &rows {
                            for i in 0..num_cols {
                                // Get value as raw bytes — use try_get to avoid panics
                                // on values that can't be converted to Vec<u8>.
                                let val: Option<Vec<u8>> = row
                                    .as_ref(i)
                                    .map(|v| match v {
                                        mysql_async::Value::NULL => None,
                                        mysql_async::Value::Bytes(b) => Some(b.clone()),
                                        mysql_async::Value::Int(n) => Some(n.to_string().into_bytes()),
                                        mysql_async::Value::UInt(n) => Some(n.to_string().into_bytes()),
                                        mysql_async::Value::Float(f) => Some(f.to_string().into_bytes()),
                                        mysql_async::Value::Double(d) => Some(d.to_string().into_bytes()),
                                        other => Some(format!("{:?}", other).into_bytes()),
                                    })
                                    .flatten();
                                row_writer.write_col(val)?;
                            }
                            row_writer.end_row().await?;
                        }

                        row_writer.finish().await?;
                    }
                    _ => {
                        // No columns = this is an OK result (INSERT/UPDATE/DELETE etc.)
                        let affected = result.affected_rows();
                        let last_insert = result.last_insert_id().unwrap_or(0);

                        // Drain the result
                        drop(result);

                        let ok = OkResponse {
                            affected_rows: affected,
                            last_insert_id: last_insert,
                            ..Default::default()
                        };
                        results.completed(ok).await?;
                    }
                }

                Ok(())
            }
            Err(e) => {
                let msg = format!("{}", e);
                results
                    .error(ErrorKind::ER_UNKNOWN_ERROR, msg.as_bytes())
                    .await?;
                Ok(())
            }
        }
    }
}

#[async_trait]
impl<W: AsyncWrite + Unpin + Send> AsyncMysqlShim<W> for CowHandler {
    type Error = io::Error;

    fn version(&self) -> String {
        "5.7.99-moosql-proxy".to_string()
    }

    fn connect_id(&self) -> u32 {
        self.conn_id
    }

    /// Accept all client connections — actual auth is handled by the upstream server.
    /// We capture the client username here for logging purposes only; the real
    /// credential check happens when we connect to upstream in on_query/on_init.
    async fn authenticate(
        &self,
        _auth_plugin: &str,
        username: &[u8],
        _salt: &[u8],
        _auth_data: &[u8],
    ) -> bool {
        let client_user = String::from_utf8_lossy(username);
        info!(
            conn_id = self.conn_id,
            client_user = %client_user,
            upstream_user = %self.upstream_user,
            "Client authenticated (upstream credentials will be used)"
        );
        true
    }

    async fn on_prepare<'a>(
        &'a mut self,
        query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> io::Result<()> {
        debug!(conn_id = self.conn_id, sql = %query, "PREPARE");

        // Ensure upstream connection exists.
        if self.upstream.is_none() {
            let user = self.upstream_user.clone();
            let password = self.upstream_password.clone();
            let db = self.current_db.clone();
            match self.connect_upstream(&user, &password, db.as_deref()).await {
                Ok(()) => {}
                Err(e) => {
                    let msg = format!("Failed to connect to upstream: {}", e);
                    info.error(ErrorKind::ER_UNKNOWN_ERROR, msg.as_bytes()).await?;
                    return Ok(());
                }
            }
        }

        // Forward PREPARE to upstream.
        let stmt = match self.upstream.as_mut().unwrap().prep(query).await {
            Ok(s) => s,
            Err(e) => {
                let msg = format!("{}", e);
                info.error(ErrorKind::ER_UNKNOWN_ERROR, msg.as_bytes()).await?;
                return Ok(());
            }
        };

        // Build param column descriptors (one entry per `?` placeholder).
        let num_params = stmt.num_params() as usize;
        let param_cols: Vec<Column> = stmt
            .params()
            .iter()
            .map(|c| Column {
                table: c.table_str().to_string(),
                column: c.name_str().to_string(),
                coltype: mysql_col_type_to_opensrv(c.column_type()),
                colflags: mysql_flags_to_opensrv(c.flags()),
            })
            .collect();

        // If the upstream returned no param descriptors but the statement has params,
        // generate placeholder columns (MySQL protocol allows this).
        let param_cols: Vec<Column> = if param_cols.is_empty() && num_params > 0 {
            (0..num_params)
                .map(|_| Column {
                    table: String::new(),
                    column: String::new(),
                    coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                    colflags: ColumnFlags::empty(),
                })
                .collect()
        } else {
            param_cols
        };

        // Build result column descriptors.
        let result_cols: Vec<Column> = stmt
            .columns()
            .iter()
            .map(|c| Column {
                table: c.table_str().to_string(),
                column: c.name_str().to_string(),
                coltype: mysql_col_type_to_opensrv(c.column_type()),
                colflags: mysql_flags_to_opensrv(c.flags()),
            })
            .collect();

        // Use the upstream statement ID as the client-visible ID so they align.
        let stmt_id = stmt.id();
        self.prepared_stmts.insert(stmt_id, stmt);

        info.reply(stmt_id, &param_cols, &result_cols).await?;
        Ok(())
    }

    async fn on_execute<'a>(
        &'a mut self,
        id: u32,
        params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        debug!(conn_id = self.conn_id, stmt_id = id, "EXECUTE");

        // Look up the upstream statement.
        let stmt = match self.prepared_stmts.get(&id).cloned() {
            Some(s) => s,
            None => {
                let msg = format!("Unknown prepared statement id {}", id);
                results.error(ErrorKind::ER_UNKNOWN_ERROR, msg.as_bytes()).await?;
                return Ok(());
            }
        };

        // Convert opensrv-mysql param values to mysql_async values.
        let mysql_params: Vec<MysqlValue> = params
            .into_iter()
            .map(|pv| opensrv_value_to_mysql(pv.value.into_inner()))
            .collect();

        let upstream_params: MysqlParams = if mysql_params.is_empty() {
            MysqlParams::Empty
        } else {
            MysqlParams::Positional(mysql_params)
        };

        // Execute on upstream.
        let conn = match self.upstream.as_mut() {
            Some(c) => c,
            None => {
                results
                    .error(ErrorKind::ER_UNKNOWN_ERROR, b"No upstream connection")
                    .await?;
                return Ok(());
            }
        };

        let query_result = conn.exec_iter(stmt, upstream_params).await;
        match query_result {
            Ok(mut result) => {
                let columns_arc = result.columns();
                match columns_arc {
                    Some(cols) if !cols.is_empty() => {
                        let opensrv_cols: Vec<Column> = cols
                            .iter()
                            .map(|c| Column {
                                table: c.table_str().to_string(),
                                column: c.name_str().to_string(),
                                coltype: mysql_col_type_to_opensrv(c.column_type()),
                                colflags: mysql_flags_to_opensrv(c.flags()),
                            })
                            .collect();

                        let num_cols = opensrv_cols.len();
                        let rows: Vec<Row> = result.collect().await.map_err(|e| {
                            io::Error::new(io::ErrorKind::Other, format!("upstream error: {}", e))
                        })?;
                        drop(result);

                        let mut row_writer = results.start(&opensrv_cols).await?;
                        for row in &rows {
                            for i in 0..num_cols {
                                let val: Option<Vec<u8>> = row
                                    .as_ref(i)
                                    .map(|v| match v {
                                        mysql_async::Value::NULL => None,
                                        mysql_async::Value::Bytes(b) => Some(b.clone()),
                                        mysql_async::Value::Int(n) => Some(n.to_string().into_bytes()),
                                        mysql_async::Value::UInt(n) => Some(n.to_string().into_bytes()),
                                        mysql_async::Value::Float(f) => Some(f.to_string().into_bytes()),
                                        mysql_async::Value::Double(d) => Some(d.to_string().into_bytes()),
                                        other => Some(format!("{:?}", other).into_bytes()),
                                    })
                                    .flatten();
                                row_writer.write_col(val)?;
                            }
                            row_writer.end_row().await?;
                        }
                        row_writer.finish().await?;
                    }
                    _ => {
                        let affected = result.affected_rows();
                        let last_insert = result.last_insert_id().unwrap_or(0);
                        drop(result);
                        let ok = OkResponse {
                            affected_rows: affected,
                            last_insert_id: last_insert,
                            ..Default::default()
                        };
                        results.completed(ok).await?;
                    }
                }
                Ok(())
            }
            Err(e) => {
                let msg = format!("{}", e);
                results.error(ErrorKind::ER_UNKNOWN_ERROR, msg.as_bytes()).await?;
                Ok(())
            }
        }
    }

    async fn on_close(&mut self, stmt: u32)
    where
        W: 'async_trait,
    {
        debug!(conn_id = self.conn_id, stmt_id = stmt, "CLOSE statement");
        self.prepared_stmts.remove(&stmt);
    }

    async fn on_init<'a>(
        &'a mut self,
        db_name: &'a str,
        writer: InitWriter<'a, W>,
    ) -> io::Result<()> {
        info!(conn_id = self.conn_id, db = %db_name, "USE database");

        // If no upstream connection yet, establish one with the target database.
        if self.upstream.is_none() {
            let user = self.upstream_user.clone();
            let password = self.upstream_password.clone();
            match self
                .connect_upstream(&user, &password, Some(db_name))
                .await
            {
                Ok(()) => {
                    info!(
                        conn_id = self.conn_id,
                        db = %db_name,
                        "Connected to upstream on USE db"
                    );
                    self.refresh_dirty_tables();
                    writer.ok().await?;
                    return Ok(());
                }
                Err(e) => {
                    let msg = format!("Failed to connect to upstream: {}", e);
                    writer
                        .error(ErrorKind::ER_BAD_DB_ERROR, msg.as_bytes())
                        .await?;
                    return Ok(());
                }
            }
        }

        // Forward USE to upstream
        if let Some(ref mut conn) = self.upstream {
            let sql = format!("USE `{}`", db_name);
            match conn.query_drop(&sql).await {
                Ok(()) => {
                    self.current_db = Some(db_name.to_string());
                    self.refresh_dirty_tables();
                    writer.ok().await?;
                }
                Err(e) => {
                    let msg = format!("{}", e);
                    writer
                        .error(ErrorKind::ER_BAD_DB_ERROR, msg.as_bytes())
                        .await?;
                }
            }
        } else {
            writer
                .error(ErrorKind::ER_UNKNOWN_ERROR, b"No upstream connection")
                .await?;
        }

        Ok(())
    }

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        debug!(conn_id = self.conn_id, sql = %query, "QUERY");

        // If no upstream connection yet, establish one using configured credentials.
        if self.upstream.is_none() {
            let user = self.upstream_user.clone();
            let password = self.upstream_password.clone();
            let db = self.current_db.clone();
            match self.connect_upstream(&user, &password, db.as_deref()).await {
                Ok(()) => {
                    info!(
                        conn_id = self.conn_id,
                        user = %user,
                        "Connected to upstream"
                    );
                }
                Err(e) => {
                    warn!(conn_id = self.conn_id, error = %e, "Failed to connect upstream");
                    let msg = format!("Failed to connect to upstream: {}", e);
                    results
                        .error(ErrorKind::ER_UNKNOWN_ERROR, msg.as_bytes())
                        .await?;
                    return Ok(());
                }
            }
        }

        // Intercept SELECT LAST_INSERT_ID() and return the overlay's cached value.
        {
            let upper = query.trim().to_ascii_uppercase();
            if upper.starts_with("SELECT") && upper.contains("LAST_INSERT_ID()") {
                let id = self.last_insert_id.unwrap_or(0);
                let cols = vec![Column {
                    table: String::new(),
                    column: "LAST_INSERT_ID()".to_string(),
                    coltype: ColumnType::MYSQL_TYPE_LONGLONG,
                    colflags: ColumnFlags::empty(),
                }];
                let mut rw = results.start(&cols).await?;
                rw.write_col(Some(id.to_string().into_bytes()))?;
                rw.end_row().await?;
                rw.finish().await?;
                return Ok(());
            }
        }

        // Refresh dirty tables on each query (could be optimized later)
        self.refresh_dirty_tables();

        // Refresh PK column map so the rewriter uses the correct column per table.
        self.refresh_pk_columns();

        // Route the query
        if !self.truncated_tables.is_empty() {
            info!(
                conn_id = self.conn_id,
                truncated = ?self.truncated_tables,
                "Routing with truncated tables"
            );
        }
        let action = query_handler::route_query(query, &self.dirty_tables, &self.pk_columns, &self.truncated_tables);

        match action {
            QueryAction::Passthrough(sql) => {
                debug!(conn_id = self.conn_id, "Passthrough query");
                self.forward_query(&sql, results).await?;
            }

            QueryAction::RewrittenSelect(sql) => {
                debug!(conn_id = self.conn_id, rewritten = %sql, "Rewritten SELECT");
                // Populate temp tables for all currently-dirty tables before
                // forwarding the rewritten query that references them.
                let tables_snapshot = self.dirty_tables.clone();
                self.populate_temp_tables(&tables_snapshot).await?;
                self.forward_query(&sql, results).await?;
            }

            QueryAction::OverlayHandled(overlay_result) => {
                debug!(conn_id = self.conn_id, "Overlay-handled write");

                // Ensure FK relations are loaded before any write processing.
                if let Err(e) = self.ensure_fk_relations_loaded().await {
                    warn!(conn_id = self.conn_id, error = %e, "Failed to load FK relations");
                    // Continue without FK enforcement rather than failing the write entirely
                }

                // Try to handle INSERT/UPDATE/DELETE via the overlay writer.
                // Returns Result<Option<(WriteResult, table_name)>, String> —
                // Err holds a client-facing error message (e.g. FK RESTRICT violation).
                let write_result: Result<Option<(crate::overlay::writer::WriteResult, String)>, String> = 'overlay: {
                    // Re-parse the SQL to get the AST
                    let sql = &overlay_result.message;
                    let parsed = match crate::sql::parser::parse_query(sql) {
                        Ok(kind) => kind,
                        Err(e) => {
                            warn!(conn_id = self.conn_id, error = %e, "Failed to re-parse overlay SQL");
                            break 'overlay Ok(None);
                        }
                    };

                    use crate::sql::parser::QueryKind;

                    match parsed {
                        QueryKind::Insert(stmt) => {
                            // Extract table name for schema fetch
                            let table_name = match &stmt {
                                sqlparser::ast::Statement::Insert(insert) => {
                                    match &insert.table {
                                        sqlparser::ast::TableObject::TableName(name) => {
                                            name.0.last()
                                                .and_then(|p| p.as_ident())
                                                .map(|i| i.value.clone())
                                        }
                                        _ => None,
                                    }
                                }
                                _ => None,
                            };

                            let table_name = match table_name {
                                Some(t) => t,
                                None => {
                                    warn!(conn_id = self.conn_id, "Could not extract table name from INSERT");
                                    break 'overlay Ok(None);
                                }
                            };

                            // Detect INSERT ... SELECT: source is a Query whose body is
                            // not a simple VALUES clause. Fall through to passthrough so
                            // we don't crash; full overlay support is a TODO.
                            if let sqlparser::ast::Statement::Insert(insert) = &stmt {
                                if let Some(source) = &insert.source {
                                    let is_values = matches!(
                                        source.body.as_ref(),
                                        sqlparser::ast::SetExpr::Values(_)
                                    );
                                    if !is_values {
                                        warn!(
                                            conn_id = self.conn_id,
                                            table = %table_name,
                                            "INSERT ... SELECT detected — falling through to passthrough (TODO: full overlay support)"
                                        );
                                        break 'overlay Ok(None);
                                    }
                                }
                            }

                            // Detect INSERT ... ON DUPLICATE KEY UPDATE: for now fall
                            // through to passthrough to avoid crashes. Full UPSERT
                            // support (try INSERT, catch PK conflict, do UPDATE) is a TODO.
                            if let sqlparser::ast::Statement::Insert(insert) = &stmt {
                                if let Some(sqlparser::ast::OnInsert::DuplicateKeyUpdate(_)) = &insert.on {
                                    warn!(
                                        conn_id = self.conn_id,
                                        table = %table_name,
                                        "INSERT ... ON DUPLICATE KEY UPDATE detected — falling through to passthrough (TODO: full overlay support)"
                                    );
                                    break 'overlay Ok(None);
                                }
                            }

                            // Fetch (or retrieve from cache) schema + defaults.
                            let cached_schema = match self.get_or_fetch_schema(&table_name).await {
                                Ok(s) => s,
                                Err(e) => {
                                    warn!(conn_id = self.conn_id, error = %e, "Failed to fetch table schema");
                                    break 'overlay Ok(None);
                                }
                            };
                            let schema = cached_schema.schema_pairs();
                            let defaults = cached_schema.defaults();

                            // Open overlay store and execute insert
                            let db = match &self.current_db {
                                Some(db) => db.clone(),
                                None => {
                                    warn!(conn_id = self.conn_id, "No database selected for overlay write");
                                    break 'overlay Ok(None);
                                }
                            };

                            match OverlayStore::open(&self.overlay_dir, &db) {
                                Ok(store) => {
                                    match writer::execute_insert(&store, &stmt, &schema, &defaults) {
                                        Ok(wr) => Ok(Some((wr, table_name.clone()))),
                                        Err(e) => {
                                            warn!(conn_id = self.conn_id, error = %e, "Overlay INSERT failed");
                                            break 'overlay Ok(None);
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!(conn_id = self.conn_id, error = %e, "Failed to open overlay store");
                                    break 'overlay Ok(None);
                                }
                            }
                        }

                        QueryKind::Update(stmt) => {
                            // Extract table name and WHERE clause from UPDATE
                            let (table_name, where_clause) = match &stmt {
                                sqlparser::ast::Statement::Update(update) => {
                                    let tname = match &update.table.relation {
                                        sqlparser::ast::TableFactor::Table { name, .. } => {
                                            name.0.last()
                                                .and_then(|p| p.as_ident())
                                                .map(|i| i.value.clone())
                                        }
                                        _ => None,
                                    };
                                    (tname, update.selection.as_ref().map(|e| e.to_string()))
                                }
                                _ => (None, None),
                            };

                            let table_name = match table_name {
                                Some(t) => t,
                                None => {
                                    warn!(conn_id = self.conn_id, "Could not extract table name from UPDATE");
                                    break 'overlay Ok(None);
                                }
                            };

                            // Fetch (or retrieve from cache) schema.
                            let schema = match self.get_or_fetch_schema(&table_name).await {
                                Ok(s) => s.schema_pairs(),
                                Err(e) => {
                                    warn!(conn_id = self.conn_id, error = %e, "Failed to fetch table schema");
                                    break 'overlay Ok(None);
                                }
                            };

                            // Fetch affected rows from upstream: SELECT * FROM <table> WHERE <where>
                            let select_sql = match &where_clause {
                                Some(w) => format!("SELECT * FROM `{}` WHERE {}", table_name, w),
                                None => format!("SELECT * FROM `{}`", table_name),
                            };

                            let upstream_rows: Vec<Vec<(String, String)>> = if let Some(ref mut conn) = self.upstream {
                                let rows: Vec<Row> = match conn.query(&select_sql).await {
                                    Ok(r) => r,
                                    Err(e) => {
                                        warn!(conn_id = self.conn_id, error = %e, "Failed to fetch upstream rows for UPDATE");
                                        break 'overlay Ok(None);
                                    }
                                };

                                rows.iter().map(|row| {
                                    schema.iter().enumerate().map(|(i, (col_name, _))| {
                                        let val = row.as_ref(i)
                                            .map(|v| match v {
                                                mysql_async::Value::NULL => "NULL".to_string(),
                                                mysql_async::Value::Bytes(b) => String::from_utf8_lossy(b).to_string(),
                                                mysql_async::Value::Int(n) => n.to_string(),
                                                mysql_async::Value::UInt(n) => n.to_string(),
                                                mysql_async::Value::Float(f) => f.to_string(),
                                                mysql_async::Value::Double(d) => d.to_string(),
                                                other => format!("{:?}", other),
                                            })
                                            .unwrap_or_else(|| "NULL".to_string());
                                        (col_name.clone(), val)
                                    }).collect()
                                }).collect()
                            } else {
                                break 'overlay Ok(None);
                            };

                            // Open overlay store and execute update
                            let db = match &self.current_db {
                                Some(db) => db.clone(),
                                None => {
                                    warn!(conn_id = self.conn_id, "No database selected for overlay write");
                                    break 'overlay Ok(None);
                                }
                            };

                            match OverlayStore::open(&self.overlay_dir, &db) {
                                Ok(store) => {
                                    match writer::execute_update(&store, &stmt, &schema, &upstream_rows) {
                                        Ok(wr) => Ok(Some((wr, table_name.clone()))),
                                        Err(e) => {
                                            warn!(conn_id = self.conn_id, error = %e, "Overlay UPDATE failed");
                                            break 'overlay Ok(None);
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!(conn_id = self.conn_id, error = %e, "Failed to open overlay store");
                                    break 'overlay Ok(None);
                                }
                            }
                        }

                        QueryKind::Delete(stmt) => {
                            // Extract table name and WHERE clause from DELETE
                            let (table_name, where_clause) = match &stmt {
                                sqlparser::ast::Statement::Delete(delete) => {
                                    let tables = match &delete.from {
                                        sqlparser::ast::FromTable::WithFromKeyword(t) => t,
                                        sqlparser::ast::FromTable::WithoutKeyword(t) => t,
                                    };
                                    let tname = tables.first().and_then(|twj| {
                                        match &twj.relation {
                                            sqlparser::ast::TableFactor::Table { name, .. } => {
                                                name.0.last()
                                                    .and_then(|p| p.as_ident())
                                                    .map(|i| i.value.clone())
                                            }
                                            _ => None,
                                        }
                                    });
                                    (tname, delete.selection.as_ref().map(|e| e.to_string()))
                                }
                                _ => (None, None),
                            };

                            let table_name = match table_name {
                                Some(t) => t,
                                None => {
                                    warn!(conn_id = self.conn_id, "Could not extract table name from DELETE");
                                    break 'overlay Ok(None);
                                }
                            };

                            // Fetch (or retrieve from cache) schema.
                            let cached_schema = match self.get_or_fetch_schema(&table_name).await {
                                Ok(s) => s,
                                Err(e) => {
                                    warn!(conn_id = self.conn_id, error = %e, "Failed to fetch table schema");
                                    break 'overlay Ok(None);
                                }
                            };
                            let schema = cached_schema.schema_pairs();
                            let pk_col = cached_schema.pk_column();

                            // Fetch affected PKs from upstream: SELECT <pk> FROM <table> WHERE <where>
                            let select_sql = match &where_clause {
                                Some(w) => format!("SELECT `{}` FROM `{}` WHERE {}", pk_col, table_name, w),
                                None => format!("SELECT `{}` FROM `{}`", pk_col, table_name),
                            };

                            let upstream_pks: Vec<String> = if let Some(ref mut conn) = self.upstream {
                                let rows: Vec<Row> = match conn.query(&select_sql).await {
                                    Ok(r) => r,
                                    Err(e) => {
                                        warn!(conn_id = self.conn_id, error = %e, "Failed to fetch upstream PKs for DELETE");
                                        break 'overlay Ok(None);
                                    }
                                };

                                rows.iter().filter_map(|row| {
                                    row.as_ref(0).map(|v| match v {
                                        mysql_async::Value::NULL => "NULL".to_string(),
                                        mysql_async::Value::Bytes(b) => String::from_utf8_lossy(b).to_string(),
                                        mysql_async::Value::Int(n) => n.to_string(),
                                        mysql_async::Value::UInt(n) => n.to_string(),
                                        _ => format!("{:?}", v),
                                    })
                                }).collect()
                            } else {
                                break 'overlay Ok(None);
                            };

                            // Enforce FK constraints before executing delete.
                            // Clone child FKs to avoid borrow issues with &mut self.
                            let child_fks: Vec<ForeignKeyInfo> = self
                                .schema_cache
                                .get_child_fks(&table_name)
                                .into_iter()
                                .cloned()
                                .collect();

                            if !child_fks.is_empty() && !upstream_pks.is_empty() {
                                match self.enforce_fk_delete(&table_name, &upstream_pks, child_fks).await {
                                    Ok(fk_result) => {
                                        // Apply SET NULL updates
                                        for (child_table, fk_col, child_pks) in &fk_result.set_null_updates {
                                            let child_schema = match self.get_or_fetch_schema(child_table).await {
                                                Ok(s) => s,
                                                Err(e) => {
                                                    warn!(conn_id = self.conn_id, error = %e, "FK SET NULL schema fetch failed");
                                                    continue;
                                                }
                                            };
                                            let child_schema_pairs = child_schema.schema_pairs();
                                            let child_pk_col = child_schema.pk_column();

                                            let db = match &self.current_db {
                                                Some(db) => db.clone(),
                                                None => continue,
                                            };

                                            // Fetch full rows from upstream for affected children
                                            let in_values: Vec<String> = child_pks
                                                .iter()
                                                .map(|pk| format!("'{}'", pk.replace('\'', "''")))
                                                .collect();
                                            let in_clause = in_values.join(",");
                                            let col_names: Vec<String> = child_schema_pairs.iter().map(|(n, _)| format!("`{}`", n)).collect();
                                            let fetch_sql = format!(
                                                "SELECT {} FROM `{}` WHERE `{}` IN ({})",
                                                col_names.join(", "), child_table, child_pk_col, in_clause
                                            );

                                            let child_upstream_rows: Vec<Vec<(String, String)>> = if let Some(ref mut conn) = self.upstream {
                                                match conn.query::<Row, _>(&fetch_sql).await {
                                                    Ok(rows) => rows.iter().map(|row| {
                                                        child_schema_pairs.iter().enumerate().map(|(i, (col_name, _))| {
                                                            let val = row.as_ref(i).map(|v| match v {
                                                                mysql_async::Value::NULL => "NULL".to_string(),
                                                                mysql_async::Value::Bytes(b) => String::from_utf8_lossy(b).to_string(),
                                                                mysql_async::Value::Int(n) => n.to_string(),
                                                                mysql_async::Value::UInt(n) => n.to_string(),
                                                                _ => format!("{:?}", v),
                                                            }).unwrap_or_else(|| "NULL".to_string());
                                                            (col_name.clone(), val)
                                                        }).collect()
                                                    }).collect(),
                                                    Err(e) => {
                                                        warn!(conn_id = self.conn_id, error = %e, "FK SET NULL upstream fetch failed");
                                                        continue;
                                                    }
                                                }
                                            } else {
                                                continue;
                                            };

                                            // Write SET NULL overlay rows
                                            {
                                                let store = match OverlayStore::open(&self.overlay_dir, &db) {
                                                    Ok(s) => s,
                                                    Err(e) => {
                                                        warn!(conn_id = self.conn_id, error = %e, "FK SET NULL overlay open failed");
                                                        continue;
                                                    }
                                                };
                                                let row_store = crate::overlay::row_store::RowStore::new(&store.conn);
                                                let col_refs: Vec<(&str, &str)> = child_schema_pairs.iter().map(|(n, t)| (n.as_str(), t.as_str())).collect();
                                                if let Err(e) = row_store.ensure_shadow_table(child_table, &col_refs) {
                                                    warn!(conn_id = self.conn_id, error = %e, "FK SET NULL ensure shadow failed");
                                                    continue;
                                                }
                                                let shadow = format!("_cow_data_{}", child_table);

                                                for upstream_row in &child_upstream_rows {
                                                    let cow_pk = upstream_row.iter()
                                                        .find(|(c, _)| c.eq_ignore_ascii_case(&child_pk_col))
                                                        .map(|(_, v)| v.clone())
                                                        .unwrap_or_else(|| "unknown".to_string());

                                                    // Build column names and values for INSERT OR REPLACE
                                                    let mut col_names_sql = vec!["_cow_pk".to_string(), "_cow_op".to_string()];
                                                    let mut placeholders = vec!["?".to_string(), "'UPDATE'".to_string()];
                                                    let mut values: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(cow_pk.clone())];

                                                    for (col_name, col_val) in upstream_row {
                                                        col_names_sql.push(format!("\"{}\"", col_name));
                                                        if col_name.eq_ignore_ascii_case(fk_col) {
                                                            placeholders.push("NULL".to_string());
                                                        } else {
                                                            placeholders.push("?".to_string());
                                                            values.push(Box::new(col_val.clone()));
                                                        }
                                                    }

                                                    let upsert_sql = format!(
                                                        "INSERT OR REPLACE INTO \"{}\" ({}) VALUES ({})",
                                                        shadow,
                                                        col_names_sql.join(", "),
                                                        placeholders.join(", ")
                                                    );
                                                    let params: Vec<&dyn rusqlite::types::ToSql> = values.iter().map(|v| v.as_ref()).collect();
                                                    if let Err(e) = store.conn.execute(&upsert_sql, params.as_slice()) {
                                                        warn!(conn_id = self.conn_id, error = %e, "FK SET NULL upsert failed");
                                                    }
                                                }

                                                let reg = crate::overlay::registry::Registry::new(&store.conn);
                                                let _ = reg.mark_dirty(child_table, crate::overlay::registry::DirtyKind::Data);
                                            }
                                            self.temp_tables.invalidate(child_table);
                                        }

                                        // Apply cascaded deletes
                                        for (child_table, child_pks) in &fk_result.cascaded_deletes {
                                            let child_schema = match self.get_or_fetch_schema(child_table).await {
                                                Ok(s) => s,
                                                Err(e) => {
                                                    warn!(conn_id = self.conn_id, error = %e, "FK CASCADE schema fetch failed");
                                                    continue;
                                                }
                                            };
                                            let child_schema_pairs = child_schema.schema_pairs();

                                            let db = match &self.current_db {
                                                Some(db) => db.clone(),
                                                None => continue,
                                            };

                                            // Write tombstones for cascaded child rows
                                            {
                                                let store = match OverlayStore::open(&self.overlay_dir, &db) {
                                                    Ok(s) => s,
                                                    Err(e) => {
                                                        warn!(conn_id = self.conn_id, error = %e, "FK CASCADE overlay open failed");
                                                        continue;
                                                    }
                                                };
                                                let row_store = crate::overlay::row_store::RowStore::new(&store.conn);
                                                let col_refs: Vec<(&str, &str)> = child_schema_pairs.iter().map(|(n, t)| (n.as_str(), t.as_str())).collect();
                                                if let Err(e) = row_store.ensure_shadow_table(child_table, &col_refs) {
                                                    warn!(conn_id = self.conn_id, error = %e, "FK CASCADE ensure shadow failed");
                                                    continue;
                                                }
                                                let shadow = format!("_cow_data_{}", child_table);

                                                for pk in child_pks {
                                                    let existing_op: Option<String> = store.conn.query_row(
                                                        &format!("SELECT _cow_op FROM \"{}\" WHERE _cow_pk = ?1", shadow),
                                                        rusqlite::params![pk],
                                                        |r| r.get(0),
                                                    ).ok();

                                                    let result = match existing_op.as_deref() {
                                                        Some("INSERT") => store.conn.execute(
                                                            &format!("DELETE FROM \"{}\" WHERE _cow_pk = ?1", shadow),
                                                            rusqlite::params![pk],
                                                        ),
                                                        Some(_) => store.conn.execute(
                                                            &format!("UPDATE \"{}\" SET _cow_op = 'DELETE' WHERE _cow_pk = ?1", shadow),
                                                            rusqlite::params![pk],
                                                        ),
                                                        None => store.conn.execute(
                                                            &format!("INSERT INTO \"{}\" (_cow_pk, _cow_op) VALUES (?1, 'DELETE')", shadow),
                                                            rusqlite::params![pk],
                                                        ),
                                                    };
                                                    if let Err(e) = result {
                                                        warn!(conn_id = self.conn_id, error = %e, "FK CASCADE tombstone write failed");
                                                    }
                                                }

                                                let reg = crate::overlay::registry::Registry::new(&store.conn);
                                                let _ = reg.mark_dirty(child_table, crate::overlay::registry::DirtyKind::Data);
                                            }
                                            self.temp_tables.invalidate(child_table);

                                            // Recurse: child table might also have children
                                            if let Err(e) = self.enforce_fk_delete_recursive(child_table, child_pks, 1).await {
                                                warn!(conn_id = self.conn_id, error = %e, "FK recursive cascade failed");
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        // RESTRICT or other FK violation — return error to client
                                        break 'overlay Err(e.to_string());
                                    }
                                }
                            }

                            // Open overlay store and execute delete
                            let db = match &self.current_db {
                                Some(db) => db.clone(),
                                None => {
                                    warn!(conn_id = self.conn_id, "No database selected for overlay write");
                                    break 'overlay Ok(None);
                                }
                            };

                            match OverlayStore::open(&self.overlay_dir, &db) {
                                Ok(store) => {
                                    match writer::execute_delete(&store, &stmt, &schema, &upstream_pks) {
                                        Ok(wr) => Ok(Some((wr, table_name.clone()))),
                                        Err(e) => {
                                            warn!(conn_id = self.conn_id, error = %e, "Overlay DELETE failed");
                                            break 'overlay Ok(None);
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!(conn_id = self.conn_id, error = %e, "Failed to open overlay store");
                                    break 'overlay Ok(None);
                                }
                            }
                        }

                        QueryKind::Ddl(stmt) => {
                            // Handle CREATE TABLE / ALTER TABLE / DROP TABLE in the overlay
                            let sql = &overlay_result.message;

                            // Extract table name for cache invalidation.
                            let ddl_table_name: String = match &stmt {
                                sqlparser::ast::Statement::CreateTable(c) => {
                                    c.name.0.last()
                                        .and_then(|p| p.as_ident())
                                        .map(|i| i.value.clone())
                                        .unwrap_or_default()
                                }
                                sqlparser::ast::Statement::AlterTable(alter) => {
                                    alter.name.0.last()
                                        .and_then(|p| p.as_ident())
                                        .map(|i| i.value.clone())
                                        .unwrap_or_default()
                                }
                                sqlparser::ast::Statement::Drop { names, .. } => {
                                    names.first()
                                        .and_then(|n| n.0.last())
                                        .and_then(|p| p.as_ident())
                                        .map(|i| i.value.clone())
                                        .unwrap_or_default()
                                }
                                sqlparser::ast::Statement::Truncate(truncate) => {
                                    truncate.table_names.first()
                                        .and_then(|t| t.name.0.last())
                                        .and_then(|p| p.as_ident())
                                        .map(|i| i.value.clone())
                                        .unwrap_or_default()
                                }
                                _ => String::new(),
                            };

                            let db = match &self.current_db {
                                Some(db) => db.clone(),
                                None => {
                                    warn!(conn_id = self.conn_id, "No database selected for DDL overlay write");
                                    break 'overlay Ok(None);
                                }
                            };

                            match OverlayStore::open(&self.overlay_dir, &db) {
                                Ok(store) => {
                                    match writer::execute_ddl(&store, sql, &stmt) {
                                        Ok(wr) => {
                                            // Invalidate schema cache so the next access re-fetches.
                                            if !ddl_table_name.is_empty() {
                                                self.schema_cache.invalidate(&ddl_table_name);
                                            }
                                            Ok(Some((wr, ddl_table_name)))
                                        }
                                        Err(e) => {
                                            warn!(conn_id = self.conn_id, error = %e, "Overlay DDL failed");
                                            break 'overlay Ok(None);
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!(conn_id = self.conn_id, error = %e, "Failed to open overlay store for DDL");
                                    break 'overlay Ok(None);
                                }
                            }
                        }

                        _ => {
                            // Other overlay-handled statements — not yet implemented
                            break 'overlay Ok(None);
                        }
                    }
                };

                // Handle FK RESTRICT violations: send error to client and return.
                let write_result = match write_result {
                    Ok(v) => v,
                    Err(fk_err) => {
                        results
                            .error(ErrorKind::ER_ROW_IS_REFERENCED_2, fk_err.as_bytes())
                            .await?;
                        return Ok(());
                    }
                };

                let (affected, last_id) = match write_result {
                    Some((wr, ref table_name)) => {
                        // Invalidate temp-table cache so the next SELECT rebuilds it.
                        if !table_name.is_empty() {
                            self.temp_tables.invalidate(table_name);
                        }
                        // Cache last insert ID for LAST_INSERT_ID() interception.
                        if wr.last_insert_id.is_some() {
                            self.last_insert_id = wr.last_insert_id;
                        }
                        (wr.affected_rows, wr.last_insert_id.unwrap_or(0) as u64)
                    }
                    None => (overlay_result.affected_rows, overlay_result.last_insert_id.unwrap_or(0) as u64),
                };

                let ok = OkResponse {
                    affected_rows: affected,
                    last_insert_id: last_id,
                    ..Default::default()
                };
                results.completed(ok).await?;
            }

            QueryAction::TransactionControl(op) => {
                debug!(conn_id = self.conn_id, op = ?op, "Transaction control");
                match op {
                    TransactionOp::Begin => {
                        self.tx_buffer.begin();
                        // Also forward BEGIN to upstream for passthrough queries
                        self.forward_query("BEGIN", results).await?;
                    }
                    TransactionOp::Commit => {
                        let _ops = self.tx_buffer.commit();
                        // TODO: apply buffered overlay ops on commit
                        self.forward_query("COMMIT", results).await?;
                    }
                    TransactionOp::Rollback => {
                        self.tx_buffer.rollback();
                        self.forward_query("ROLLBACK", results).await?;
                    }
                }
            }

            QueryAction::Call(sql) => {
                debug!(conn_id = self.conn_id, "CALL with dirty tables — rewriting SP body");

                // Extract procedure name from CALL statement
                let proc_name = sql
                    .trim()
                    .strip_prefix("CALL ")
                    .or_else(|| sql.trim().strip_prefix("call "))
                    .and_then(|rest| rest.split('(').next())
                    .map(|s| s.trim().to_string());

                let proc_name = match proc_name {
                    Some(name) => name,
                    None => {
                        // Can't parse — just passthrough
                        self.forward_query(&sql, results).await?;
                        return Ok(());
                    }
                };

                // Fetch SP body from upstream
                let sp_body = {
                    let conn = match self.upstream.as_mut() {
                        Some(c) => c,
                        None => {
                            results.error(ErrorKind::ER_UNKNOWN_ERROR, b"No upstream connection").await?;
                            return Ok(());
                        }
                    };
                    let show_sql = format!("SHOW CREATE PROCEDURE `{}`", proc_name);
                    let row: Option<mysql_async::Row> = conn.query_first(&show_sql).await
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Failed to fetch SP: {e}")))?;
                    match row {
                        Some(r) => {
                            // Column 2 is typically "Create Procedure"
                            let body: Option<String> = r.get(2);
                            body
                        }
                        None => None,
                    }
                };

                match sp_body {
                    Some(create_sql) => {
                        let dirty_refs: Vec<&str> = self.dirty_tables.iter().map(|s| s.as_str()).collect();
                        let truncated_refs: Vec<&str> = self.truncated_tables.iter().map(|s| s.as_str()).collect();
                        match crate::sql::sp_rewriter::rewrite_sp_from_definition_statements(
                            &create_sql, &dirty_refs, "_cow_temp_", &self.pk_columns, &truncated_refs,
                        ) {
                            Ok(stmts) if !stmts.is_empty() => {
                                debug!(conn_id = self.conn_id, stmt_count = stmts.len(), "Executing rewritten SP statements");
                                // Populate temp tables first
                                let tables_snapshot = self.dirty_tables.clone();
                                self.populate_temp_tables(&tables_snapshot).await?;
                                // Execute intermediate statements, discard results
                                if stmts.len() > 1 {
                                    for stmt_sql in &stmts[..stmts.len() - 1] {
                                        if let Some(conn) = self.upstream.as_mut() {
                                            let _ = conn.query_drop(stmt_sql).await;
                                        }
                                    }
                                }
                                // Forward last statement's result to client
                                let last_stmt = stmts.last().unwrap();
                                self.forward_query(last_stmt, results).await?;
                            }
                            Ok(_) => {
                                // Empty SP body — just return OK
                                results.completed(OkResponse::default()).await?;
                            }
                            Err(e) => {
                                warn!(conn_id = self.conn_id, error = %e, "SP rewrite failed, falling back to passthrough");
                                self.forward_query(&sql, results).await?;
                            }
                        }
                    }
                    None => {
                        warn!(conn_id = self.conn_id, "Could not fetch SP body, passthrough");
                        self.forward_query(&sql, results).await?;
                    }
                }
            }

            QueryAction::Rejected(msg) => {
                warn!(conn_id = self.conn_id, reason = %msg, "Query rejected");
                results
                    .error(ErrorKind::ER_PARSE_ERROR, msg.as_bytes())
                    .await?;
            }
        }

        Ok(())
    }
}

/// Convert an opensrv-mysql `ValueInner` (from a client EXECUTE packet) into a
/// `mysql_async::Value` that can be passed to `exec_iter`.
fn opensrv_value_to_mysql(v: ValueInner<'_>) -> MysqlValue {
    match v {
        ValueInner::NULL => MysqlValue::NULL,
        ValueInner::Int(i) => MysqlValue::Int(i),
        ValueInner::UInt(u) => MysqlValue::UInt(u),
        ValueInner::Double(d) => MysqlValue::Double(d),
        ValueInner::Bytes(b) => MysqlValue::Bytes(b.to_vec()),
        // Date/Time values arrive as raw binary-encoded bytes.
        // Convert them to Bytes so mysql_async re-serialises them as strings,
        // which is safe for all practical query types.
        ValueInner::Date(b) => MysqlValue::Bytes(b.to_vec()),
        ValueInner::Time(b) => MysqlValue::Bytes(b.to_vec()),
        ValueInner::Datetime(b) => MysqlValue::Bytes(b.to_vec()),
    }
}

/// Convert mysql_async ColumnType to opensrv-mysql ColumnType.
fn mysql_col_type_to_opensrv(ct: mysql_async::consts::ColumnType) -> ColumnType {
    // Both crates use the same MySQL protocol column type values,
    // so we can convert via the raw byte value.
    let raw = ct as u8;
    match ColumnType::try_from(raw) {
        Ok(t) => t,
        Err(_) => ColumnType::MYSQL_TYPE_VAR_STRING, // safe fallback
    }
}

/// Convert mysql_async ColumnFlags to opensrv-mysql ColumnFlags.
fn mysql_flags_to_opensrv(flags: mysql_async::consts::ColumnFlags) -> ColumnFlags {
    // Both use the same bitflags values from the MySQL protocol.
    ColumnFlags::from_bits_truncate(flags.bits() as u16)
}

/// Returns true if the mysql_async error indicates a dropped or broken connection
/// that may be recoverable by reconnecting.
fn is_connection_error(e: &mysql_async::Error) -> bool {
    match e {
        mysql_async::Error::Driver(mysql_async::DriverError::ConnectionClosed) => true,
        mysql_async::Error::Io(io_err) => {
            // IoError::Io wraps a std::io::Error — check for connection-reset/broken-pipe kinds.
            let msg = io_err.to_string();
            msg.contains("Broken pipe")
                || msg.contains("Connection reset")
                || msg.contains("connection reset")
                || msg.contains("broken pipe")
                || msg.contains("EOF")
                || msg.contains("os error 32")
                || msg.contains("os error 104")
        }
        _ => false,
    }
}
