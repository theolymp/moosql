use mysql_async::prelude::*;
use mysql_async::Conn;
use std::collections::HashMap;

pub struct TempTableManager {
    /// Tracks which tables have been created in this session and their version.
    created: HashMap<String, u64>,
}

pub struct TempRow {
    pub pk: String,
    pub op: String,
    pub values: Vec<String>,
}

impl TempTableManager {
    pub fn new() -> Self {
        Self {
            created: HashMap::new(),
        }
    }

    /// Creates two temp tables per dirty base table:
    /// - `_cow_temp_<table>`: data rows only (inserts + updates, NO tombstones).
    ///   Schema matches the base table exactly (no extra columns).
    /// - `_cow_meta_<table>`: all overlay PKs (`_cow_pk TEXT, _cow_op TEXT`),
    ///   used for the `NOT IN` filter to exclude overridden base rows.
    ///
    /// Integer column types are upgraded to BIGINT so overlay IDs (counting
    /// down from i64::MAX) never overflow.
    pub async fn ensure_temp_table(
        &mut self,
        conn: &mut Conn,
        table: &str,
        columns: &[(&str, &str)],
        rows: &[TempRow],
    ) -> anyhow::Result<()> {
        let data_name = format!("_cow_temp_{}", table);
        let meta_name = format!("_cow_meta_{}", table);

        // Drop both if they exist.
        conn.query_drop(format!("DROP TEMPORARY TABLE IF EXISTS `{}`", data_name))
            .await?;
        conn.query_drop(format!("DROP TEMPORARY TABLE IF EXISTS `{}`", meta_name))
            .await?;

        // --- Create data table (same columns as base, no _cow_pk/_cow_op) ---
        let col_defs: String = columns
            .iter()
            .map(|(name, ty)| {
                let upgraded = upgrade_int_type(ty);
                format!("`{}` {}", name, upgraded)
            })
            .collect::<Vec<_>>()
            .join(", ");

        let create_data_sql = if col_defs.is_empty() {
            // Degenerate case: base table has no columns — shouldn't happen in practice.
            format!(
                "CREATE TEMPORARY TABLE `{}` (`_cow_dummy` TINYINT)",
                data_name
            )
        } else {
            format!(
                "CREATE TEMPORARY TABLE `{}` ({})",
                data_name, col_defs
            )
        };
        conn.query_drop(create_data_sql).await?;

        // --- Create meta table ---
        let create_meta_sql = format!(
            "CREATE TEMPORARY TABLE `{}` (\
                `_cow_pk` VARCHAR(255) NOT NULL, \
                `_cow_op` VARCHAR(10) NOT NULL\
            )",
            meta_name
        );
        conn.query_drop(create_meta_sql).await?;

        // --- Populate both tables ---
        // Build meta and data inserts as bulk statements (VALUES (?,?),(?,?),...).
        // Meta table: all ops (insert, update, delete).
        // Data table: non-tombstone rows only.

        if !rows.is_empty() {
            // --- Bulk meta insert ---
            let meta_placeholders = std::iter::repeat("(?, ?)")
                .take(rows.len())
                .collect::<Vec<_>>()
                .join(", ");
            let meta_insert = format!(
                "INSERT INTO `{}` (`_cow_pk`, `_cow_op`) VALUES {}",
                meta_name, meta_placeholders
            );
            let meta_params: Vec<mysql_async::Value> = rows
                .iter()
                .flat_map(|row| {
                    vec![
                        mysql_async::Value::Bytes(row.pk.as_bytes().to_vec()),
                        mysql_async::Value::Bytes(row.op.as_bytes().to_vec()),
                    ]
                })
                .collect();
            conn.exec_drop(meta_insert, meta_params).await?;

            // --- Bulk data insert (non-tombstone rows only) ---
            if !columns.is_empty() {
                let col_names: String = columns
                    .iter()
                    .map(|(name, _)| format!("`{}`", name))
                    .collect::<Vec<_>>()
                    .join(", ");

                let row_placeholder = format!(
                    "({})",
                    std::iter::repeat("?")
                        .take(columns.len())
                        .collect::<Vec<_>>()
                        .join(", ")
                );

                let data_rows: Vec<&TempRow> = rows
                    .iter()
                    .filter(|row| row.op.to_lowercase() != "delete")
                    .collect();

                if !data_rows.is_empty() {
                    let all_placeholders = std::iter::repeat(row_placeholder.as_str())
                        .take(data_rows.len())
                        .collect::<Vec<_>>()
                        .join(", ");
                    let data_insert = format!(
                        "INSERT INTO `{}` ({}) VALUES {}",
                        data_name, col_names, all_placeholders
                    );
                    let data_params: Vec<mysql_async::Value> = data_rows
                        .iter()
                        .flat_map(|row| {
                            row.values.iter().map(|v| {
                                if v == "NULL" || v.is_empty() {
                                    mysql_async::Value::NULL
                                } else {
                                    mysql_async::Value::Bytes(v.as_bytes().to_vec())
                                }
                            })
                        })
                        .collect();
                    conn.exec_drop(data_insert, data_params).await?;
                }
            }
        }

        self.created.insert(table.to_string(), 1);
        Ok(())
    }

    /// Removes the given table from the tracking map.
    pub fn invalidate(&mut self, table: &str) {
        self.created.remove(table);
    }

    /// Clears all tracked tables.
    #[allow(dead_code)]
    pub fn invalidate_all(&mut self) {
        self.created.clear();
    }

    /// Returns true if the temp table for `table` has already been created this session.
    pub fn is_current(&self, table: &str) -> bool {
        self.created.contains_key(table)
    }
}

/// If the column type contains "int" (case-insensitive), upgrade to BIGINT
/// so that overlay IDs counting down from i64::MAX don't overflow.
fn upgrade_int_type(ty: &str) -> String {
    let lower = ty.to_lowercase();
    if lower.contains("int") && !lower.starts_with("big") {
        "BIGINT".to_string()
    } else {
        ty.to_string()
    }
}
