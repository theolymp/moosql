use std::path::Path;

use anyhow::{Context, Result};

use crate::overlay::registry::Registry;
use crate::overlay::row_store::RowStore;
use crate::overlay::store::OverlayStore;

/// Represents the changes found in one overlay table.
#[derive(Debug)]
pub struct TableDiff {
    pub db_name: String,
    pub table_name: String,
    /// Each insert row is a list of (column, value) pairs (user columns only).
    pub inserts: Vec<Vec<(String, String)>>,
    /// Each update row is a list of (column, value) pairs (user columns only).
    pub updates: Vec<Vec<(String, String)>>,
    /// Primary key values for deleted rows.
    pub deletes: Vec<String>,
    /// If the table has a schema overlay, store the DDL here.
    pub schema_change: Option<String>,
    /// Whether the table was truncated in the overlay.
    pub truncated: bool,
}

/// Output format for the diff command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffFormat {
    Text,
    Sql,
}

/// Collect diffs from all overlay databases.
pub fn collect_diffs(
    overlay_dir: &Path,
    db_filter: Option<&str>,
    table_filter: Option<&str>,
) -> Result<Vec<TableDiff>> {
    if !overlay_dir.exists() {
        return Ok(vec![]);
    }

    let mut diffs = Vec::new();

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

        if let Some(filter) = db_filter {
            if db_name != filter {
                continue;
            }
        }

        let store = OverlayStore::open(overlay_dir, &db_name)
            .with_context(|| format!("Failed to open overlay for database '{db_name}'"))?;
        let reg = Registry::new(&store.conn);
        let row_store = RowStore::new(&store.conn);
        let dirty = reg.list_dirty()?;

        for info in dirty {
            if let Some(filter) = table_filter {
                if info.table_name != filter {
                    continue;
                }
            }

            let truncated = reg.is_truncated(&info.table_name).unwrap_or(false);

            // Read schema overlay if present.
            let schema_change = if info.has_schema {
                let schema: Option<String> = store
                    .conn
                    .query_row(
                        "SELECT overlay_schema FROM _cow_tables WHERE table_name = ?1",
                        [&info.table_name],
                        |row| row.get(0),
                    )
                    .ok()
                    .flatten();
                schema
            } else {
                None
            };

            let mut inserts = Vec::new();
            let mut updates = Vec::new();
            let mut deletes = Vec::new();

            if info.has_data {
                let rows = row_store.get_all_overlay_data(&info.table_name)?;

                for row in rows {
                    // Find _cow_op and _cow_pk (clone to avoid borrow issues).
                    let op = row
                        .iter()
                        .find(|(k, _)| k == "_cow_op")
                        .map(|(_, v)| v.clone())
                        .unwrap_or_default();
                    let pk = row
                        .iter()
                        .find(|(k, _)| k == "_cow_pk")
                        .map(|(_, v)| v.clone())
                        .unwrap_or_default();

                    // Filter out internal columns for user-visible data.
                    let user_cols: Vec<(String, String)> = row
                        .into_iter()
                        .filter(|(k, _)| !k.starts_with("_cow_"))
                        .collect();

                    match op.as_str() {
                        "INSERT" => inserts.push(user_cols),
                        "UPDATE" => updates.push(user_cols),
                        "DELETE" => deletes.push(pk),
                        _ => {}
                    }
                }
            }

            diffs.push(TableDiff {
                db_name: db_name.clone(),
                table_name: info.table_name,
                inserts,
                updates,
                deletes,
                schema_change,
                truncated,
            });
        }
    }

    // Sort for deterministic output.
    diffs.sort_by(|a, b| (&a.db_name, &a.table_name).cmp(&(&b.db_name, &b.table_name)));
    Ok(diffs)
}

/// Format the collected diffs as text summary.
pub fn format_summary(diffs: &[TableDiff]) -> String {
    if diffs.is_empty() {
        return "No changes in overlay.".to_string();
    }

    let mut out = String::new();
    let mut current_db = "";

    for d in diffs {
        if d.db_name != current_db {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(&format!("{}:\n", d.db_name));
            current_db = &d.db_name;
        }

        let mut parts = Vec::new();

        if d.truncated {
            parts.push("(truncated)".to_string());
        }

        if !d.inserts.is_empty() {
            parts.push(format!("+{} inserted", d.inserts.len()));
        }
        if !d.updates.is_empty() {
            parts.push(format!("~{} updated", d.updates.len()));
        }
        if !d.deletes.is_empty() {
            parts.push(format!("-{} deleted", d.deletes.len()));
        }
        if d.schema_change.is_some() {
            parts.push("(schema changed)".to_string());
        }
        if parts.is_empty() && d.inserts.is_empty() && d.updates.is_empty() && d.deletes.is_empty()
        {
            // Table is in _cow_tables but has no row changes and no schema — created in overlay
            parts.push("(created in overlay)".to_string());
        }

        out.push_str(&format!("  {:<15}{}\n", d.table_name.clone() + ":", parts.join(", ")));
    }

    out
}

/// Format the collected diffs in verbose text mode.
pub fn format_verbose(diffs: &[TableDiff]) -> String {
    if diffs.is_empty() {
        return "No changes in overlay.".to_string();
    }

    let mut out = String::new();

    for d in diffs {
        out.push_str(&format!("{}.{}:\n", d.db_name, d.table_name));

        if d.truncated {
            out.push_str("  (truncated)\n");
        }

        if let Some(ref schema) = d.schema_change {
            out.push_str(&format!("  (schema changed: {})\n", schema));
        }

        for row in &d.inserts {
            let cols: Vec<String> = row
                .iter()
                .map(|(k, v)| format!("{k}={}", quote_val(v)))
                .collect();
            out.push_str(&format!("  + INSERT {}\n", cols.join(" ")));
        }

        for row in &d.updates {
            let cols: Vec<String> = row
                .iter()
                .map(|(k, v)| format!("{k}={}", quote_val(v)))
                .collect();
            out.push_str(&format!("  ~ UPDATE {}\n", cols.join(" ")));
        }

        for pk in &d.deletes {
            out.push_str(&format!("  - DELETE id={pk}\n"));
        }

        out.push('\n');
    }

    out
}

/// Format the collected diffs as SQL statements.
pub fn format_sql(diffs: &[TableDiff]) -> String {
    if diffs.is_empty() {
        return "-- No changes in overlay.\n".to_string();
    }

    let mut out = String::new();

    for d in diffs {
        out.push_str(&format!("-- {}.{}\n", d.db_name, d.table_name));

        if d.truncated {
            out.push_str(&format!("TRUNCATE TABLE `{}`;\n", d.table_name));
        }

        if let Some(ref schema) = d.schema_change {
            out.push_str(&format!("{}\n", schema));
        }

        for row in &d.inserts {
            let col_names: Vec<String> = row.iter().map(|(k, _)| format!("`{k}`")).collect();
            let col_vals: Vec<String> = row.iter().map(|(_, v)| sql_literal(v)).collect();
            out.push_str(&format!(
                "INSERT INTO `{}` ({}) VALUES ({});\n",
                d.table_name,
                col_names.join(", "),
                col_vals.join(", "),
            ));
        }

        for row in &d.updates {
            // The first column is assumed to be the PK for the WHERE clause.
            if row.is_empty() {
                continue;
            }
            let pk = &row[0];
            let sets: Vec<String> = row
                .iter()
                .skip(1)
                .map(|(k, v)| format!("`{k}` = {}", sql_literal(v)))
                .collect();
            if sets.is_empty() {
                continue;
            }
            out.push_str(&format!(
                "UPDATE `{}` SET {} WHERE `{}` = {};\n",
                d.table_name,
                sets.join(", "),
                pk.0,
                sql_literal(&pk.1),
            ));
        }

        for pk in &d.deletes {
            out.push_str(&format!(
                "DELETE FROM `{}` WHERE `id` = {};\n",
                d.table_name,
                sql_literal(pk),
            ));
        }

        out.push('\n');
    }

    out
}

/// Format diffs in verbose + full mode. For UPDATEs, fetch base rows from upstream.
pub async fn format_verbose_full(
    diffs: &[TableDiff],
    upstream: &str,
    user: &str,
    password: &str,
) -> Result<String> {
    use mysql_async::prelude::*;
    use mysql_async::Opts;

    if diffs.is_empty() {
        return Ok("No changes in overlay.".to_string());
    }

    let opts = Opts::from_url(&format!("mysql://{user}:{password}@{upstream}"))?;
    let pool = mysql_async::Pool::new(opts);
    let mut conn = pool.get_conn().await?;

    let mut out = String::new();

    for d in diffs {
        out.push_str(&format!("{}.{}:\n", d.db_name, d.table_name));

        if d.truncated {
            out.push_str("  (truncated)\n");
        }

        if let Some(ref schema) = d.schema_change {
            out.push_str(&format!("  (schema changed: {})\n", schema));
        }

        for row in &d.inserts {
            let cols: Vec<String> = row
                .iter()
                .map(|(k, v)| format!("{k}={}", quote_val(v)))
                .collect();
            out.push_str(&format!("  + INSERT {}\n", cols.join(" ")));
        }

        for row in &d.updates {
            if row.is_empty() {
                continue;
            }
            let pk = &row[0];
            // Fetch the base row from upstream.
            let query = format!(
                "SELECT * FROM `{}`.`{}` WHERE `{}` = {}",
                d.db_name,
                d.table_name,
                pk.0,
                sql_literal(&pk.1),
            );
            let base_row: Option<mysql_async::Row> =
                conn.query_first(&query).await.unwrap_or(None);

            out.push_str(&format!("  ~ UPDATE {}={}:\n", pk.0, quote_val(&pk.1)));

            match base_row {
                Some(base) => {
                    for (col, new_val) in row.iter().skip(1) {
                        let old_val = get_mysql_col_as_string(&base, col);
                        if old_val != *new_val {
                            out.push_str(&format!(
                                "      {}: {} -> {}\n",
                                col,
                                quote_val(&old_val),
                                quote_val(new_val),
                            ));
                        }
                    }
                }
                None => {
                    // Base row not found — show all columns
                    let cols: Vec<String> = row
                        .iter()
                        .skip(1)
                        .map(|(k, v)| format!("{k}={}", quote_val(v)))
                        .collect();
                    out.push_str(&format!("      (base row not found) {}\n", cols.join(" ")));
                }
            }
        }

        for pk in &d.deletes {
            out.push_str(&format!("  - DELETE id={pk}\n"));
        }

        out.push('\n');
    }

    pool.disconnect().await?;
    Ok(out)
}

/// Extract a column value from a mysql_async Row as a String.
fn get_mysql_col_as_string(row: &mysql_async::Row, col: &str) -> String {
    use mysql_async::Value;
    // Try to find the column index by name.
    let idx = row.columns_ref().iter().position(|c| c.name_str() == col);
    match idx {
        Some(i) => match row.as_ref(i) {
            Some(Value::NULL) | None => "NULL".to_string(),
            Some(Value::Bytes(b)) => String::from_utf8_lossy(b).into_owned(),
            Some(Value::Int(n)) => n.to_string(),
            Some(Value::UInt(n)) => n.to_string(),
            Some(Value::Float(f)) => f.to_string(),
            Some(Value::Double(f)) => f.to_string(),
            Some(other) => format!("{:?}", other),
        },
        None => "NULL".to_string(),
    }
}

/// Quote a value for display: numbers stay bare, strings get single-quoted.
fn quote_val(v: &str) -> String {
    if v == "NULL" {
        return "NULL".to_string();
    }
    // If it parses as a number, don't quote it.
    if v.parse::<i64>().is_ok() || v.parse::<f64>().is_ok() {
        v.to_string()
    } else {
        format!("'{}'", v.replace('\'', "\\'"))
    }
}

/// Produce a SQL literal from a value string.
fn sql_literal(v: &str) -> String {
    if v == "NULL" {
        return "NULL".to_string();
    }
    if v.parse::<i64>().is_ok() || v.parse::<f64>().is_ok() {
        v.to_string()
    } else {
        format!("'{}'", v.replace('\'', "''"))
    }
}

/// Top-level entry point for the diff command.
pub async fn run_diff(
    overlay_dir: &Path,
    format: DiffFormat,
    verbose: bool,
    full: bool,
    upstream: Option<&str>,
    user: Option<&str>,
    password: Option<&str>,
    db_filter: Option<&str>,
    table_filter: Option<&str>,
) -> Result<()> {
    let diffs = collect_diffs(overlay_dir, db_filter, table_filter)?;

    let output = if full && verbose {
        let upstream =
            upstream.ok_or_else(|| anyhow::anyhow!("--upstream is required with --full"))?;
        let user = user.unwrap_or("root");
        let password = password.unwrap_or("");
        format_verbose_full(&diffs, upstream, user, password).await?
    } else {
        match (format, verbose) {
            (DiffFormat::Sql, _) => format_sql(&diffs),
            (DiffFormat::Text, true) => format_verbose(&diffs),
            (DiffFormat::Text, false) => format_summary(&diffs),
        }
    };

    print!("{output}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::overlay::registry::DirtyKind;
    use tempfile::TempDir;

    /// Helper: create an overlay with test data and return the temp dir.
    fn setup_test_overlay() -> TempDir {
        let dir = TempDir::new().unwrap();
        let store = OverlayStore::open(dir.path(), "testdb").unwrap();
        let reg = Registry::new(&store.conn);
        let row_store = RowStore::new(&store.conn);

        // Mark users table as dirty (data).
        reg.mark_dirty("users", DirtyKind::Data).unwrap();

        // Create the shadow table.
        row_store
            .ensure_shadow_table(
                "users",
                &[
                    ("id", "INTEGER"),
                    ("name", "TEXT"),
                    ("email", "TEXT"),
                    ("active", "INTEGER"),
                ],
            )
            .unwrap();

        // Insert rows into the shadow table.
        store
            .conn
            .execute(
                "INSERT INTO _cow_data_users (_cow_pk, _cow_op, id, name, email, active) \
                 VALUES ('9223372036854775807', 'INSERT', 9223372036854775807, 'David', 'david@test.com', 1)",
                [],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO _cow_data_users (_cow_pk, _cow_op, id, name, email, active) \
                 VALUES ('1', 'UPDATE', 1, 'Alice', 'alice-new@test.com', 1)",
                [],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO _cow_data_users (_cow_pk, _cow_op, id, name, email, active) \
                 VALUES ('3', 'DELETE', 3, NULL, NULL, NULL)",
                [],
            )
            .unwrap();

        drop(store);
        dir
    }

    #[test]
    fn test_collect_diffs() {
        let dir = setup_test_overlay();
        let diffs = collect_diffs(dir.path(), None, None).unwrap();
        assert_eq!(diffs.len(), 1);
        let d = &diffs[0];
        assert_eq!(d.db_name, "testdb");
        assert_eq!(d.table_name, "users");
        assert_eq!(d.inserts.len(), 1);
        assert_eq!(d.updates.len(), 1);
        assert_eq!(d.deletes.len(), 1);
        assert_eq!(d.deletes[0], "3");
    }

    #[test]
    fn test_collect_diffs_with_db_filter() {
        let dir = setup_test_overlay();
        let diffs = collect_diffs(dir.path(), Some("nope"), None).unwrap();
        assert!(diffs.is_empty());
    }

    #[test]
    fn test_collect_diffs_with_table_filter() {
        let dir = setup_test_overlay();
        let diffs = collect_diffs(dir.path(), None, Some("orders")).unwrap();
        assert!(diffs.is_empty());

        let diffs = collect_diffs(dir.path(), None, Some("users")).unwrap();
        assert_eq!(diffs.len(), 1);
    }

    #[test]
    fn test_format_summary() {
        let dir = setup_test_overlay();
        let diffs = collect_diffs(dir.path(), None, None).unwrap();
        let output = format_summary(&diffs);
        assert!(output.contains("testdb:"));
        assert!(output.contains("+1 inserted"));
        assert!(output.contains("~1 updated"));
        assert!(output.contains("-1 deleted"));
    }

    #[test]
    fn test_format_verbose() {
        let dir = setup_test_overlay();
        let diffs = collect_diffs(dir.path(), None, None).unwrap();
        let output = format_verbose(&diffs);
        assert!(output.contains("testdb.users:"));
        assert!(output.contains("+ INSERT"));
        assert!(output.contains("name='David'"));
        assert!(output.contains("~ UPDATE"));
        assert!(output.contains("email='alice-new@test.com'"));
        assert!(output.contains("- DELETE id=3"));
    }

    #[test]
    fn test_format_sql() {
        let dir = setup_test_overlay();
        let diffs = collect_diffs(dir.path(), None, None).unwrap();
        let output = format_sql(&diffs);
        assert!(output.contains("INSERT INTO `users`"));
        assert!(output.contains("'David'"));
        assert!(output.contains("UPDATE `users` SET"));
        assert!(output.contains("'alice-new@test.com'"));
        assert!(output.contains("DELETE FROM `users`"));
    }

    #[test]
    fn test_empty_overlay() {
        let dir = TempDir::new().unwrap();
        let diffs = collect_diffs(dir.path(), None, None).unwrap();
        assert!(diffs.is_empty());
        assert_eq!(format_summary(&diffs), "No changes in overlay.");
    }

    #[test]
    fn test_schema_only_change() {
        let dir = TempDir::new().unwrap();
        let store = OverlayStore::open(dir.path(), "testdb").unwrap();
        let reg = Registry::new(&store.conn);
        reg.mark_dirty("products", DirtyKind::Schema).unwrap();
        store
            .conn
            .execute(
                "UPDATE _cow_tables SET overlay_schema = 'ALTER TABLE products ADD COLUMN price DECIMAL(10,2)' WHERE table_name = 'products'",
                [],
            )
            .unwrap();
        drop(store);

        let diffs = collect_diffs(dir.path(), None, None).unwrap();
        assert_eq!(diffs.len(), 1);
        assert!(diffs[0].schema_change.is_some());
        let summary = format_summary(&diffs);
        assert!(summary.contains("(schema changed)"));
    }

    #[test]
    fn test_truncated_table() {
        let dir = TempDir::new().unwrap();
        let store = OverlayStore::open(dir.path(), "testdb").unwrap();
        store
            .conn
            .execute(
                "INSERT INTO _cow_tables (table_name, has_data, truncated) VALUES ('logs', 1, 1)",
                [],
            )
            .unwrap();
        drop(store);

        let diffs = collect_diffs(dir.path(), None, None).unwrap();
        assert_eq!(diffs.len(), 1);
        assert!(diffs[0].truncated);
        let summary = format_summary(&diffs);
        assert!(summary.contains("(truncated)"));
        let sql = format_sql(&diffs);
        assert!(sql.contains("TRUNCATE TABLE `logs`"));
    }

    #[test]
    fn test_quote_val() {
        assert_eq!(quote_val("42"), "42");
        assert_eq!(quote_val("NULL"), "NULL");
        assert_eq!(quote_val("hello"), "'hello'");
        assert_eq!(quote_val("it's"), "'it\\'s'");
    }

    #[test]
    fn test_sql_literal() {
        assert_eq!(sql_literal("42"), "42");
        assert_eq!(sql_literal("NULL"), "NULL");
        assert_eq!(sql_literal("hello"), "'hello'");
        assert_eq!(sql_literal("it's"), "'it''s'");
    }
}
