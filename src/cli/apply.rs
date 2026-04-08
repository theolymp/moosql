use std::io::{self, Write as IoWrite};
use std::path::Path;

use anyhow::{Context, Result};
use mysql_async::prelude::*;

use crate::overlay::registry::Registry;
use crate::overlay::row_store::RowStore;
use crate::overlay::schema_tracker::SchemaTracker;
use crate::overlay::store::OverlayStore;
use crate::proxy::auth::verify_upstream;

/// A single SQL statement to be applied upstream, with metadata for display.
struct ApplyStatement {
    db_name: String,
    table_name: String,
    sql: String,
}

/// Collect all SQL statements to apply from the overlay directory.
fn collect_statements(
    overlay_dir: &Path,
    db_filter: Option<&str>,
    table_filter: Option<&str>,
) -> Result<Vec<ApplyStatement>> {
    let mut statements = Vec::new();

    if !overlay_dir.exists() {
        return Ok(statements);
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

        // Apply db filter
        if let Some(filter) = db_filter {
            if db_name != filter {
                continue;
            }
        }

        let store = OverlayStore::open(overlay_dir, &db_name)?;
        let reg = Registry::new(&store.conn);
        let tracker = SchemaTracker::new(&store.conn);
        let row_store = RowStore::new(&store.conn);

        let mut dirty = reg.list_dirty()?;
        // Sort for deterministic output
        dirty.sort_by(|a, b| a.table_name.cmp(&b.table_name));

        for info in dirty {
            // Apply table filter
            if let Some(filter) = table_filter {
                if info.table_name != filter {
                    continue;
                }
            }

            let table = &info.table_name;
            let is_truncated = reg.is_truncated(table)?;

            // DDL first
            if info.has_schema {
                if let Some(schema_sql) = tracker.get_overlay_schema(table)? {
                    if schema_sql == "DROPPED" {
                        statements.push(ApplyStatement {
                            db_name: db_name.clone(),
                            table_name: table.clone(),
                            sql: format!("DROP TABLE IF EXISTS `{}`;", table),
                        });
                    } else {
                        // schema_sql may be CREATE TABLE or ALTER TABLE
                        statements.push(ApplyStatement {
                            db_name: db_name.clone(),
                            table_name: table.clone(),
                            sql: format!("{};", schema_sql.trim_end_matches(';')),
                        });
                    }
                }
            }

            if info.has_data {
                // TRUNCATE before INSERTs if the truncated flag is set
                if is_truncated {
                    statements.push(ApplyStatement {
                        db_name: db_name.clone(),
                        table_name: table.clone(),
                        sql: format!("TRUNCATE TABLE `{}`;", table),
                    });
                }

                // Read all overlay rows for this table
                let rows = row_store.get_all_overlay_data(table)?;

                // Separate by operation
                let mut inserts = Vec::new();
                let mut updates = Vec::new();
                let mut deletes = Vec::new();

                for row in rows {
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

                    // Data columns (exclude _cow_pk and _cow_op)
                    let data_cols: Vec<(String, String)> = row
                        .into_iter()
                        .filter(|(k, _)| k != "_cow_pk" && k != "_cow_op")
                        .collect();

                    match op.as_str() {
                        "INSERT" => inserts.push((pk, data_cols)),
                        "UPDATE" => updates.push((pk, data_cols)),
                        "DELETE" => deletes.push((pk, data_cols)),
                        _ => {}
                    }
                }

                // Generate INSERT statements
                for (_pk, cols) in inserts {
                    if cols.is_empty() {
                        continue;
                    }
                    let col_names: Vec<String> =
                        cols.iter().map(|(k, _)| format!("`{}`", k)).collect();
                    let col_vals: Vec<String> =
                        cols.iter().map(|(_, v)| format_sql_value(v)).collect();

                    statements.push(ApplyStatement {
                        db_name: db_name.clone(),
                        table_name: table.clone(),
                        sql: format!(
                            "INSERT INTO `{}` ({}) VALUES ({});",
                            table,
                            col_names.join(", "),
                            col_vals.join(", ")
                        ),
                    });
                }

                // Generate UPDATE statements (pk is the primary key value)
                for (pk, cols) in updates {
                    if cols.is_empty() {
                        continue;
                    }
                    // The _cow_pk holds the pk column=value in format "col=val"
                    // We use it directly in the WHERE clause
                    let set_clause: Vec<String> = cols
                        .iter()
                        .map(|(k, v)| format!("`{}` = {}", k, format_sql_value(v)))
                        .collect();

                    statements.push(ApplyStatement {
                        db_name: db_name.clone(),
                        table_name: table.clone(),
                        sql: format!(
                            "UPDATE `{}` SET {} WHERE {};",
                            table,
                            set_clause.join(", "),
                            pk
                        ),
                    });
                }

                // Generate DELETE statements
                for (pk, _cols) in deletes {
                    statements.push(ApplyStatement {
                        db_name: db_name.clone(),
                        table_name: table.clone(),
                        sql: format!("DELETE FROM `{}` WHERE {};", table, pk),
                    });
                }
            }
        }
    }

    Ok(statements)
}

/// Format a stored value for use in SQL.
/// Values stored as "NULL" become SQL NULL; others are single-quoted strings
/// unless they look like numeric values.
fn format_sql_value(val: &str) -> String {
    if val == "NULL" {
        return "NULL".to_string();
    }

    // Try to detect numeric values (integer or float) to avoid quoting them.
    // This covers plain integers and decimals.
    if val.parse::<i64>().is_ok() || val.parse::<f64>().is_ok() {
        return val.to_string();
    }

    // Escape single quotes by doubling them
    let escaped = val.replace('\'', "''");
    format!("'{}'", escaped)
}

pub async fn run_apply(
    overlay_dir: &Path,
    upstream_addr: &str,
    user: &str,
    password: &str,
    db_filter: Option<&str>,
    table_filter: Option<&str>,
    dry_run: bool,
    auto_yes: bool,
    reset_after: bool,
) -> Result<()> {
    let statements = collect_statements(overlay_dir, db_filter, table_filter)
        .context("Failed to collect overlay changes")?;

    if statements.is_empty() {
        println!("No changes in overlay. Nothing to apply.");
        return Ok(());
    }

    if dry_run {
        println!("-- Dry run: {} statement(s) would be applied", statements.len());
        let mut current_db = String::new();
        for stmt in &statements {
            if stmt.db_name != current_db {
                println!("\n-- Database: {}", stmt.db_name);
                println!("USE `{}`;", stmt.db_name);
                current_db = stmt.db_name.clone();
            }
            println!("{}", stmt.sql);
        }
        return Ok(());
    }

    // Print summary
    println!("Overlay changes to apply:");
    let mut current_db = String::new();
    for stmt in &statements {
        if stmt.db_name != current_db {
            println!("  Database: {}", stmt.db_name);
            current_db = stmt.db_name.clone();
        }
        println!("    [{}] {}", stmt.table_name, truncate_sql(&stmt.sql, 80));
    }
    println!();
    println!("{} statement(s) to execute.", statements.len());

    if !auto_yes {
        print!("Apply {} change(s) to upstream? [y/N] ", statements.len());
        io::stdout().flush()?;

        let mut line = String::new();
        io::stdin().read_line(&mut line)?;
        let answer = line.trim().to_lowercase();
        if answer != "y" && answer != "yes" {
            println!("Aborted.");
            return Ok(());
        }
    }

    // Group statements by database
    let mut by_db: Vec<(String, Vec<&ApplyStatement>)> = Vec::new();
    for stmt in &statements {
        if let Some(entry) = by_db.iter_mut().find(|(db, _)| db == &stmt.db_name) {
            entry.1.push(stmt);
        } else {
            by_db.push((stmt.db_name.clone(), vec![stmt]));
        }
    }

    let mut total_executed = 0usize;

    for (db_name, db_stmts) in &by_db {
        println!("Connecting to upstream for database `{}`...", db_name);

        let mut conn = verify_upstream(upstream_addr, user, password, Some(db_name))
            .await
            .with_context(|| format!("Failed to connect to upstream for database `{}`", db_name))?;

        // Execute all statements in a transaction
        conn.query_drop("BEGIN").await.context("Failed to begin transaction")?;

        for stmt in db_stmts {
            if let Err(e) = conn.query_drop(&stmt.sql).await {
                eprintln!("Error executing SQL: {}", stmt.sql);
                eprintln!("  Error: {}", e);
                conn.query_drop("ROLLBACK")
                    .await
                    .unwrap_or_else(|re| eprintln!("Rollback failed: {}", re));
                return Err(anyhow::anyhow!(
                    "Failed to execute statement in database `{}`: {}",
                    db_name,
                    e
                ));
            }
            total_executed += 1;
        }

        conn.query_drop("COMMIT").await.context("Failed to commit transaction")?;
    }

    println!("Applied successfully. {} statement(s) executed.", total_executed);

    if reset_after {
        println!("Resetting overlay...");
        crate::cli::commands::reset_overlay(overlay_dir, None)?;
        println!("Overlay reset.");
    }

    Ok(())
}

/// Truncate a SQL string for display purposes.
fn truncate_sql(sql: &str, max_len: usize) -> String {
    if sql.len() <= max_len {
        sql.to_string()
    } else {
        format!("{}...", &sql[..max_len])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::overlay::registry::{DirtyKind, Registry};
    use crate::overlay::row_store::RowStore;
    use crate::overlay::store::OverlayStore;
    use tempfile::TempDir;

    fn make_store(dir: &TempDir, db: &str) -> OverlayStore {
        OverlayStore::open(dir.path(), db).unwrap()
    }

    /// Set up a table with INSERT/UPDATE/DELETE rows in the overlay.
    fn setup_data_table(store: &OverlayStore, table: &str) {
        let reg = Registry::new(&store.conn);
        let row_store = RowStore::new(&store.conn);

        // Create shadow table with id + name columns
        row_store
            .ensure_shadow_table(table, &[("id", "INTEGER"), ("name", "TEXT")])
            .unwrap();

        // Mark dirty (data)
        reg.mark_dirty(table, DirtyKind::Data).unwrap();

        // Insert an INSERT row
        store
            .conn
            .execute(
                &format!(
                    "INSERT INTO \"_cow_data_{table}\" (_cow_pk, _cow_op, id, name) VALUES (?, ?, ?, ?)"
                ),
                rusqlite::params!["id=1", "INSERT", 1i64, "Alice"],
            )
            .unwrap();

        // Insert an UPDATE row
        store
            .conn
            .execute(
                &format!(
                    "INSERT INTO \"_cow_data_{table}\" (_cow_pk, _cow_op, id, name) VALUES (?, ?, ?, ?)"
                ),
                rusqlite::params!["id=2", "UPDATE", 2i64, "Bob Updated"],
            )
            .unwrap();

        // Insert a DELETE row
        store
            .conn
            .execute(
                &format!(
                    "INSERT INTO \"_cow_data_{table}\" (_cow_pk, _cow_op, id, name) VALUES (?, ?, ?, ?)"
                ),
                rusqlite::params!["id=3", "DELETE", 3i64, "Charlie"],
            )
            .unwrap();
    }

    #[test]
    fn test_generate_apply_sql() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir, "mydb");
        setup_data_table(&store, "users");
        drop(store);

        let stmts = collect_statements(dir.path(), None, None).unwrap();

        // Should have INSERT, UPDATE, DELETE statements
        let sqls: Vec<&str> = stmts.iter().map(|s| s.sql.as_str()).collect();

        let insert = sqls.iter().find(|s| s.contains("INSERT INTO"));
        let update = sqls.iter().find(|s| s.contains("UPDATE"));
        let delete = sqls.iter().find(|s| s.contains("DELETE FROM"));

        assert!(insert.is_some(), "Should have an INSERT statement, got: {:?}", sqls);
        assert!(update.is_some(), "Should have an UPDATE statement, got: {:?}", sqls);
        assert!(delete.is_some(), "Should have a DELETE statement, got: {:?}", sqls);

        // Verify content
        let insert_sql = insert.unwrap();
        assert!(insert_sql.contains("`users`"), "INSERT should reference users table");
        assert!(insert_sql.contains("Alice"), "INSERT should contain Alice value");

        let update_sql = update.unwrap();
        assert!(update_sql.contains("WHERE id=2"), "UPDATE should use pk in WHERE clause");
        assert!(update_sql.contains("Bob Updated"), "UPDATE should contain new value");

        let delete_sql = delete.unwrap();
        assert!(delete_sql.contains("WHERE id=3"), "DELETE should use pk in WHERE clause");
    }

    #[test]
    fn test_generate_apply_sql_with_truncate() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir, "mydb");

        let reg = Registry::new(&store.conn);
        let row_store = RowStore::new(&store.conn);

        let table = "products";
        row_store
            .ensure_shadow_table(table, &[("id", "INTEGER"), ("name", "TEXT")])
            .unwrap();

        // Set truncated flag
        store.conn.execute(
            "INSERT INTO _cow_tables (table_name, has_data, truncated) VALUES ('products', 1, 1)
             ON CONFLICT(table_name) DO UPDATE SET has_data=1, truncated=1",
            [],
        ).unwrap();

        // Add an INSERT after truncate
        store.conn.execute(
            "INSERT INTO \"_cow_data_products\" (_cow_pk, _cow_op, id, name) VALUES (?, ?, ?, ?)",
            rusqlite::params!["id=10", "INSERT", 10i64, "Widget"],
        ).unwrap();

        drop(store);

        let stmts = collect_statements(dir.path(), None, None).unwrap();
        let sqls: Vec<&str> = stmts.iter().map(|s| s.sql.as_str()).collect();

        // First statement for products should be TRUNCATE
        let truncate_pos = sqls.iter().position(|s| s.contains("TRUNCATE"));
        let insert_pos = sqls.iter().position(|s| s.contains("INSERT INTO"));

        assert!(truncate_pos.is_some(), "Should have a TRUNCATE statement");
        assert!(insert_pos.is_some(), "Should have an INSERT statement");
        assert!(
            truncate_pos.unwrap() < insert_pos.unwrap(),
            "TRUNCATE should come before INSERT"
        );
    }

    #[test]
    fn test_dry_run_output() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir, "testdb");
        setup_data_table(&store, "orders");
        drop(store);

        // dry_run collects statements but doesn't connect — just verify it works
        let stmts = collect_statements(dir.path(), None, None).unwrap();
        assert!(!stmts.is_empty(), "Should have statements to apply");

        // Verify all statements have db_name set
        for stmt in &stmts {
            assert_eq!(stmt.db_name, "testdb");
            assert_eq!(stmt.table_name, "orders");
            assert!(!stmt.sql.is_empty());
        }
    }

    #[test]
    fn test_db_filter() {
        let dir = TempDir::new().unwrap();

        // Two databases
        {
            let store = make_store(&dir, "db1");
            setup_data_table(&store, "table1");
        }
        {
            let store = make_store(&dir, "db2");
            setup_data_table(&store, "table2");
        }

        // Filter to db1 only
        let stmts = collect_statements(dir.path(), Some("db1"), None).unwrap();
        assert!(
            stmts.iter().all(|s| s.db_name == "db1"),
            "All statements should be from db1"
        );

        // Filter to db2 only
        let stmts = collect_statements(dir.path(), Some("db2"), None).unwrap();
        assert!(
            stmts.iter().all(|s| s.db_name == "db2"),
            "All statements should be from db2"
        );
    }

    #[test]
    fn test_table_filter() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir, "mydb");
        setup_data_table(&store, "users");
        setup_data_table(&store, "orders");
        drop(store);

        let stmts = collect_statements(dir.path(), None, Some("users")).unwrap();
        assert!(
            stmts.iter().all(|s| s.table_name == "users"),
            "All statements should be for users table"
        );
        assert!(!stmts.is_empty(), "Should have statements for users");
    }

    #[test]
    fn test_format_sql_value() {
        assert_eq!(format_sql_value("NULL"), "NULL");
        assert_eq!(format_sql_value("42"), "42");
        assert_eq!(format_sql_value("3.14"), "3.14");
        assert_eq!(format_sql_value("hello"), "'hello'");
        assert_eq!(format_sql_value("it's"), "'it''s'");
        assert_eq!(format_sql_value(""), "''");
    }

    #[test]
    fn test_schema_ddl_in_statements() {
        let dir = TempDir::new().unwrap();
        let store = make_store(&dir, "mydb");

        let reg = Registry::new(&store.conn);
        let tracker = crate::overlay::schema_tracker::SchemaTracker::new(&store.conn);

        // Simulate a CREATE TABLE DDL change
        tracker
            .update_overlay_schema("newtable", "CREATE TABLE newtable (id INT PRIMARY KEY, val TEXT)")
            .unwrap();
        reg.mark_dirty("newtable", DirtyKind::Schema).unwrap();

        drop(store);

        let stmts = collect_statements(dir.path(), None, None).unwrap();
        let ddl = stmts.iter().find(|s| s.sql.contains("CREATE TABLE"));
        assert!(ddl.is_some(), "Should have a CREATE TABLE statement");
    }
}
