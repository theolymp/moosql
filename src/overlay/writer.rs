use std::collections::HashMap;

use anyhow::{anyhow, bail, Context};
use sqlparser::ast::{AssignmentTarget, Expr, FromTable, ObjectName, SetExpr, Statement, TableObject, Value};

use crate::sql::parser::parse_single_statement;

use crate::overlay::registry::{DirtyKind, Registry};
use crate::overlay::row_store::RowStore;
use crate::overlay::schema_tracker::SchemaTracker;
use crate::overlay::sequence::SequenceTracker;
use crate::overlay::store::OverlayStore;

/// Result of executing a write statement against the overlay.
pub struct WriteResult {
    pub affected_rows: u64,
    pub last_insert_id: Option<i64>,
}

/// Extract the table name from a sqlparser INSERT statement.
fn extract_table_name(stmt: &Statement) -> anyhow::Result<String> {
    match stmt {
        Statement::Insert(insert) => match &insert.table {
            TableObject::TableName(name) => {
                // Take the last part of the object name as the table name
                let last = name
                    .0
                    .last()
                    .ok_or_else(|| anyhow!("INSERT has empty table name"))?;
                match last.as_ident() {
                    Some(ident) => Ok(ident.value.clone()),
                    None => bail!("INSERT table name is not a simple identifier"),
                }
            }
            TableObject::TableFunction(_) => {
                bail!("INSERT into table function not supported")
            }
        },
        _ => bail!("Not an INSERT statement"),
    }
}

/// Extract column names from a sqlparser INSERT statement.
fn extract_columns(stmt: &Statement) -> anyhow::Result<Vec<String>> {
    match stmt {
        Statement::Insert(insert) => Ok(insert
            .columns
            .iter()
            .map(|ident| ident.value.clone())
            .collect()),
        _ => bail!("Not an INSERT statement"),
    }
}

/// Extract value rows from a sqlparser INSERT statement.
/// Each row is a Vec of string representations of the values.
fn extract_value_rows(stmt: &Statement) -> anyhow::Result<Vec<Vec<String>>> {
    match stmt {
        Statement::Insert(insert) => {
            let source = insert
                .source
                .as_ref()
                .ok_or_else(|| anyhow!("INSERT has no source (VALUES clause)"))?;

            match source.body.as_ref() {
                SetExpr::Values(values) => {
                    let mut rows = Vec::new();
                    for row_exprs in &values.rows {
                        let mut row = Vec::new();
                        for expr in row_exprs {
                            row.push(expr_to_string(expr)?);
                        }
                        rows.push(row);
                    }
                    Ok(rows)
                }
                _ => bail!("INSERT source is not a VALUES clause"),
            }
        }
        _ => bail!("Not an INSERT statement"),
    }
}

/// Convert an AST expression to a string value for SQLite storage.
fn expr_to_string(expr: &Expr) -> anyhow::Result<String> {
    match expr {
        Expr::Value(vws) => match &vws.value {
            Value::Number(n, _) => Ok(n.to_string()),
            Value::SingleQuotedString(s) => Ok(s.clone()),
            Value::DoubleQuotedString(s) => Ok(s.clone()),
            Value::Boolean(b) => Ok(if *b { "1".to_string() } else { "0".to_string() }),
            Value::Null => Ok("NULL".to_string()),
            Value::HexStringLiteral(s) => Ok(format!("X'{s}'")),
            Value::Placeholder(s) => bail!("Placeholder {s} not supported in overlay INSERT"),
            other => Ok(format!("{other}")),
        },
        Expr::UnaryOp { op, expr } => {
            // Handle negative numbers like -1
            let inner = expr_to_string(expr)?;
            Ok(format!("{op}{inner}"))
        }
        other => {
            // Fallback: render as SQL text
            Ok(format!("{other}"))
        }
    }
}

/// Determine if a column is an auto-increment primary key.
/// Heuristic: column named "id" with an integer type.
fn is_auto_increment_pk(col_name: &str, col_type: &str) -> bool {
    let name_lower = col_name.to_lowercase();
    let type_lower = col_type.to_lowercase();
    (name_lower == "id")
        && (type_lower.contains("int") || type_lower == "integer")
}

/// Find the primary key column name from the schema.
/// Returns the first column that looks like an auto-increment PK.
fn find_pk_column(schema: &[(String, String)]) -> Option<&str> {
    schema
        .iter()
        .find(|(name, ty)| is_auto_increment_pk(name, ty))
        .map(|(name, _)| name.as_str())
}

/// Execute an INSERT statement against the SQLite overlay.
///
/// `schema` is the full list of (column_name, column_type) pairs for the table,
/// as fetched from the upstream MariaDB via SHOW COLUMNS.
/// `defaults` is a map of column_name -> default_value for columns with non-NULL defaults.
pub fn execute_insert(
    store: &OverlayStore,
    stmt: &Statement,
    schema: &[(String, String)],
    defaults: &HashMap<String, String>,
) -> anyhow::Result<WriteResult> {
    let table_name = extract_table_name(stmt)?;
    let insert_columns = extract_columns(stmt)?;
    let value_rows = extract_value_rows(stmt)?;

    // Ensure shadow table exists with all schema columns
    let row_store = RowStore::new(&store.conn);
    let col_refs: Vec<(&str, &str)> = schema
        .iter()
        .map(|(n, t)| (n.as_str(), t.as_str()))
        .collect();
    row_store
        .ensure_shadow_table(&table_name, &col_refs)
        .context("Failed to ensure shadow table")?;

    let seq = SequenceTracker::new(&store.conn);
    let pk_col = find_pk_column(schema);

    let mut last_insert_id: Option<i64> = None;
    let mut affected_rows: u64 = 0;

    for row_values in &value_rows {
        if row_values.len() != insert_columns.len() {
            bail!(
                "Column count ({}) doesn't match value count ({})",
                insert_columns.len(),
                row_values.len()
            );
        }

        // Build column->value map from the INSERT
        let mut col_val_map: Vec<(&str, String)> = insert_columns
            .iter()
            .zip(row_values.iter())
            .map(|(c, v)| (c.as_str(), v.clone()))
            .collect();

        // Fill in missing columns from defaults
        for (col_name, col_type) in schema {
            if !col_val_map.iter().any(|(c, _)| c.eq_ignore_ascii_case(col_name)) {
                if let Some(default_val) = defaults.get(col_name) {
                    col_val_map.push((col_name.as_str(), default_val.clone()));
                }
            }
            // Suppress unused variable warning for col_type
            let _ = col_type;
        }

        // If PK column exists and wasn't supplied, generate an overlay ID
        let generated_id = if let Some(pk) = pk_col {
            if !insert_columns.iter().any(|c| c.eq_ignore_ascii_case(pk)) {
                let id = seq
                    .next_id(&table_name)
                    .context("Failed to get next overlay ID")?;
                col_val_map.push((pk, id.to_string()));
                Some(id)
            } else {
                None
            }
        } else {
            None
        };

        // Serialize PK value for _cow_pk
        let cow_pk = if let Some(pk) = pk_col {
            col_val_map
                .iter()
                .find(|(c, _)| c.eq_ignore_ascii_case(pk))
                .map(|(_, v)| v.clone())
                .unwrap_or_else(|| "unknown".to_string())
        } else {
            // No PK column identified — use a generated sequence as fallback
            let id = seq
                .next_id(&table_name)
                .context("Failed to get next overlay ID for PK-less table")?;
            id.to_string()
        };

        // Build the INSERT into the shadow table
        let shadow = format!("_cow_data_{table_name}");
        let mut sql_cols = vec!["_cow_pk".to_string(), "_cow_op".to_string()];
        let mut sql_vals: Vec<String> = vec![cow_pk, "INSERT".to_string()];

        for (col, val) in &col_val_map {
            sql_cols.push(format!("\"{}\"", col));
            sql_vals.push(val.clone());
        }

        let placeholders: Vec<String> = (1..=sql_cols.len()).map(|i| format!("?{i}")).collect();
        let insert_sql = format!(
            "INSERT INTO \"{}\" ({}) VALUES ({})",
            shadow,
            sql_cols.join(", "),
            placeholders.join(", "),
        );

        let params: Vec<&dyn rusqlite::types::ToSql> = sql_vals
            .iter()
            .map(|v| v as &dyn rusqlite::types::ToSql)
            .collect();

        store
            .conn
            .execute(&insert_sql, params.as_slice())
            .with_context(|| format!("Failed to insert into shadow table {shadow}"))?;

        if let Some(id) = generated_id {
            last_insert_id = Some(id);
        }
        affected_rows += 1;
    }

    // Mark table as dirty
    let reg = Registry::new(&store.conn);
    reg.mark_dirty(&table_name, DirtyKind::Data)
        .context("Failed to mark table as dirty")?;

    Ok(WriteResult {
        affected_rows,
        last_insert_id,
    })
}

/// Convenience function: parse SQL and execute an INSERT against the overlay.
/// `schema` provides the upstream table's column definitions.
/// `defaults` provides column default values (column_name -> default_value).
pub fn execute_insert_sql(
    store: &OverlayStore,
    sql: &str,
    schema: &[(String, String)],
) -> anyhow::Result<WriteResult> {
    let defaults = HashMap::new();
    execute_insert_sql_with_defaults(store, sql, schema, &defaults)
}

/// Convenience function: parse SQL and execute an INSERT against the overlay,
/// applying the provided column defaults for any columns not explicitly set.
pub fn execute_insert_sql_with_defaults(
    store: &OverlayStore,
    sql: &str,
    schema: &[(String, String)],
    defaults: &HashMap<String, String>,
) -> anyhow::Result<WriteResult> {
    let stmt = parse_single_statement(sql).map_err(|e| anyhow!("Failed to parse SQL: {e}"))?;
    execute_insert(store, &stmt, schema, defaults)
}

/// Extract the table name from a parsed UPDATE statement.
fn extract_update_table_name(stmt: &Statement) -> anyhow::Result<String> {
    match stmt {
        Statement::Update(update) => {
            // The table is in update.table (a TableWithJoins).
            // Extract the first table relation's name.
            let relation = &update.table.relation;
            match relation {
                sqlparser::ast::TableFactor::Table { name, .. } => {
                    let last = name
                        .0
                        .last()
                        .ok_or_else(|| anyhow!("UPDATE has empty table name"))?;
                    match last.as_ident() {
                        Some(ident) => Ok(ident.value.clone()),
                        None => bail!("UPDATE table name is not a simple identifier"),
                    }
                }
                _ => bail!("UPDATE table is not a simple table reference"),
            }
        }
        _ => bail!("Not an UPDATE statement"),
    }
}

/// Extract the table name from a parsed DELETE statement.
fn extract_delete_table_name(stmt: &Statement) -> anyhow::Result<String> {
    match stmt {
        Statement::Delete(delete) => {
            let tables = match &delete.from {
                FromTable::WithFromKeyword(tables) => tables,
                FromTable::WithoutKeyword(tables) => tables,
            };
            let first = tables
                .first()
                .ok_or_else(|| anyhow!("DELETE has no FROM clause"))?;
            match &first.relation {
                sqlparser::ast::TableFactor::Table { name, .. } => {
                    let last = name
                        .0
                        .last()
                        .ok_or_else(|| anyhow!("DELETE has empty table name"))?;
                    match last.as_ident() {
                        Some(ident) => Ok(ident.value.clone()),
                        None => bail!("DELETE table name is not a simple identifier"),
                    }
                }
                _ => bail!("DELETE table is not a simple table reference"),
            }
        }
        _ => bail!("Not a DELETE statement"),
    }
}

/// Execute an UPDATE statement against the SQLite overlay.
///
/// `upstream_rows` contains the rows matched by the WHERE clause, fetched from
/// upstream *before* the update is applied. Each row is a list of (column_name, value) pairs.
/// The SET assignments from the SQL are applied on top of these rows to produce new values.
pub fn execute_update(
    store: &OverlayStore,
    stmt: &Statement,
    schema: &[(String, String)],
    upstream_rows: &[Vec<(String, String)>],
) -> anyhow::Result<WriteResult> {
    let update = match stmt {
        Statement::Update(u) => u,
        _ => bail!("Not an UPDATE statement"),
    };

    let table_name = extract_update_table_name(stmt)?;

    // Ensure shadow table exists
    let row_store = RowStore::new(&store.conn);
    let col_refs: Vec<(&str, &str)> = schema
        .iter()
        .map(|(n, t)| (n.as_str(), t.as_str()))
        .collect();
    row_store
        .ensure_shadow_table(&table_name, &col_refs)
        .context("Failed to ensure shadow table")?;

    let pk_col = find_pk_column(schema);
    let mut affected_rows: u64 = 0;

    // Parse SET assignments into (column_name, value_expr_string) pairs
    let assignments: Vec<(String, String)> = update
        .assignments
        .iter()
        .map(|a| {
            let col_name = match &a.target {
                AssignmentTarget::ColumnName(name) => {
                    name.0
                        .last()
                        .and_then(|p| p.as_ident())
                        .map(|i| i.value.clone())
                        .unwrap_or_else(|| format!("{}", a.target))
                }
                AssignmentTarget::Tuple(_) => format!("{}", a.target),
            };
            let value = expr_to_string(&a.value).unwrap_or_else(|_| format!("{}", a.value));
            (col_name, value)
        })
        .collect();

    let shadow = format!("_cow_data_{table_name}");

    // --- Batch shadow lookup ---
    // Compute the PK string for every upstream row first, then fetch all existing
    // overlay ops in a single IN query instead of one SELECT per row.
    let row_pks: Vec<String> = upstream_rows
        .iter()
        .map(|upstream_row| {
            if let Some(pk) = pk_col {
                upstream_row
                    .iter()
                    .find(|(c, _)| c.eq_ignore_ascii_case(pk))
                    .map(|(_, v)| v.clone())
                    .unwrap_or_else(|| "unknown".to_string())
            } else {
                "unknown".to_string()
            }
        })
        .collect();

    // Fetch existing ops for all PKs in one query.
    let existing_ops: HashMap<String, String> = if !row_pks.is_empty() {
        let placeholders: Vec<String> = (1..=row_pks.len()).map(|i| format!("?{i}")).collect();
        let batch_sql = format!(
            "SELECT _cow_pk, _cow_op FROM \"{}\" WHERE _cow_pk IN ({})",
            shadow,
            placeholders.join(", ")
        );
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            row_pks.iter().map(|v| v as &dyn rusqlite::types::ToSql).collect();
        let mut stmt_prepared = store.conn.prepare(&batch_sql)
            .with_context(|| format!("Failed to prepare batch shadow lookup for {shadow}"))?;
        let rows_iter = stmt_prepared
            .query_map(param_refs.as_slice(), |row| {
                let pk: String = row.get(0)?;
                let op: String = row.get(1)?;
                Ok((pk, op))
            })
            .with_context(|| format!("Failed to execute batch shadow lookup for {shadow}"))?;
        rows_iter.filter_map(|r| r.ok()).collect()
    } else {
        HashMap::new()
    };

    for (upstream_row, cow_pk) in upstream_rows.iter().zip(row_pks.iter()) {
        // Build a mutable map from the upstream row
        let mut col_val_map: Vec<(String, String)> = upstream_row.clone();

        // Apply SET assignments: overwrite matching columns
        for (set_col, set_val) in &assignments {
            let found = col_val_map
                .iter_mut()
                .find(|(c, _)| c.eq_ignore_ascii_case(set_col));
            if let Some((_, v)) = found {
                *v = set_val.clone();
            } else {
                // Column not in upstream row — add it
                col_val_map.push((set_col.clone(), set_val.clone()));
            }
        }

        let existing_op = existing_ops.get(cow_pk);

        if existing_op.is_some() {
            // Row already in overlay — update it in place
            // Keep the original _cow_op if it was INSERT (overlay-only row),
            // otherwise set to UPDATE
            let new_op = match existing_op.map(|s| s.as_str()) {
                Some("INSERT") => "INSERT",
                _ => "UPDATE",
            };

            let mut set_clauses: Vec<String> = vec![format!("_cow_op = ?1")];
            let mut params: Vec<String> = vec![new_op.to_string()];

            for (col, val) in &col_val_map {
                let idx = params.len() + 1;
                set_clauses.push(format!("\"{}\" = ?{}", col, idx));
                params.push(val.clone());
            }

            let update_sql = format!(
                "UPDATE \"{}\" SET {} WHERE _cow_pk = ?{}",
                shadow,
                set_clauses.join(", "),
                params.len() + 1,
            );
            params.push(cow_pk.clone());

            let param_refs: Vec<&dyn rusqlite::types::ToSql> = params
                .iter()
                .map(|v| v as &dyn rusqlite::types::ToSql)
                .collect();

            store
                .conn
                .execute(&update_sql, param_refs.as_slice())
                .with_context(|| format!("Failed to update shadow table {shadow}"))?;
        } else {
            // New overlay row — insert with UPDATE op
            let mut sql_cols = vec!["_cow_pk".to_string(), "_cow_op".to_string()];
            let mut sql_vals: Vec<String> = vec![cow_pk.clone(), "UPDATE".to_string()];

            for (col, val) in &col_val_map {
                sql_cols.push(format!("\"{}\"", col));
                sql_vals.push(val.clone());
            }

            let placeholders: Vec<String> =
                (1..=sql_cols.len()).map(|i| format!("?{i}")).collect();
            let insert_sql = format!(
                "INSERT INTO \"{}\" ({}) VALUES ({})",
                shadow,
                sql_cols.join(", "),
                placeholders.join(", "),
            );

            let param_refs: Vec<&dyn rusqlite::types::ToSql> = sql_vals
                .iter()
                .map(|v| v as &dyn rusqlite::types::ToSql)
                .collect();

            store
                .conn
                .execute(&insert_sql, param_refs.as_slice())
                .with_context(|| format!("Failed to insert into shadow table {shadow}"))?;
        }

        affected_rows += 1;
    }

    // Mark table as dirty
    let reg = Registry::new(&store.conn);
    reg.mark_dirty(&table_name, DirtyKind::Data)
        .context("Failed to mark table as dirty")?;

    Ok(WriteResult {
        affected_rows,
        last_insert_id: None,
    })
}

/// Convenience function: parse SQL and execute an UPDATE against the overlay.
pub fn execute_update_sql(
    store: &OverlayStore,
    sql: &str,
    schema: &[(String, String)],
    upstream_rows: &[Vec<(String, String)>],
) -> anyhow::Result<WriteResult> {
    let stmt = parse_single_statement(sql).map_err(|e| anyhow!("Failed to parse SQL: {e}"))?;
    execute_update(store, &stmt, schema, upstream_rows)
}

/// Execute a DELETE statement against the SQLite overlay.
///
/// `upstream_pks` contains the primary key values of rows matched by the WHERE clause.
/// For each PK:
///   - If the row is overlay-only (`_cow_op = 'INSERT'`), it is simply removed.
///   - Otherwise, a tombstone row (`_cow_op = 'DELETE'`) is written.
pub fn execute_delete(
    store: &OverlayStore,
    stmt: &Statement,
    schema: &[(String, String)],
    upstream_pks: &[String],
) -> anyhow::Result<WriteResult> {
    let table_name = extract_delete_table_name(stmt)?;

    // Ensure shadow table exists
    let row_store = RowStore::new(&store.conn);
    let col_refs: Vec<(&str, &str)> = schema
        .iter()
        .map(|(n, t)| (n.as_str(), t.as_str()))
        .collect();
    row_store
        .ensure_shadow_table(&table_name, &col_refs)
        .context("Failed to ensure shadow table")?;

    let shadow = format!("_cow_data_{table_name}");
    let mut affected_rows: u64 = 0;

    // --- Batch shadow lookup ---
    // Fetch existing ops for all PKs in one IN query instead of one SELECT per PK.
    let existing_ops: HashMap<String, String> = if !upstream_pks.is_empty() {
        let placeholders: Vec<String> =
            (1..=upstream_pks.len()).map(|i| format!("?{i}")).collect();
        let batch_sql = format!(
            "SELECT _cow_pk, _cow_op FROM \"{}\" WHERE _cow_pk IN ({})",
            shadow,
            placeholders.join(", ")
        );
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            upstream_pks.iter().map(|v| v as &dyn rusqlite::types::ToSql).collect();
        let mut stmt_prepared = store.conn.prepare(&batch_sql)
            .with_context(|| format!("Failed to prepare batch shadow lookup for {shadow}"))?;
        let rows_iter = stmt_prepared
            .query_map(param_refs.as_slice(), |row| {
                let pk: String = row.get(0)?;
                let op: String = row.get(1)?;
                Ok((pk, op))
            })
            .with_context(|| format!("Failed to execute batch shadow lookup for {shadow}"))?;
        rows_iter.filter_map(|r| r.ok()).collect()
    } else {
        HashMap::new()
    };

    for pk in upstream_pks {
        let existing_op = existing_ops.get(pk).map(|s| s.as_str());

        match existing_op {
            Some("INSERT") => {
                // Overlay-only row — just delete it, no tombstone needed
                store
                    .conn
                    .execute(
                        &format!("DELETE FROM \"{}\" WHERE _cow_pk = ?1", shadow),
                        rusqlite::params![pk],
                    )
                    .with_context(|| {
                        format!("Failed to delete overlay-only row from {shadow}")
                    })?;
            }
            Some(_) => {
                // Row exists in overlay (UPDATE or previous state) — convert to tombstone
                store
                    .conn
                    .execute(
                        &format!(
                            "UPDATE \"{}\" SET _cow_op = 'DELETE' WHERE _cow_pk = ?1",
                            shadow
                        ),
                        rusqlite::params![pk],
                    )
                    .with_context(|| format!("Failed to tombstone row in {shadow}"))?;
            }
            None => {
                // Row only exists in upstream — insert a tombstone
                store
                    .conn
                    .execute(
                        &format!(
                            "INSERT INTO \"{}\" (_cow_pk, _cow_op) VALUES (?1, 'DELETE')",
                            shadow
                        ),
                        rusqlite::params![pk],
                    )
                    .with_context(|| format!("Failed to insert tombstone into {shadow}"))?;
            }
        }

        affected_rows += 1;
    }

    // Mark table as dirty
    let reg = Registry::new(&store.conn);
    reg.mark_dirty(&table_name, DirtyKind::Data)
        .context("Failed to mark table as dirty")?;

    Ok(WriteResult {
        affected_rows,
        last_insert_id: None,
    })
}

/// Convenience function: parse SQL and execute a DELETE against the overlay.
pub fn execute_delete_sql(
    store: &OverlayStore,
    sql: &str,
    schema: &[(String, String)],
    upstream_pks: &[String],
) -> anyhow::Result<WriteResult> {
    let stmt = parse_single_statement(sql).map_err(|e| anyhow!("Failed to parse SQL: {e}"))?;
    execute_delete(store, &stmt, schema, upstream_pks)
}

/// Extract the last simple identifier from an ObjectName.
pub fn object_name_to_table(name: &ObjectName) -> anyhow::Result<String> {
    let last = name
        .0
        .last()
        .ok_or_else(|| anyhow!("Empty object name in DDL statement"))?;
    match last.as_ident() {
        Some(ident) => Ok(ident.value.clone()),
        None => bail!("DDL table name is not a simple identifier"),
    }
}

/// Execute a DDL statement (CREATE TABLE / ALTER TABLE / DROP TABLE) against the overlay.
///
/// - CREATE TABLE: stores full SQL as both `base_schema` and `overlay_schema`, marks dirty Schema.
/// - ALTER TABLE: updates `overlay_schema` to the ALTER SQL text (v1 record-only), marks dirty Schema.
/// - DROP TABLE: if the table is already tracked, stores "DROPPED" as `overlay_schema`; otherwise
///   inserts a new row with "DROPPED" so the overlay knows it was dropped.
pub fn execute_ddl(
    store: &OverlayStore,
    sql: &str,
    stmt: &Statement,
) -> anyhow::Result<WriteResult> {
    match stmt {
        Statement::CreateTable(create) => {
            let table_name = object_name_to_table(&create.name)?;
            let tracker = SchemaTracker::new(&store.conn);
            tracker
                .store_base_schema(&table_name, sql)
                .context("Failed to store base_schema for CREATE TABLE")?;
            tracker
                .update_overlay_schema(&table_name, sql)
                .context("Failed to store overlay_schema for CREATE TABLE")?;

            let reg = Registry::new(&store.conn);
            reg.mark_dirty(&table_name, DirtyKind::Schema)
                .context("Failed to mark table dirty after CREATE TABLE")?;

            Ok(WriteResult {
                affected_rows: 0,
                last_insert_id: None,
            })
        }

        Statement::AlterTable(alter) => {
            let table_name = object_name_to_table(&alter.name)?;

            // Check if table is tracked; if not, we still record the ALTER.
            let tracker = SchemaTracker::new(&store.conn);
            tracker
                .update_overlay_schema(&table_name, sql)
                .context("Failed to store overlay_schema for ALTER TABLE")?;

            let reg = Registry::new(&store.conn);
            reg.mark_dirty(&table_name, DirtyKind::Schema)
                .context("Failed to mark table dirty after ALTER TABLE")?;

            Ok(WriteResult {
                affected_rows: 0,
                last_insert_id: None,
            })
        }

        Statement::Drop {
            names,
            object_type,
            ..
        } => {
            use sqlparser::ast::ObjectType;
            if !matches!(object_type, ObjectType::Table) {
                bail!("Only DROP TABLE is handled by execute_ddl");
            }

            for name in names {
                let table_name = object_name_to_table(name)?;

                // Drop the shadow data table if it exists
                let shadow = format!("_cow_data_{table_name}");
                store
                    .conn
                    .execute_batch(&format!("DROP TABLE IF EXISTS \"{shadow}\";"))
                    .with_context(|| {
                        format!("Failed to drop shadow table {shadow} on DROP TABLE")
                    })?;

                // Record a tombstone in the schema tracker
                let tracker = SchemaTracker::new(&store.conn);
                tracker
                    .update_overlay_schema(&table_name, "DROPPED")
                    .context("Failed to store DROPPED overlay_schema for DROP TABLE")?;

                let reg = Registry::new(&store.conn);
                reg.mark_dirty(&table_name, DirtyKind::Schema)
                    .context("Failed to mark table dirty after DROP TABLE")?;
            }

            Ok(WriteResult {
                affected_rows: 0,
                last_insert_id: None,
            })
        }

        Statement::Truncate(truncate) => {
            for truncate_table in &truncate.table_names {
                let table_name = object_name_to_table(&truncate_table.name)?;

                // Clear any existing overlay data for this table (shadow table rows).
                let shadow = format!("_cow_data_{table_name}");
                store
                    .conn
                    .execute_batch(&format!("DELETE FROM \"{shadow}\";"))
                    .ok(); // ignore error if shadow table doesn't exist yet

                // Mark the table as truncated in the overlay so reads exclude base rows.
                store
                    .conn
                    .execute(
                        "INSERT INTO _cow_tables (table_name, has_schema, has_data, truncated)
                         VALUES (?1, 0, 1, 1)
                         ON CONFLICT(table_name) DO UPDATE SET
                           has_data  = 1,
                           truncated = 1",
                        rusqlite::params![&table_name],
                    )
                    .with_context(|| {
                        format!("Failed to mark table {table_name} as truncated")
                    })?;
            }

            Ok(WriteResult {
                affected_rows: 0,
                last_insert_id: None,
            })
        }

        other => bail!("execute_ddl: unsupported statement type: {}", other),
    }
}

/// Convenience function: parse SQL and execute a DDL statement against the overlay.
pub fn execute_ddl_sql(store: &OverlayStore, sql: &str) -> anyhow::Result<WriteResult> {
    let stmt = parse_single_statement(sql).map_err(|e| anyhow!("Failed to parse DDL SQL: {e}"))?;
    execute_ddl(store, sql, &stmt)
}

/// Fetch table schema from upstream MariaDB.
/// Returns a list of (column_name, column_type) pairs.
pub async fn fetch_table_schema(
    upstream: &mut mysql_async::Conn,
    table: &str,
) -> anyhow::Result<Vec<(String, String)>> {
    use mysql_async::prelude::*;

    let sql = format!("SHOW COLUMNS FROM `{}`", table);
    let rows: Vec<mysql_async::Row> = upstream
        .query(&sql)
        .await
        .with_context(|| format!("Failed to fetch schema for table `{table}`"))?;

    let mut columns = Vec::new();
    for row in rows {
        let field: String = row
            .get(0)
            .ok_or_else(|| anyhow!("Missing Field column in SHOW COLUMNS"))?;
        let col_type: String = row
            .get(1)
            .ok_or_else(|| anyhow!("Missing Type column in SHOW COLUMNS"))?;
        columns.push((field, col_type));
    }

    Ok(columns)
}

/// Fetch column defaults for a table from upstream MariaDB.
/// Returns a map of column_name -> default_value for columns that have non-NULL defaults.
/// Uses `SHOW COLUMNS` which returns: Field, Type, Null, Key, Default, Extra.
pub async fn fetch_table_defaults(
    upstream: &mut mysql_async::Conn,
    table: &str,
) -> anyhow::Result<HashMap<String, String>> {
    use mysql_async::prelude::*;

    let sql = format!("SHOW COLUMNS FROM `{}`", table);
    let rows: Vec<mysql_async::Row> = upstream
        .query(&sql)
        .await
        .with_context(|| format!("Failed to fetch schema for table `{table}`"))?;

    let mut defaults = HashMap::new();
    for row in rows {
        let field: String = row
            .get(0)
            .ok_or_else(|| anyhow!("Missing Field column in SHOW COLUMNS"))?;
        // Column index 4 is the Default column (may be NULL if no default).
        let default: Option<String> = row.get(4);
        if let Some(val) = default {
            defaults.insert(field, val);
        }
    }

    Ok(defaults)
}

/// Fetch the primary-key column name for a table from upstream MariaDB.
///
/// Uses `SHOW COLUMNS` and looks for the first column whose Key field is `"PRI"`.
/// Returns `None` if no PRI column is found (e.g. the table has no explicit PK).
pub async fn fetch_pk_column(
    upstream: &mut mysql_async::Conn,
    table: &str,
) -> anyhow::Result<Option<String>> {
    use mysql_async::prelude::*;

    let sql = format!("SHOW COLUMNS FROM `{}`", table);
    let rows: Vec<mysql_async::Row> = upstream
        .query(&sql)
        .await
        .with_context(|| format!("Failed to fetch schema for table `{table}`"))?;

    for row in rows {
        let field: String = row
            .get(0)
            .ok_or_else(|| anyhow!("Missing Field column in SHOW COLUMNS"))?;
        // Column index 3 is the Key column (e.g. "PRI", "UNI", "MUL", or "").
        let key: String = row.get(3).unwrap_or_default();
        if key == "PRI" {
            return Ok(Some(field));
        }
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::overlay::registry::Registry;
    use crate::overlay::row_store::RowStore;
    use crate::overlay::schema_tracker::SchemaTracker;
    use crate::overlay::store::OverlayStore;
    use tempfile::TempDir;

    #[test]
    fn test_execute_insert_simple() {
        let dir = TempDir::new().unwrap();
        let store = OverlayStore::open(dir.path(), "testdb").unwrap();

        // Simulate: table "users" has columns (id INT, name VARCHAR(50), active BOOL)
        let columns = vec![
            ("id".to_string(), "INTEGER".to_string()),
            ("name".to_string(), "TEXT".to_string()),
            ("active".to_string(), "INTEGER".to_string()),
        ];

        let sql = "INSERT INTO users (name, active) VALUES ('alice', 1)";
        let result = execute_insert_sql(&store, sql, &columns).unwrap();

        assert_eq!(result.affected_rows, 1);
        assert!(result.last_insert_id.is_some());

        // Verify the row is in SQLite
        let row_store = RowStore::new(&store.conn);
        let rows = row_store.get_overlay_rows("users").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].op, "INSERT");
    }

    #[test]
    fn test_execute_insert_multi_row() {
        let dir = TempDir::new().unwrap();
        let store = OverlayStore::open(dir.path(), "testdb").unwrap();

        let columns = vec![
            ("id".to_string(), "INTEGER".to_string()),
            ("name".to_string(), "TEXT".to_string()),
        ];

        let sql = "INSERT INTO users (name) VALUES ('alice'), ('bob')";
        let result = execute_insert_sql(&store, sql, &columns).unwrap();

        assert_eq!(result.affected_rows, 2);

        // Verify both rows are in SQLite
        let row_store = RowStore::new(&store.conn);
        let rows = row_store.get_overlay_rows("users").unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r.op == "INSERT"));
    }

    #[test]
    fn test_execute_insert_with_explicit_id() {
        let dir = TempDir::new().unwrap();
        let store = OverlayStore::open(dir.path(), "testdb").unwrap();

        let columns = vec![
            ("id".to_string(), "INTEGER".to_string()),
            ("name".to_string(), "TEXT".to_string()),
        ];

        let sql = "INSERT INTO users (id, name) VALUES (42, 'alice')";
        let result = execute_insert_sql(&store, sql, &columns).unwrap();

        assert_eq!(result.affected_rows, 1);
        // When id is explicitly provided, no auto-generated ID
        assert!(result.last_insert_id.is_none());

        let row_store = RowStore::new(&store.conn);
        let rows = row_store.get_overlay_rows("users").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].pk, "42");
    }

    #[test]
    fn test_execute_insert_marks_table_dirty() {
        let dir = TempDir::new().unwrap();
        let store = OverlayStore::open(dir.path(), "testdb").unwrap();

        let columns = vec![
            ("id".to_string(), "INTEGER".to_string()),
            ("name".to_string(), "TEXT".to_string()),
        ];

        let sql = "INSERT INTO products (name) VALUES ('widget')";
        execute_insert_sql(&store, sql, &columns).unwrap();

        let reg = Registry::new(&store.conn);
        assert!(reg.is_dirty("products").unwrap());
    }

    #[test]
    fn test_execute_insert_auto_increment_ids_count_down() {
        let dir = TempDir::new().unwrap();
        let store = OverlayStore::open(dir.path(), "testdb").unwrap();

        let columns = vec![
            ("id".to_string(), "INTEGER".to_string()),
            ("name".to_string(), "TEXT".to_string()),
        ];

        let sql1 = "INSERT INTO users (name) VALUES ('alice')";
        let r1 = execute_insert_sql(&store, sql1, &columns).unwrap();

        let sql2 = "INSERT INTO users (name) VALUES ('bob')";
        let r2 = execute_insert_sql(&store, sql2, &columns).unwrap();

        // IDs count down from i64::MAX
        assert_eq!(r1.last_insert_id, Some(i64::MAX));
        assert_eq!(r2.last_insert_id, Some(i64::MAX - 1));
    }

    #[test]
    fn test_execute_update() {
        let dir = TempDir::new().unwrap();
        let store = OverlayStore::open(dir.path(), "testdb").unwrap();
        let schema = vec![
            ("id".to_string(), "INTEGER".to_string()),
            ("name".to_string(), "TEXT".to_string()),
        ];

        // Simulate: row id=1, name='alice' exists in base, we UPDATE to name='bob'
        let upstream_rows = vec![vec![
            ("id".to_string(), "1".to_string()),
            ("name".to_string(), "alice".to_string()),
        ]];

        let sql = "UPDATE users SET name = 'bob' WHERE id = 1";
        let result = execute_update_sql(&store, sql, &schema, &upstream_rows).unwrap();

        assert_eq!(result.affected_rows, 1);

        let row_store = RowStore::new(&store.conn);
        let rows = row_store.get_overlay_rows("users").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].pk, "1");
        assert_eq!(rows[0].op, "UPDATE");
    }

    #[test]
    fn test_execute_update_overlay_insert_row_stays_insert() {
        let dir = TempDir::new().unwrap();
        let store = OverlayStore::open(dir.path(), "testdb").unwrap();
        let schema = vec![
            ("id".to_string(), "INTEGER".to_string()),
            ("name".to_string(), "TEXT".to_string()),
        ];

        // First INSERT a row into overlay
        execute_insert_sql(
            &store,
            "INSERT INTO users (id, name) VALUES (99, 'temp')",
            &schema,
        )
        .unwrap();

        // Now UPDATE that overlay-only row — should keep _cow_op as INSERT
        let upstream_rows = vec![vec![
            ("id".to_string(), "99".to_string()),
            ("name".to_string(), "temp".to_string()),
        ]];
        let result =
            execute_update_sql(&store, "UPDATE users SET name = 'changed' WHERE id = 99", &schema, &upstream_rows)
                .unwrap();

        assert_eq!(result.affected_rows, 1);

        let row_store = RowStore::new(&store.conn);
        let rows = row_store.get_overlay_rows("users").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].pk, "99");
        assert_eq!(rows[0].op, "INSERT"); // stays INSERT, not UPDATE
    }

    #[test]
    fn test_execute_delete_tombstone() {
        let dir = TempDir::new().unwrap();
        let store = OverlayStore::open(dir.path(), "testdb").unwrap();
        let schema = vec![
            ("id".to_string(), "INTEGER".to_string()),
            ("name".to_string(), "TEXT".to_string()),
        ];

        // Ensure shadow table exists
        let row_store = RowStore::new(&store.conn);
        let col_refs: Vec<(&str, &str)> = schema
            .iter()
            .map(|(n, t)| (n.as_str(), t.as_str()))
            .collect();
        row_store.ensure_shadow_table("users", &col_refs).unwrap();

        // Delete row with PK "1" (exists in base)
        let upstream_pks = vec!["1".to_string()];
        let sql = "DELETE FROM users WHERE id = 1";
        let result = execute_delete_sql(&store, sql, &schema, &upstream_pks).unwrap();

        assert_eq!(result.affected_rows, 1);

        let rows = row_store.get_overlay_rows("users").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].op, "DELETE");
    }

    #[test]
    fn test_execute_delete_overlay_only_row() {
        let dir = TempDir::new().unwrap();
        let store = OverlayStore::open(dir.path(), "testdb").unwrap();
        let schema = vec![
            ("id".to_string(), "INTEGER".to_string()),
            ("name".to_string(), "TEXT".to_string()),
        ];

        // First INSERT a row into overlay
        execute_insert_sql(
            &store,
            "INSERT INTO users (id, name) VALUES (99, 'temp')",
            &schema,
        )
        .unwrap();

        // Now DELETE it — should remove from overlay, not create tombstone
        let upstream_pks = vec!["99".to_string()];
        let result = execute_delete_sql(
            &store,
            "DELETE FROM users WHERE id = 99",
            &schema,
            &upstream_pks,
        )
        .unwrap();

        assert_eq!(result.affected_rows, 1);

        let row_store = RowStore::new(&store.conn);
        let rows = row_store.get_overlay_rows("users").unwrap();
        assert_eq!(rows.len(), 0); // Row should be gone, not tombstoned
    }

    #[test]
    fn test_ddl_create_table() {
        let dir = TempDir::new().unwrap();
        let store = OverlayStore::open(dir.path(), "testdb").unwrap();

        let sql = "CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(100))";
        let result = execute_ddl_sql(&store, sql).unwrap();
        assert_eq!(result.affected_rows, 0);

        let reg = Registry::new(&store.conn);
        assert!(reg.is_dirty("products").unwrap());

        let tracker = SchemaTracker::new(&store.conn);
        let schema = tracker.get_overlay_schema("products").unwrap();
        assert!(schema.is_some());
        assert_eq!(schema.as_deref(), Some(sql));
    }

    #[test]
    fn test_ddl_alter_table() {
        let dir = TempDir::new().unwrap();
        let store = OverlayStore::open(dir.path(), "testdb").unwrap();

        // First create
        execute_ddl_sql(
            &store,
            "CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(100))",
        )
        .unwrap();

        // Then alter
        let alter_sql = "ALTER TABLE products ADD COLUMN price DECIMAL(10,2)";
        let result = execute_ddl_sql(&store, alter_sql).unwrap();
        assert_eq!(result.affected_rows, 0);

        let reg = Registry::new(&store.conn);
        assert!(reg.is_dirty("products").unwrap());

        let tracker = SchemaTracker::new(&store.conn);
        let schema = tracker.get_overlay_schema("products").unwrap();
        // overlay_schema is now the ALTER SQL text (v1 record-only)
        assert_eq!(schema.as_deref(), Some(alter_sql));
    }

    #[test]
    fn test_ddl_drop_table() {
        let dir = TempDir::new().unwrap();
        let store = OverlayStore::open(dir.path(), "testdb").unwrap();

        // First create
        execute_ddl_sql(&store, "CREATE TABLE temp (id INT)").unwrap();

        // Then drop
        let result = execute_ddl_sql(&store, "DROP TABLE temp").unwrap();
        assert_eq!(result.affected_rows, 0);

        let tracker = SchemaTracker::new(&store.conn);
        let schema = tracker.get_overlay_schema("temp").unwrap();
        assert!(schema.as_deref() == Some("DROPPED"));
    }

    #[test]
    fn test_ddl_drop_base_table_sets_tombstone() {
        let dir = TempDir::new().unwrap();
        let store = OverlayStore::open(dir.path(), "testdb").unwrap();

        // Drop a table that was never tracked in the overlay (simulates a base table)
        let result = execute_ddl_sql(&store, "DROP TABLE base_orders").unwrap();
        assert_eq!(result.affected_rows, 0);

        let tracker = SchemaTracker::new(&store.conn);
        let schema = tracker.get_overlay_schema("base_orders").unwrap();
        assert_eq!(schema.as_deref(), Some("DROPPED"));

        let reg = Registry::new(&store.conn);
        assert!(reg.is_dirty("base_orders").unwrap());
    }

    #[test]
    fn test_execute_insert_applies_defaults() {
        let dir = TempDir::new().unwrap();
        let store = OverlayStore::open(dir.path(), "testdb").unwrap();

        let schema = vec![
            ("id".to_string(), "INTEGER".to_string()),
            ("name".to_string(), "TEXT".to_string()),
            ("active".to_string(), "INTEGER".to_string()),
        ];
        let defaults = HashMap::from([("active".to_string(), "1".to_string())]);

        // Only name is provided — active should get default value of 1
        let sql = "INSERT INTO users (name) VALUES ('alice')";
        let result = execute_insert_sql_with_defaults(&store, sql, &schema, &defaults).unwrap();

        assert_eq!(result.affected_rows, 1);

        let row_store = RowStore::new(&store.conn);
        let rows = row_store.get_all_overlay_data("users").unwrap();
        assert_eq!(rows.len(), 1);

        let active_val = rows[0]
            .iter()
            .find(|(k, _)| k == "active")
            .map(|(_, v)| v.as_str());
        assert_eq!(active_val, Some("1"));
    }

    #[test]
    fn test_execute_insert_explicit_value_overrides_default() {
        let dir = TempDir::new().unwrap();
        let store = OverlayStore::open(dir.path(), "testdb").unwrap();

        let schema = vec![
            ("id".to_string(), "INTEGER".to_string()),
            ("name".to_string(), "TEXT".to_string()),
            ("active".to_string(), "INTEGER".to_string()),
        ];
        let defaults = HashMap::from([("active".to_string(), "1".to_string())]);

        // Explicitly set active=0, default should not override it
        let sql = "INSERT INTO users (name, active) VALUES ('bob', 0)";
        let result = execute_insert_sql_with_defaults(&store, sql, &schema, &defaults).unwrap();

        assert_eq!(result.affected_rows, 1);

        let row_store = RowStore::new(&store.conn);
        let rows = row_store.get_all_overlay_data("users").unwrap();
        assert_eq!(rows.len(), 1);

        let active_val = rows[0]
            .iter()
            .find(|(k, _)| k == "active")
            .map(|(_, v)| v.as_str());
        assert_eq!(active_val, Some("0"));
    }
}
