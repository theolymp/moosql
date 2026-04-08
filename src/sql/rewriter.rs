use std::collections::HashMap;

use sqlparser::ast::{Query, Select, SetExpr, Statement, TableFactor};

use super::parser::parse_single_statement;

/// Bundles the parameters that every rewriter helper needs, eliminating
/// per-function parameter sprawl.
pub struct RewriteContext<'a> {
    pub dirty_tables: &'a [&'a str],
    pub temp_prefix: &'a str,
    pub pk_columns: &'a HashMap<String, String>,
    pub truncated_tables: &'a [&'a str],
}

/// Rewrite a SELECT statement so that every dirty table reference becomes a
/// UNION-based derived subquery merging base rows with overlay rows.
///
/// If `dirty_tables` is empty the original SQL is returned unchanged.
///
/// `pk_columns` maps table names to their primary-key column name. If a table
/// is not present in the map, `"id"` is used as a fallback.
///
/// `truncated_tables` lists tables that have been TRUNCATEd in the overlay.
/// For these tables, only the overlay temp table is queried (no UNION with base).
pub fn rewrite_select(
    sql: &str,
    dirty_tables: &[&str],
    temp_prefix: &str,
    pk_columns: &HashMap<String, String>,
    truncated_tables: &[&str],
) -> anyhow::Result<String> {
    if dirty_tables.is_empty() {
        return Ok(sql.to_string());
    }

    let ctx = RewriteContext {
        dirty_tables,
        temp_prefix,
        pk_columns,
        truncated_tables,
    };

    let stmt = match parse_single_statement(sql) {
        Ok(s) => s,
        Err(_) => return Ok(sql.to_string()),
    };
    let rewritten = rewrite_statement(stmt, &ctx)?;

    Ok(rewritten.to_string())
}

fn rewrite_statement(stmt: Statement, ctx: &RewriteContext<'_>) -> anyhow::Result<Statement> {
    match stmt {
        Statement::Query(query) => {
            let rewritten = rewrite_query(*query, ctx)?;
            Ok(Statement::Query(Box::new(rewritten)))
        }
        other => Ok(other),
    }
}

fn rewrite_query(mut query: Query, ctx: &RewriteContext<'_>) -> anyhow::Result<Query> {
    match *query.body {
        SetExpr::Select(mut select) => {
            rewrite_select_from(&mut select, ctx)?;
            query.body = Box::new(SetExpr::Select(select));
            Ok(query)
        }
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            let left = rewrite_query(
                Query {
                    with: None,
                    body: left,
                    order_by: None,
                    limit_clause: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                    pipe_operators: vec![],
                },
                ctx,
            )?;
            let right = rewrite_query(
                Query {
                    with: None,
                    body: right,
                    order_by: None,
                    limit_clause: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                    pipe_operators: vec![],
                },
                ctx,
            )?;
            query.body = Box::new(SetExpr::SetOperation {
                op,
                set_quantifier,
                left: left.body,
                right: right.body,
            });
            Ok(query)
        }
        other => {
            query.body = Box::new(other);
            Ok(query)
        }
    }
}

fn rewrite_select_from(select: &mut Select, ctx: &RewriteContext<'_>) -> anyhow::Result<()> {
    for table_with_joins in &mut select.from {
        // Rewrite the primary relation.
        table_with_joins.relation =
            rewrite_table_factor(table_with_joins.relation.clone(), ctx)?;

        // Rewrite each joined relation.
        for join in &mut table_with_joins.joins {
            join.relation = rewrite_table_factor(join.relation.clone(), ctx)?;
        }
    }
    Ok(())
}

/// Replace a `TableFactor::Table` whose name is in `dirty_tables` with a
/// derived subquery that merges base rows with overlay temp-table rows.
///
/// If the table is also in `truncated_tables`, only the overlay temp table is
/// used (no UNION with the base table — all base rows are suppressed).
///
/// `TableFactor::Derived` subqueries are recursed into so that nested SELECTs
/// are also rewritten.
fn rewrite_table_factor(
    factor: TableFactor,
    ctx: &RewriteContext<'_>,
) -> anyhow::Result<TableFactor> {
    match factor {
        TableFactor::Table {
            ref name,
            ref alias,
            ..
        } => {
            // Extract the simple table name (last identifier segment).
            let table_name = name
                .0
                .last()
                .and_then(|p| p.as_ident())
                .map(|i| i.value.clone())
                .unwrap_or_else(|| name.to_string());

            if !ctx.dirty_tables.contains(&table_name.as_str()) {
                return Ok(factor);
            }

            // Determine alias: use the existing alias name if present, else the table name.
            let alias_name = alias
                .as_ref()
                .map(|a| a.name.value.clone())
                .unwrap_or_else(|| table_name.clone());

            let temp_table = format!("{}{}", ctx.temp_prefix, table_name);

            // If the table was TRUNCATEd, return only overlay rows — skip the base.
            let query_sql = if ctx.truncated_tables.contains(&table_name.as_str()) {
                format!(
                    "SELECT * FROM (SELECT * FROM {temp}) AS {alias}",
                    temp = temp_table,
                    alias = alias_name,
                )
            } else {
                let meta_table = format!("_cow_meta_{}", table_name);

                // Look up the PK column for this table; fall back to "id" if unknown.
                let pk_col = ctx
                    .pk_columns
                    .get(&table_name)
                    .map(|s| s.as_str())
                    .unwrap_or("id");

                // Build the UNION subquery as a SQL string then parse it back so we
                // get a proper AST node without constructing every field by hand.
                //
                // _cow_meta_<table> holds ALL overlay PKs (used for NOT IN).
                // _cow_temp_<table> holds only non-tombstone data rows with the
                // same column set as the base table, so SELECT * is safe.
                format!(
                    "SELECT * FROM (\
                        SELECT * FROM {table} WHERE CAST({pk_col} AS CHAR) NOT IN (SELECT _cow_pk FROM {meta}) \
                        UNION ALL \
                        SELECT * FROM {temp}\
                    ) AS {alias}",
                    table = table_name,
                    pk_col = pk_col,
                    meta = meta_table,
                    temp = temp_table,
                    alias = alias_name,
                )
            };

            let outer_stmt = parse_single_statement(&query_sql)
                .map_err(|e| anyhow::anyhow!("Failed to parse generated SQL: {e}"))?;

            // Unwrap: SELECT * FROM (<subquery>) AS alias  — we want the FROM factor.
            if let Statement::Query(outer_query) = outer_stmt {
                if let SetExpr::Select(outer_select) = *outer_query.body {
                    if let Some(first_from) = outer_select.from.into_iter().next() {
                        return Ok(first_from.relation);
                    }
                }
            }

            anyhow::bail!("Unexpected AST shape when building subquery for {table_name}");
        }

        TableFactor::Derived {
            lateral,
            subquery,
            alias,
            sample,
        } => {
            // Recurse into derived subqueries.
            let rewritten = rewrite_query(*subquery, ctx)?;
            Ok(TableFactor::Derived {
                lateral,
                subquery: Box::new(rewritten),
                alias,
                sample,
            })
        }

        // All other table factor variants are left untouched.
        other => Ok(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rewrite_simple_select_with_dirty_table() {
        let result = rewrite_select(
            "SELECT * FROM users WHERE active = 1",
            &["users"],
            "_cow_temp_",
            &HashMap::new(),
            &[],
        )
        .unwrap();
        assert!(result.contains("UNION ALL"));
        assert!(result.contains("_cow_temp_users"));
        assert!(result.contains("_cow_meta_users")); // meta table used for NOT IN filter
    }

    #[test]
    fn test_no_rewrite_when_no_dirty_tables() {
        let sql = "SELECT * FROM users WHERE active = 1";
        let result = rewrite_select(sql, &[], "_cow_temp_", &HashMap::new(), &[]).unwrap();
        assert!(!result.contains("UNION"));
    }

    #[test]
    fn test_rewrite_join_one_dirty() {
        let result = rewrite_select(
            "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
            &["users"],
            "_cow_temp_",
            &HashMap::new(),
            &[],
        )
        .unwrap();
        assert!(result.contains("_cow_temp_users"));
        assert!(!result.contains("_cow_temp_orders"));
    }

    #[test]
    fn test_rewrite_join_both_dirty() {
        let result = rewrite_select(
            "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
            &["users", "orders"],
            "_cow_temp_",
            &HashMap::new(),
            &[],
        )
        .unwrap();
        assert!(result.contains("_cow_temp_users"));
        assert!(result.contains("_cow_temp_orders"));
    }

    #[test]
    fn test_rewrite_uses_actual_pk_column() {
        let mut pk_columns = HashMap::new();
        pk_columns.insert("employee_projects".to_string(), "employee_id".to_string());

        let result = rewrite_select(
            "SELECT * FROM employee_projects",
            &["employee_projects"],
            "_cow_temp_",
            &pk_columns,
            &[],
        )
        .unwrap();
        assert!(result.contains("CAST(employee_id AS CHAR)"));
        assert!(!result.contains("CAST(id AS CHAR)"));
    }

    #[test]
    fn test_rewrite_falls_back_to_id_when_pk_unknown() {
        let result = rewrite_select(
            "SELECT * FROM users",
            &["users"],
            "_cow_temp_",
            &HashMap::new(),
            &[],
        )
        .unwrap();
        assert!(result.contains("CAST(id AS CHAR)"));
    }

    #[test]
    fn test_rewrite_truncated_table() {
        let result = rewrite_select(
            "SELECT * FROM users WHERE active = 1",
            &["users"],
            "_cow_temp_",
            &HashMap::new(),
            &["users"], // truncated
        )
        .unwrap();
        // Should NOT contain base table reference or UNION
        assert!(!result.contains("UNION"));
        assert!(result.contains("_cow_temp_users"));
        // Should not reference _cow_meta or the raw base table
        assert!(!result.contains("_cow_meta_users"));
    }
}
