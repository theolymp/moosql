use std::collections::HashMap;

use crate::sql::parser::{parse_query, QueryKind};
use crate::sql::rewriter::rewrite_select;

/// Rewrite a stored procedure body by parsing each statement and rewriting
/// any SELECT statements that reference dirty tables.
///
/// The body must be wrapped in `BEGIN...END`. The wrapper is stripped,
/// statements are split on semicolons, SELECTs referencing dirty tables are
/// rewritten with UNION-based overlays, and the result is reassembled as
/// `BEGIN\n...\nEND`.
///
/// `pk_columns` maps table names to their primary-key column name. If a table
/// is not present in the map, `"id"` is used as a fallback.
///
/// `truncated_tables` lists tables that have been TRUNCATEd in the overlay.
pub fn rewrite_sp_body(
    body: &str,
    dirty_tables: &[&str],
    temp_prefix: &str,
    pk_columns: &HashMap<String, String>,
    truncated_tables: &[&str],
) -> anyhow::Result<String> {
    // Strip the BEGIN/END wrapper (case-insensitive, trimmed).
    let trimmed = body.trim();
    let inner = strip_begin_end(trimmed)?;

    // Split on semicolons and process each statement.
    let mut rewritten_stmts: Vec<String> = Vec::new();

    for raw in inner.split(';') {
        let stmt = raw.trim();
        if stmt.is_empty() {
            continue;
        }

        let kind = parse_query(stmt)?;
        let rewritten = match kind {
            QueryKind::Select(_) => rewrite_select(stmt, dirty_tables, temp_prefix, pk_columns, truncated_tables)?,
            _ => stmt.to_string(),
        };

        rewritten_stmts.push(rewritten);
    }

    let body_inner = rewritten_stmts
        .iter()
        .map(|s| format!("{};", s))
        .collect::<Vec<_>>()
        .join("\n");

    Ok(format!("BEGIN\n{}\nEND", body_inner))
}

/// Like `rewrite_sp_body`, but returns individual rewritten statements
/// instead of wrapping in BEGIN/END. Useful for executing rewritten SP
/// bodies as ad-hoc SQL (since BEGIN/END blocks need multi-statement mode).
pub fn rewrite_sp_body_statements(
    body: &str,
    dirty_tables: &[&str],
    temp_prefix: &str,
    pk_columns: &HashMap<String, String>,
    truncated_tables: &[&str],
) -> anyhow::Result<Vec<String>> {
    let trimmed = body.trim();
    let inner = strip_begin_end(trimmed)?;

    let mut stmts = Vec::new();
    for raw in inner.split(';') {
        let stmt = raw.trim();
        if stmt.is_empty() {
            continue;
        }
        let kind = parse_query(stmt)?;
        let rewritten = match kind {
            QueryKind::Select(_) => rewrite_select(stmt, dirty_tables, temp_prefix, pk_columns, truncated_tables)?,
            _ => stmt.to_string(),
        };
        stmts.push(rewritten);
    }
    Ok(stmts)
}

/// Extract SP body from SHOW CREATE PROCEDURE output, rewrite, return individual statements.
pub fn rewrite_sp_from_definition_statements(
    create_sql: &str,
    dirty_tables: &[&str],
    temp_prefix: &str,
    pk_columns: &HashMap<String, String>,
    truncated_tables: &[&str],
) -> anyhow::Result<Vec<String>> {
    let upper = create_sql.to_uppercase();
    let begin = upper.find("BEGIN")
        .ok_or_else(|| anyhow::anyhow!("No BEGIN found in SP definition"))?;
    let end = upper.rfind("END")
        .ok_or_else(|| anyhow::anyhow!("No END found in SP definition"))?;
    let body = &create_sql[begin..end + 3];
    rewrite_sp_body_statements(body, dirty_tables, temp_prefix, pk_columns, truncated_tables)
}

/// Extract the SP body from a `SHOW CREATE PROCEDURE` output and rewrite it.
///
/// Finds the first `BEGIN` and the last `END` in `create_sql`, extracts
/// everything between (inclusive), then delegates to `rewrite_sp_body`.
pub fn rewrite_sp_from_definition(
    create_sql: &str,
    dirty_tables: &[&str],
    temp_prefix: &str,
    pk_columns: &HashMap<String, String>,
    truncated_tables: &[&str],
) -> anyhow::Result<String> {
    // Find first BEGIN (case-insensitive).
    let begin_pos = create_sql
        .to_uppercase()
        .find("BEGIN")
        .ok_or_else(|| anyhow::anyhow!("No BEGIN found in CREATE PROCEDURE definition"))?;

    // Find last END (case-insensitive).
    let upper = create_sql.to_uppercase();
    let end_pos = upper
        .rfind("END")
        .ok_or_else(|| anyhow::anyhow!("No END found in CREATE PROCEDURE definition"))?;

    if end_pos < begin_pos {
        anyhow::bail!("END appears before BEGIN in CREATE PROCEDURE definition");
    }

    // Extract inclusive of BEGIN and END (3 chars).
    let body = &create_sql[begin_pos..end_pos + 3];
    rewrite_sp_body(body, dirty_tables, temp_prefix, pk_columns, truncated_tables)
}

/// Strip the `BEGIN` prefix and `END` suffix from an SP body string.
fn strip_begin_end(body: &str) -> anyhow::Result<&str> {
    let upper = body.to_uppercase();

    let after_begin = if upper.starts_with("BEGIN") {
        body["BEGIN".len()..].trim_start_matches(['\n', '\r', ' ', '\t'])
    } else {
        anyhow::bail!("SP body does not start with BEGIN");
    };

    // Find the last END (case-insensitive).
    let upper_inner = after_begin.to_uppercase();
    let end_pos = upper_inner
        .rfind("END")
        .ok_or_else(|| anyhow::anyhow!("SP body does not contain END"))?;

    let inner = after_begin[..end_pos].trim_end_matches(['\n', '\r', ' ', '\t']);
    Ok(inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rewrite_simple_sp_body() {
        let body = "BEGIN\nSELECT * FROM users WHERE active = 1;\nINSERT INTO log (msg) VALUES ('queried');\nEND";
        let result = rewrite_sp_body(body, &["users"], "_cow_temp_", &HashMap::new(), &[]).unwrap();
        assert!(result.contains("UNION ALL"));
        assert!(result.contains("_cow_temp_users"));
    }

    #[test]
    fn test_sp_body_no_dirty_tables() {
        let body = "BEGIN\nSELECT * FROM orders;\nEND";
        let result = rewrite_sp_body(body, &[], "_cow_temp_", &HashMap::new(), &[]).unwrap();
        assert!(!result.contains("UNION"));
    }
}
