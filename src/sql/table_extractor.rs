use std::ops::ControlFlow;

use sqlparser::ast::{Statement, visit_relations};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

/// Extract table names from a SQL string. Returns deduplicated list of table
/// names, using only the last part of dotted names (e.g. `mydb.users` → `users`).
pub fn extract_tables(sql: &str) -> anyhow::Result<Vec<String>> {
    let dialect = MySqlDialect {};
    let stmts = Parser::parse_sql(&dialect, sql)?;
    let mut tables = Vec::new();
    let _ = visit_relations(&stmts, |relation| {
        let name = relation
            .0
            .last()
            .and_then(|part| part.as_ident())
            .map(|ident| ident.value.clone())
            .unwrap_or_else(|| relation.to_string());
        if !tables.contains(&name) {
            tables.push(name);
        }
        ControlFlow::<()>::Continue(())
    });
    Ok(tables)
}

/// Extract table names from an already-parsed statement.
pub fn extract_tables_from_stmt(stmt: &Statement) -> Vec<String> {
    let mut tables = Vec::new();
    let _ = visit_relations(stmt, |relation| {
        let name = relation
            .0
            .last()
            .and_then(|part| part.as_ident())
            .map(|ident| ident.value.clone())
            .unwrap_or_else(|| relation.to_string());
        if !tables.contains(&name) {
            tables.push(name);
        }
        ControlFlow::<()>::Continue(())
    });
    tables
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_single_table() {
        let tables = extract_tables("SELECT * FROM users").unwrap();
        assert_eq!(tables, vec!["users"]);
    }

    #[test]
    fn test_extract_join_tables() {
        let tables =
            extract_tables("SELECT * FROM orders JOIN users ON orders.user_id = users.id").unwrap();
        assert!(tables.contains(&"orders".to_string()));
        assert!(tables.contains(&"users".to_string()));
        assert_eq!(tables.len(), 2);
    }

    #[test]
    fn test_extract_subquery_tables() {
        let tables =
            extract_tables("SELECT * FROM orders WHERE user_id IN (SELECT id FROM users)")
                .unwrap();
        assert!(tables.contains(&"orders".to_string()));
        assert!(tables.contains(&"users".to_string()));
        assert_eq!(tables.len(), 2);
    }

    #[test]
    fn test_extract_insert_table() {
        let tables = extract_tables("INSERT INTO orders (name) VALUES ('x')").unwrap();
        assert_eq!(tables, vec!["orders"]);
    }

    #[test]
    fn test_extract_update_table() {
        let tables = extract_tables("UPDATE products SET price = 10 WHERE id = 1").unwrap();
        assert_eq!(tables, vec!["products"]);
    }

    #[test]
    fn test_extract_qualified_table() {
        let tables = extract_tables("SELECT * FROM mydb.users").unwrap();
        assert_eq!(tables, vec!["users"]);
    }
}
