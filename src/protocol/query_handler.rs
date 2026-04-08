use std::collections::HashMap;

use crate::sql::parser::TransactionOp;

pub enum QueryAction {
    Passthrough(String),
    RewrittenSelect(String),
    OverlayHandled(OverlayResult),
    TransactionControl(TransactionOp),
    Call(String),
    Rejected(String),
}

pub struct OverlayResult {
    pub affected_rows: u64,
    pub last_insert_id: Option<i64>,
    pub message: String,
}

pub fn route_query(
    sql: &str,
    dirty_tables: &[String],
    pk_columns: &HashMap<String, String>,
    truncated_tables: &[String],
) -> QueryAction {
    let parsed = match crate::sql::parser::parse_query(sql) {
        Ok(kind) => kind,
        Err(_) => return QueryAction::Passthrough(sql.to_string()),
    };

    use crate::sql::parser::QueryKind;

    match parsed {
        QueryKind::Unparseable(raw) => {
            // Forward unparseable queries to upstream — covers SHOW variants and
            // MySQL-specific syntax that sqlparser doesn't handle.
            QueryAction::Passthrough(raw)
        }

        QueryKind::Select(stmt) => {
            let tables = crate::sql::table_extractor::extract_tables_from_stmt(&stmt);
            let dirty_refs: Vec<&str> = tables
                .iter()
                .filter(|t| dirty_tables.contains(t))
                .map(|t| t.as_str())
                .collect();

            if dirty_refs.is_empty() {
                QueryAction::Passthrough(stmt.to_string())
            } else {
                let truncated_refs: Vec<&str> = truncated_tables
                    .iter()
                    .map(|t| t.as_str())
                    .collect();
                match crate::sql::rewriter::rewrite_select(sql, &dirty_refs, "_cow_temp_", pk_columns, &truncated_refs) {
                    Ok(rewritten) => QueryAction::RewrittenSelect(rewritten),
                    Err(_) => QueryAction::Passthrough(stmt.to_string()),
                }
            }
        }

        QueryKind::Insert(_) | QueryKind::Update(_) | QueryKind::Delete(_) => {
            QueryAction::OverlayHandled(OverlayResult {
                affected_rows: 0,
                last_insert_id: None,
                message: sql.to_string(),
            })
        }

        QueryKind::Ddl(_) => QueryAction::OverlayHandled(OverlayResult {
            affected_rows: 0,
            last_insert_id: None,
            message: sql.to_string(),
        }),

        QueryKind::Transaction(op) => QueryAction::TransactionControl(op),

        QueryKind::Call(_) => {
            if dirty_tables.is_empty() {
                QueryAction::Passthrough(sql.to_string())
            } else {
                QueryAction::Call(sql.to_string())
            }
        }

        QueryKind::Passthrough(stmt) => QueryAction::Passthrough(stmt.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::TransactionOp;

    #[test]
    fn test_passthrough_select_no_dirty() {
        let action = route_query("SELECT * FROM users", &[], &HashMap::new(), &[]);
        assert!(matches!(action, QueryAction::Passthrough(_)));
    }

    #[test]
    fn test_rewrite_select_dirty_table() {
        let action = route_query("SELECT * FROM users", &["users".to_string()], &HashMap::new(), &[]);
        assert!(matches!(action, QueryAction::RewrittenSelect(_)));
    }

    #[test]
    fn test_insert_goes_to_overlay() {
        let action = route_query("INSERT INTO users (name) VALUES ('alice')", &[], &HashMap::new(), &[]);
        assert!(matches!(action, QueryAction::OverlayHandled(_)));
    }

    #[test]
    fn test_ddl_goes_to_overlay() {
        let action = route_query("CREATE TABLE foo (id INT)", &[], &HashMap::new(), &[]);
        assert!(matches!(action, QueryAction::OverlayHandled(_)));
    }

    #[test]
    fn test_begin_is_transaction_control() {
        let action = route_query("BEGIN", &[], &HashMap::new(), &[]);
        assert!(matches!(
            action,
            QueryAction::TransactionControl(TransactionOp::Begin)
        ));
    }

    #[test]
    fn test_set_is_passthrough() {
        let action = route_query("SET NAMES utf8mb4", &[], &HashMap::new(), &[]);
        assert!(matches!(action, QueryAction::Passthrough(_)));
    }

    #[test]
    fn test_unparseable_is_passthrough() {
        let action = route_query("THIS IS NOT SQL AT ALL ???", &[], &HashMap::new(), &[]);
        assert!(matches!(action, QueryAction::Passthrough(_)));
    }
}
