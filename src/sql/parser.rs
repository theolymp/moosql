use sqlparser::ast::Statement;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

#[derive(Debug)]
pub enum TransactionOp {
    Begin,
    Commit,
    Rollback,
}

#[derive(Debug)]
pub enum QueryKind {
    Select(Statement),
    Insert(Statement),
    Update(Statement),
    Delete(Statement),
    Ddl(Statement),
    Transaction(TransactionOp),
    Call(Statement),
    Passthrough(Statement),
    Unparseable(String),
}

/// Parse `sql` and return the first (and expected only) statement.
/// Returns an error if the SQL cannot be parsed or yields no statements.
pub fn parse_single_statement(sql: &str) -> anyhow::Result<Statement> {
    let dialect = MySqlDialect {};
    let mut stmts = Parser::parse_sql(&dialect, sql)?;
    stmts.into_iter().next().ok_or_else(|| anyhow::anyhow!("Empty SQL"))
}

pub fn parse_query(sql: &str) -> anyhow::Result<QueryKind> {
    let dialect = MySqlDialect {};
    let result = Parser::parse_sql(&dialect, sql);

    let mut stmts = match result {
        Ok(stmts) => stmts,
        Err(_) => return Ok(QueryKind::Unparseable(sql.to_string())),
    };

    if stmts.is_empty() {
        return Ok(QueryKind::Unparseable(sql.to_string()));
    }

    let stmt = stmts.remove(0);
    let kind = match &stmt {
        Statement::Query(_) => QueryKind::Select(stmt),
        Statement::Insert(_) => QueryKind::Insert(stmt),
        Statement::Update(_) => QueryKind::Update(stmt),
        Statement::Delete(_) => QueryKind::Delete(stmt),
        Statement::CreateTable(_)
        | Statement::CreateIndex(_)
        | Statement::CreateView(_)
        | Statement::AlterTable(_)
        | Statement::Drop { .. }
        | Statement::Truncate(_) => QueryKind::Ddl(stmt),
        Statement::StartTransaction { .. } => QueryKind::Transaction(TransactionOp::Begin),
        Statement::Commit { .. } => QueryKind::Transaction(TransactionOp::Commit),
        Statement::Rollback { .. } => QueryKind::Transaction(TransactionOp::Rollback),
        Statement::Call(_) => QueryKind::Call(stmt),
        _ => QueryKind::Passthrough(stmt),
    };

    Ok(kind)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_select() {
        let kind = parse_query("SELECT * FROM users").unwrap();
        assert!(matches!(kind, QueryKind::Select(_)));
    }

    #[test]
    fn test_classify_insert() {
        let kind = parse_query("INSERT INTO users (name) VALUES ('alice')").unwrap();
        assert!(matches!(kind, QueryKind::Insert(_)));
    }

    #[test]
    fn test_classify_update() {
        let kind = parse_query("UPDATE users SET name = 'bob' WHERE id = 1").unwrap();
        assert!(matches!(kind, QueryKind::Update(_)));
    }

    #[test]
    fn test_classify_delete() {
        let kind = parse_query("DELETE FROM users WHERE id = 1").unwrap();
        assert!(matches!(kind, QueryKind::Delete(_)));
    }

    #[test]
    fn test_classify_ddl_create() {
        let kind = parse_query("CREATE TABLE foo (id INT PRIMARY KEY)").unwrap();
        assert!(matches!(kind, QueryKind::Ddl(_)));
    }

    #[test]
    fn test_classify_ddl_alter() {
        let kind = parse_query("ALTER TABLE foo ADD COLUMN bar VARCHAR(255)").unwrap();
        assert!(matches!(kind, QueryKind::Ddl(_)));
    }

    #[test]
    fn test_classify_ddl_drop() {
        let kind = parse_query("DROP TABLE foo").unwrap();
        assert!(matches!(kind, QueryKind::Ddl(_)));
    }

    #[test]
    fn test_classify_passthrough_set() {
        let kind = parse_query("SET NAMES utf8mb4").unwrap();
        assert!(matches!(kind, QueryKind::Passthrough(_)));
    }

    #[test]
    fn test_classify_begin() {
        let kind = parse_query("BEGIN").unwrap();
        assert!(matches!(kind, QueryKind::Transaction(TransactionOp::Begin)));
    }

    #[test]
    fn test_classify_call() {
        let kind = parse_query("CALL my_procedure()").unwrap();
        assert!(matches!(kind, QueryKind::Call(_)));
    }
}
