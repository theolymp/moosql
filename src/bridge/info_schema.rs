// INFORMATION_SCHEMA query detection and overlay patching structures.
//
// When the overlay adds columns via ALTER TABLE or creates new tables,
// queries against INFORMATION_SCHEMA should reflect these changes.
// This module detects such queries and provides data structures for patching.
// The actual result-set patching happens in the wire protocol layer (Task 12).

/// Detects whether a SQL query references INFORMATION_SCHEMA.
///
/// Performs a case-insensitive check for "INFORMATION_SCHEMA." in the SQL string.
///
/// # Examples
///
/// ```
/// assert!(is_info_schema_query("SELECT * FROM information_schema.columns WHERE table_name = 'users'"));
/// assert!(is_info_schema_query("SELECT * FROM INFORMATION_SCHEMA.TABLES"));
/// assert!(!is_info_schema_query("SELECT * FROM users"));
/// ```
#[allow(dead_code)]
pub fn is_info_schema_query(sql: &str) -> bool {
    sql.to_uppercase().contains("INFORMATION_SCHEMA.")
}

/// Represents a column overlay for patching INFORMATION_SCHEMA.COLUMNS.
///
/// When overlay operations add new columns, this structure captures the metadata
/// needed to patch query results so they reflect the new schema.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct OverlayColumn {
    pub table_schema: String,
    pub table_name: String,
    pub column_name: String,
    pub data_type: String,
    pub ordinal_position: u32,
    pub is_nullable: String,
    pub column_default: Option<String>,
}

/// Represents a table overlay for patching INFORMATION_SCHEMA.TABLES.
///
/// When overlay operations create new tables, this structure captures the metadata
/// needed to patch query results so they reflect the new table definitions.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct OverlayTable {
    pub table_schema: String,
    pub table_name: String,
    pub table_type: String,
    pub engine: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_info_schema_query() {
        assert!(is_info_schema_query(
            "SELECT * FROM information_schema.columns WHERE table_name = 'users'"
        ));
        assert!(is_info_schema_query("SELECT * FROM INFORMATION_SCHEMA.TABLES"));
    }

    #[test]
    fn test_should_not_detect_regular_query() {
        assert!(!is_info_schema_query("SELECT * FROM users"));
        assert!(!is_info_schema_query("SELECT * FROM my_table"));
    }
}
