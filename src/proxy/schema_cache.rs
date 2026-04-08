use std::collections::HashMap;

/// Describes a foreign key relationship between two tables.
#[derive(Clone, Debug)]
pub struct ForeignKeyInfo {
    pub child_table: String,
    pub child_column: String,
    pub parent_table: String,
    pub parent_column: String,
    pub on_delete: FkAction,
    pub on_update: FkAction,
}

/// The action to take when a referenced row is deleted or updated.
#[derive(Clone, Debug, PartialEq)]
pub enum FkAction {
    Restrict,
    Cascade,
    SetNull,
    NoAction,
    SetDefault,
}

impl FkAction {
    pub fn from_str(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "CASCADE" => Self::Cascade,
            "SET NULL" => Self::SetNull,
            "SET DEFAULT" => Self::SetDefault,
            "NO ACTION" => Self::NoAction,
            _ => Self::Restrict,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CachedColumn {
    pub name: String,
    pub col_type: String,
    pub default: Option<String>,
    pub is_pk: bool,
}

#[derive(Clone, Debug)]
pub struct CachedTableSchema {
    pub columns: Vec<CachedColumn>,
}

impl CachedTableSchema {
    /// Get (name, type) pairs for compatibility with existing code.
    pub fn schema_pairs(&self) -> Vec<(String, String)> {
        self.columns.iter().map(|c| (c.name.clone(), c.col_type.clone())).collect()
    }

    /// Get column defaults map.
    pub fn defaults(&self) -> HashMap<String, String> {
        self.columns.iter()
            .filter_map(|c| c.default.as_ref().map(|d| (c.name.clone(), d.clone())))
            .collect()
    }

    /// Get the PK column name (first PRI column, or "id" fallback).
    pub fn pk_column(&self) -> String {
        self.columns.iter()
            .find(|c| c.is_pk)
            .map(|c| c.name.clone())
            .unwrap_or_else(|| "id".to_string())
    }
}

pub struct SchemaCache {
    cache: HashMap<String, CachedTableSchema>,
    fk_relations: Option<Vec<ForeignKeyInfo>>,
}

impl SchemaCache {
    pub fn new() -> Self {
        Self { cache: HashMap::new(), fk_relations: None }
    }

    pub fn get(&self, table: &str) -> Option<&CachedTableSchema> {
        self.cache.get(table)
    }

    pub fn insert(&mut self, table: String, schema: CachedTableSchema) {
        self.cache.insert(table, schema);
    }

    pub fn invalidate(&mut self, table: &str) {
        self.cache.remove(table);
    }

    pub fn invalidate_all(&mut self) {
        self.cache.clear();
    }

    /// Returns all FK relations where the given table is the parent (referenced) table.
    pub fn get_child_fks(&self, parent_table: &str) -> Vec<&ForeignKeyInfo> {
        match &self.fk_relations {
            Some(rels) => rels
                .iter()
                .filter(|fk| fk.parent_table.eq_ignore_ascii_case(parent_table))
                .collect(),
            None => Vec::new(),
        }
    }

    /// Store the FK relations for the current database.
    pub fn set_fk_relations(&mut self, relations: Vec<ForeignKeyInfo>) {
        self.fk_relations = Some(relations);
    }

    /// Whether FK relations have been loaded yet.
    pub fn fk_relations_loaded(&self) -> bool {
        self.fk_relations.is_some()
    }
}
