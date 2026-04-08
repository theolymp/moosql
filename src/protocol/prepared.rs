use std::collections::HashMap;

pub struct PreparedStatementCache {
    statements: HashMap<u32, (String, String)>, // id -> (original, rewritten)
    next_id: u32,
}

impl PreparedStatementCache {
    pub fn new() -> Self {
        Self {
            statements: HashMap::new(),
            next_id: 1,
        }
    }

    pub fn prepare(&mut self, original: String, rewritten: String) -> u32 {
        let id = self.next_id;
        self.statements.insert(id, (original, rewritten));
        self.next_id += 1;
        id
    }

    pub fn get_rewritten(&self, id: u32) -> Option<&str> {
        self.statements.get(&id).map(|(_, rewritten)| rewritten.as_str())
    }

    pub fn deallocate(&mut self, id: u32) {
        self.statements.remove(&id);
    }
}
