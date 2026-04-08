#[derive(Debug, Clone)]
pub enum BufferedOp {
    Insert {
        table: String,
        pk: String,
        data: Vec<(String, String)>,
    },
    Update {
        table: String,
        pk: String,
        data: Vec<(String, String)>,
    },
    Delete {
        table: String,
        pk: String,
    },
}

pub struct TransactionBuffer {
    active: bool,
    operations: Vec<BufferedOp>,
}

impl TransactionBuffer {
    pub fn new() -> Self {
        Self {
            active: false,
            operations: Vec::new(),
        }
    }

    pub fn begin(&mut self) {
        self.active = true;
        self.operations.clear();
    }

    pub fn is_active(&self) -> bool {
        self.active
    }

    pub fn push(&mut self, op: BufferedOp) {
        self.operations.push(op);
    }

    /// Marks the transaction as committed and returns all buffered operations.
    pub fn commit(&mut self) -> Vec<BufferedOp> {
        self.active = false;
        std::mem::take(&mut self.operations)
    }

    pub fn rollback(&mut self) {
        self.active = false;
        self.operations.clear();
    }
}

impl Default for TransactionBuffer {
    fn default() -> Self {
        Self::new()
    }
}
