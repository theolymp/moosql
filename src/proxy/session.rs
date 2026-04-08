use std::path::PathBuf;

use crate::bridge::temp_tables::TempTableManager;
use crate::overlay::transaction::TransactionBuffer;

pub struct Session {
    pub upstream_addr: String,
    pub overlay_dir: PathBuf,
    pub current_db: Option<String>,
    pub temp_tables: TempTableManager,
    pub tx_buffer: TransactionBuffer,
}

impl Session {
    pub fn new(upstream_addr: impl Into<String>, overlay_dir: impl Into<PathBuf>) -> Self {
        Self {
            upstream_addr: upstream_addr.into(),
            overlay_dir: overlay_dir.into(),
            current_db: None,
            temp_tables: TempTableManager::new(),
            tx_buffer: TransactionBuffer::new(),
        }
    }
}
