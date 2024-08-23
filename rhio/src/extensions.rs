use p2panda_core::Extension;
use serde::{Deserialize, Serialize};

pub type LogId = String;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RhioExtensions {
    pub log_id: Option<LogId>,
}

impl Extension<LogId> for RhioExtensions {
    fn extract(&self) -> Option<LogId> {
        self.log_id.clone()
    }
}
