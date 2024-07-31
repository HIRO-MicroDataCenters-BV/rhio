use p2panda_core::{Extension, Extensions, Operation};
use p2panda_store::LogId;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RhioExtensions {
    pub log_id: Option<LogId>,
}

impl Extensions for RhioExtensions {}

impl Extension<LogId> for RhioExtensions {
    fn extract(operation: &Operation<RhioExtensions>) -> LogId {
        match &operation.header.extensions {
            Some(extensions) => match &extensions.log_id {
                Some(log_id) => log_id.to_owned(),
                None => LogId(operation.header.public_key.to_string()),
            },
            None => LogId(operation.header.public_key.to_string()),
        }
    }
}
