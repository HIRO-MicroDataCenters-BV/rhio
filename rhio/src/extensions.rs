use p2panda_core::{Extension, Extensions, Operation};
use p2panda_store::LogId;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RhioExtensions {
    stream_name: Option<LogId>,
}

impl Extensions for RhioExtensions {}

impl Extension<LogId> for RhioExtensions {
    fn extract(operation: &Operation<RhioExtensions>) -> LogId {
        match &operation.header.extensions {
            Some(extensions) => match &extensions.stream_name {
                Some(stream_name) => stream_name.to_owned(),
                None => LogId(operation.header.public_key.to_string()),
            },
            None => LogId(operation.header.public_key.to_string()),
        }
    }
}
