use p2panda_store::LogId;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Extensions {
    stream_name: Option<LogId>,
}

impl p2panda_core::Extensions for Extensions {}

impl p2panda_core::Extension<LogId> for Extensions {
    fn extract(operation: &p2panda_core::Operation<Extensions>) -> LogId {
        match &operation.header.extensions {
            Some(extensions) => match &extensions.stream_name {
                Some(stream_name) => stream_name.to_owned(),
                None => LogId(operation.header.public_key.to_string()),
            },
            None => LogId(operation.header.public_key.to_string()),
        }
    }
}
