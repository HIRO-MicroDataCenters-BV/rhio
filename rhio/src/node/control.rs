use std::path::PathBuf;

use async_nats::Subject;

#[deprecated]
#[derive(Clone, Debug)]
pub enum NodeControl {
    /// Node should import this file from given path into MinIO database and respond with resulting
    /// bao-tree hash, optionally to NATS Core reply channel.
    ImportBlob {
        file_path: PathBuf,
        reply_subject: Option<Subject>,
    },
}
