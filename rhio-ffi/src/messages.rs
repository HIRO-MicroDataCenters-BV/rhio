use rhio::messages::{FileSystemEvent, Message as InnerMessage, MessageMeta as InnerMessageMeta};

use crate::types::{Hash, Path};

#[derive(Clone, Debug, uniffi::Record)]
pub struct FileSystemCreateEvent {
    path: Path,
    hash: Hash,
}

#[derive(Clone, Debug, uniffi::Object)]
pub enum Message {
    // Sync files in a directory
    FileSystem(FileSystemCreateEvent),

    // Share arbitrary blobs
    BlobAnnouncement(Hash),

    // Application messages
    Application(Vec<u8>),
}

#[derive(Debug, uniffi::Enum)]
pub enum MessageType {
    FileSystem,
    BlobAnnouncement,
    Application,
}

#[uniffi::export]
impl Message {
    pub fn r#type(&self) -> MessageType {
        match self {
            Message::FileSystem(_) => MessageType::FileSystem,
            Message::BlobAnnouncement(_) => MessageType::BlobAnnouncement,
            Message::Application(_) => MessageType::Application,
        }
    }

    #[uniffi::constructor]
    pub fn file_system(path: Path, hash: Hash) -> Self {
        Self::FileSystem(FileSystemCreateEvent { path, hash })
    }

    #[uniffi::constructor]
    pub fn blob_announcement(hash: Hash) -> Self {
        Self::BlobAnnouncement(hash)
    }

    #[uniffi::constructor]
    pub fn application(bytes: Vec<u8>) -> Self {
        Self::Application(bytes)
    }

    pub fn as_file_system_create(&self) -> FileSystemCreateEvent {
        if let Self::FileSystem(fs_event) = self {
            fs_event.clone()
        } else {
            panic!("not a FileSystem message");
        }
    }

    pub fn as_blob_announcement(&self) -> Hash {
        if let Self::BlobAnnouncement(s) = self {
            s.clone()
        } else {
            panic!("not a NeighborDown message");
        }
    }

    pub fn as_application(&self) -> Vec<u8> {
        if let Self::Application(s) = self {
            s.clone()
        } else {
            panic!("not a Application message");
        }
    }
}

#[derive(Debug, Clone, uniffi::Object)]
pub struct MessageMeta(pub(crate) InnerMessageMeta);

#[uniffi::export]
impl MessageMeta {
    #[uniffi::method]
    pub fn delivered_from(&self) -> String {
        self.0.delivered_from.to_string()
    }

    #[uniffi::method]
    pub fn operation_timestamp(&self) -> u64 {
        self.0.operation_timestamp
    }
}

impl From<InnerMessage> for Message {
    fn from(value: InnerMessage) -> Self {
        match value {
            InnerMessage::FileSystem(FileSystemEvent::Create(path, hash)) => {
                Message::FileSystem(FileSystemCreateEvent {
                    path: path.into(),
                    hash: hash.into(),
                })
            }
            InnerMessage::BlobAnnouncement(hash) => Message::BlobAnnouncement(hash.into()),
            InnerMessage::Application(bytes) => Message::Application(bytes),
            _ => unimplemented!(),
        }
    }
}

impl From<Message> for InnerMessage {
    fn from(value: Message) -> Self {
        match value {
            Message::FileSystem(FileSystemCreateEvent { path, hash }) => {
                InnerMessage::FileSystem(FileSystemEvent::Create(path.into(), hash.into()))
            }
            Message::BlobAnnouncement(hash) => InnerMessage::BlobAnnouncement(hash.into()),
            Message::Application(bytes) => InnerMessage::Application(bytes),
        }
    }
}
