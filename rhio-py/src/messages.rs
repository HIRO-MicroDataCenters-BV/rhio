use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use rhio::messages::{Message as InnerMessage, MessageMeta as InnerMessageMeta};
use rhio::node::TopicSender;

use crate::error::{CallbackError, RhioError};

type Hash = String;
type Path = String;

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
    pub fn blob_announcement(hash: String) -> Result<Self, RhioError> {
        Ok(Self::BlobAnnouncement(
            hash.parse().map_err(anyhow::Error::from)?,
        ))
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

    pub fn as_blob_announcement(&self) -> String {
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

    pub fn operation_timestamp(&self) -> u64 {
        self.0.operation_timestamp
    }
}

impl From<rhio::messages::Message<Vec<u8>>> for Message {
    fn from(value: rhio::messages::Message<Vec<u8>>) -> Self {
        match value {
            InnerMessage::FileSystem(rhio::messages::FileSystemEvent::Create(path, hash)) => {
                Message::FileSystem(FileSystemCreateEvent {
                    path: path.to_string_lossy().to_string(),
                    hash: hash.to_string(),
                })
            }
            InnerMessage::BlobAnnouncement(hash) => Message::BlobAnnouncement(hash.to_string()),
            InnerMessage::Application(bytes) => Message::Application(bytes),
            _ => unimplemented!(),
        }
    }
}

impl TryFrom<Message> for rhio::messages::Message<Vec<u8>> {
    type Error = anyhow::Error;

    fn try_from(value: Message) -> Result<Self, Self::Error> {
        let value = match value {
            Message::FileSystem(FileSystemCreateEvent { path, hash }) => {
                rhio::messages::Message::FileSystem(rhio::messages::FileSystemEvent::Create(
                    PathBuf::from(path),
                    hash.parse()?,
                ))
            }
            Message::BlobAnnouncement(hash) => {
                rhio::messages::Message::BlobAnnouncement(hash.parse()?)
            }
            Message::Application(bytes) => rhio::messages::Message::Application(bytes),
        };
        Ok(value)
    }
}

#[uniffi::export(with_foreign)]
#[async_trait::async_trait]
pub trait GossipMessageCallback: Send + Sync + 'static {
    async fn on_message(
        &self,
        msg: Arc<Message>,
        meta: Arc<MessageMeta>,
    ) -> Result<(), CallbackError>;
}

#[derive(uniffi::Object)]
pub struct Sender{
    pub(crate) inner: TopicSender<Vec<u8>>,
    pub ready_fut: Mutex<Option<Pin<Box<dyn Future<Output=()> + Send + 'static>>>>
}

#[uniffi::export]
impl Sender {
    pub async fn send(&self, message: &Message) -> Result<MessageMeta, RhioError> {
        let message = rhio::messages::Message::<Vec<u8>>::try_from(message.clone())?;
        let meta = self.inner.send(message).await?;
        Ok(MessageMeta(meta))
    }

    pub async fn ready(&self) {
        let fut = self.ready_fut.lock().unwrap().take();
        match fut {
            Some(fut) => fut.await,
            None => (),
        }
    } 
}
