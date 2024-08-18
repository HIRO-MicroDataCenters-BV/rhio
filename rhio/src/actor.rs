use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::time::{self, SystemTime};

use anyhow::{Context, Result};
use futures_lite::FutureExt;
use iroh_blobs::store::bao_tree::io::fsm::AsyncSliceReader;
use iroh_blobs::store::{MapEntry, Store};
use p2panda_blobs::{Blobs, DownloadBlobEvent, ImportBlobEvent};
use p2panda_core::{Hash, PrivateKey};
use p2panda_net::network::{InEvent, OutEvent};
use p2panda_store::MemoryStore as LogsMemoryStore;
use s3::creds::Credentials;
use s3::{Bucket, BucketConfiguration, Region};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::{Stream, StreamExt, StreamMap};
use tokio_util::task::LocalPoolHandle;
use tracing::{debug, error, info};

use crate::extensions::RhioExtensions;
use crate::messages::{FromBytes, GossipOperation, Message, MessageMeta, ToBytes};
use crate::operations::{create, ingest};
use crate::topic_id::TopicId;

pub type SubscribeResult<T> = Result<(
    broadcast::Receiver<(Message<T>, MessageMeta)>,
    Pin<Box<dyn Future<Output = ()> + Send>>,
)>;

pub enum ToRhioActor<T> {
    ImportFile {
        path: PathBuf,
        reply: oneshot::Sender<Result<Hash>>,
    },
    ImportUrl {
        url: String,
        reply: oneshot::Sender<Result<Hash>>,
    },
    ExportBlobFilesystem {
        hash: Hash,
        path: PathBuf,
        reply: oneshot::Sender<Result<()>>,
    },
    ExportBlobMinio {
        hash: Hash,
        bucket_name: String,
        region: Region,
        credentials: Credentials,
        reply: oneshot::Sender<Result<()>>,
    },
    DownloadBlob {
        hash: Hash,
        reply: oneshot::Sender<Result<()>>,
    },
    PublishEvent {
        topic: TopicId,
        message: Message<T>,
        reply: oneshot::Sender<Result<MessageMeta>>,
    },
    Subscribe {
        topic: TopicId,
        topic_tx: mpsc::Sender<InEvent>,
        topic_rx: broadcast::Receiver<OutEvent>,
        reply: oneshot::Sender<SubscribeResult<T>>,
    },
    Shutdown,
}

pub struct RhioActor<T, S>
where
    T: Serialize + DeserializeOwned + Clone + std::fmt::Debug,
    S: Store + Clone + Send + Sync + 'static,
{
    blobs: Blobs<S>,
    private_key: PrivateKey,
    store: LogsMemoryStore<RhioExtensions>,
    topic_gossip_tx: HashMap<TopicId, mpsc::Sender<InEvent>>,
    topic_topic_rx: StreamMap<TopicId, Pin<Box<dyn Stream<Item = OutEvent> + Send + 'static>>>,
    topic_subscribers_tx: HashMap<TopicId, broadcast::Sender<(Message<T>, MessageMeta)>>,
    broadcast_join: broadcast::Sender<TopicId>,
    joined_topics: HashSet<TopicId>,
    inbox: mpsc::Receiver<ToRhioActor<T>>,
}

impl<T, S> RhioActor<T, S>
where
    T: Serialize + DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
    S: Store + Clone + Send + Sync + 'static,
{
    pub fn spawn(
        private_key: PrivateKey,
        blobs: Blobs<S>,
        store: LogsMemoryStore<RhioExtensions>,
        inbox: mpsc::Receiver<ToRhioActor<T>>,
        rt: LocalPoolHandle,
    ) -> JoinHandle<()> {
        let (broadcast_join, _) = broadcast::channel::<TopicId>(128);
        let mut actor = Self {
            private_key,
            blobs,
            store,
            topic_gossip_tx: HashMap::default(),
            topic_topic_rx: StreamMap::default(),
            topic_subscribers_tx: HashMap::new(),
            broadcast_join,
            joined_topics: HashSet::new(),
            inbox,
        };
        let task = rt.spawn_pinned(|| async move {
            if let Err(err) = actor.run().await {
                panic!("rhio actor failed: {err:?}");
            }
        });
        task
    }

    pub async fn run(&mut self) -> Result<()> {
        loop {
            tokio::select! {
                Some(msg) = self.inbox.recv() => {
                    if !self
                        .on_actor_message(msg)
                        .await
                        .context("on_actor_message")?
                    {
                        return Ok(());
                    }

                },
                Some((topic_id, msg)) = self.topic_topic_rx.next() => {
                    self
                        .on_gossip_event(topic_id, msg)
                        .await;
                }
            }
        }
    }

    async fn send_operation(
        &mut self,
        topic: TopicId,
        operation: GossipOperation<T>,
    ) -> Result<()> {
        match self.topic_gossip_tx.get_mut(&topic) {
            Some(tx) => {
                tx.send(InEvent::Message {
                    bytes: operation.to_bytes(),
                })
                .await
            }
            None => {
                return Err(anyhow::anyhow!(
                    "Attempted to send operation on unknown topic {topic:?}"
                ))
            }
        }?;
        Ok(())
    }

    async fn on_actor_message(&mut self, msg: ToRhioActor<T>) -> Result<bool> {
        match msg {
            ToRhioActor::ImportUrl { url, reply } => {
                let result = self.on_import_url(&url).await;
                if let Ok(hash) = result {
                    info!("imported blob: {hash} {url}");
                }
                reply.send(result).ok();
            }
            ToRhioActor::ImportFile { path, reply } => {
                let result = self.on_import_blob(&path).await;
                if let Ok(hash) = result {
                    info!("imported blob: {hash} {path:?}");
                }
                reply.send(result).ok();
            }
            ToRhioActor::ExportBlobFilesystem { path, reply, hash } => {
                let result = self.on_export_blob_filesystem(hash, &path).await;
                if result.is_ok() {
                    info!("exported blob to filesystem: {hash} {path:?}");
                }
                reply.send(result).ok();
            }
            ToRhioActor::DownloadBlob { hash, reply } => {
                let result = self.on_download_blob(hash).await;
                if result.is_ok() {
                    info!("downloaded blob {hash}");
                }
                reply.send(result).ok();
            }
            ToRhioActor::ExportBlobMinio {
                hash,
                bucket_name,
                region,
                credentials,
                reply,
            } => {
                let result = self
                    .on_export_blob_minio(hash, bucket_name.clone(), region, credentials)
                    .await;
                if result.is_ok() {
                    info!("exported blob to minio: {hash} {bucket_name}");
                }
                reply.send(result).ok();
            }
            ToRhioActor::PublishEvent {
                topic,
                message,
                reply,
            } => {
                let result = self.on_publish_event(topic, message).await;
                reply.send(result).ok();
            }
            ToRhioActor::Shutdown => {
                return Ok(false);
            }
            ToRhioActor::Subscribe {
                topic,
                topic_tx,
                topic_rx,
                reply,
            } => {
                let result = self.on_subscribe(topic, topic_tx, topic_rx).await;
                reply.send(result).ok();
            }
        }

        Ok(true)
    }

    async fn on_subscribe(
        &mut self,
        topic: TopicId,
        topic_tx: mpsc::Sender<InEvent>,
        mut topic_rx: broadcast::Receiver<OutEvent>,
    ) -> Result<(
        broadcast::Receiver<(Message<T>, MessageMeta)>,
        Pin<Box<dyn Future<Output = ()> + Send>>,
    )> {
        // If we didn't already subscribe to this topic, then add the topic gossip channels
        // to our sender and receiver maps.
        if !self.topic_gossip_tx.contains_key(&topic) {
            self.topic_gossip_tx.insert(topic, topic_tx);

            let rx_stream = Box::pin(async_stream::stream! {
              while let Ok(item) = topic_rx.recv().await {
                  yield item;
              }
            });

            self.topic_topic_rx.insert(topic, rx_stream);
        }

        // Get a receiver channel which will be sent decoded gossip events arriving on this topic.
        let rx = if let Some(tx) = self.topic_subscribers_tx.get(&topic) {
            tx.subscribe()
        } else {
            let (tx, rx) = broadcast::channel(128);
            self.topic_subscribers_tx.insert(topic, tx);
            rx
        };

        // Subscribe to the broadcast channel which receives "topic joined" events.
        let mut joined_rx = self.broadcast_join.subscribe();

        // Flag if the topic has already been joined.
        let has_joined = self.joined_topics.contains(&topic);

        // Future which returns when the topic has been joined (or is already joined).
        let fut = async move {
            if has_joined {
                return;
            }
            loop {
                let joined_topic = joined_rx.recv().await.expect("channel is not dropped");
                if joined_topic == topic {
                    return;
                }
            }
        };

        Ok((rx, fut.boxed()))
    }

    async fn on_publish_event(
        &mut self,
        topic: TopicId,
        message: Message<T>,
    ) -> Result<MessageMeta> {
        // Create a p2panda operation which contains a topic message in it's body. The topic id
        // will be used in the log id.
        let operation = self.create_operation(topic, message).await?;

        // Send the operation on it's gossip topic.
        self.send_operation(topic, operation.clone()).await?;

        // Construct the message meta data which will be sent to any subscribing clients along
        // with the topic message itself.
        let message_context = MessageMeta {
            operation_timestamp: operation.header.timestamp,
            delivered_from: self.private_key.public_key(),
            received_at: time::SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("can calculate duration since UNIX_EPOCH")
                .as_secs(),
        };
        Ok(message_context)
    }

    async fn create_operation(
        &mut self,
        topic: TopicId,
        message: Message<T>,
    ) -> Result<GossipOperation<T>> {
        // The log id is {PUBLIC_KEY}/{TOPIC_ID} string.
        let log_id = format!("{}/{}", self.private_key.public_key().to_hex(), topic).into();

        // Create an operation for this event.
        let operation = create(&mut self.store, &self.private_key, &log_id, &message)?;
        Ok(GossipOperation {
            header: operation.header,
            message,
        })
    }

    async fn on_import_blob(&mut self, path: &Path) -> Result<Hash> {
        let mut stream = self.blobs.import_blob(path.to_path_buf()).await;

        let event = stream
            .next()
            .await
            .expect("no event arrived on blob import stream");

        let hash = match event {
            ImportBlobEvent::Abort(err) => Err(anyhow::anyhow!("failed importing blob: {err}")),
            ImportBlobEvent::Done(hash) => Ok(hash),
        }?;

        Ok(hash)
    }

    async fn on_import_url(&mut self, url: &String) -> Result<Hash> {
        let stream = reqwest::get(url)
            .await?
            .bytes_stream()
            .map(|result| result.map_err(|err| io::Error::new(io::ErrorKind::Other, err)));
        let mut stream = self.blobs.import_blob_from_stream(stream).await;

        let event = stream
            .next()
            .await
            .expect("no event arrived on blob import stream");

        let hash = match event {
            ImportBlobEvent::Abort(err) => Err(anyhow::anyhow!("failed importing blob: {err}")),
            ImportBlobEvent::Done(hash) => Ok(hash),
        }?;

        Ok(hash)
    }

    async fn on_gossip_event(&mut self, topic: TopicId, event: OutEvent) {
        match event {
            OutEvent::Ready => {
                self.joined_topics.insert(topic);
                self.broadcast_join
                    .send(topic)
                    .expect("broadcast_join channel not dropped");
            }
            OutEvent::Message {
                bytes,
                delivered_from,
            } => {
                // Ingest the operation, this performs all expected validation.
                let operation = match GossipOperation::from_bytes(&bytes) {
                    Ok(operation) => operation,
                    Err(err) => {
                        error!("Failed to decode gossip operation: {err}");
                        return;
                    }
                };

                match ingest(
                    &mut self.store,
                    operation.header.clone(),
                    Some(operation.body()),
                ) {
                    Ok(result) => result,
                    Err(err) => {
                        error!("Failed to ingest operation from {delivered_from}: {err}");
                        return;
                    }
                };

                debug!(
                    "Received operation: {} {} {} {}",
                    operation.header.public_key,
                    operation.header.seq_num,
                    operation.header.timestamp,
                    operation.header.hash(),
                );

                let message_context = MessageMeta {
                    operation_timestamp: operation.header.timestamp,
                    delivered_from,
                    received_at: time::SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .expect("can calculate duration since UNIX_EPOCH")
                        .as_secs(),
                };

                let tx = self
                    .topic_subscribers_tx
                    .get(&topic)
                    .expect("topic is known");

                let _ = tx.send((operation.message.clone(), message_context));
            }
        }
    }

    async fn on_download_blob(&mut self, hash: Hash) -> Result<()> {
        let mut stream = self.blobs.download_blob(hash).await;
        while let Some(event) = stream.next().await {
            match event {
                DownloadBlobEvent::Abort(err) => {
                    error!("failed downloading file: {err}");
                }
                DownloadBlobEvent::Done => (),
            }
        }
        Ok(())
    }

    async fn on_export_blob_filesystem(&mut self, hash: Hash, path: &PathBuf) -> Result<()> {
        self.blobs.export_blob(hash, path).await
    }

    async fn on_export_blob_minio(
        &mut self,
        hash: Hash,
        bucket_name: String,
        region: Region,
        credentials: Credentials,
    ) -> Result<()> {
        let entry = self.blobs.get(hash).await?.expect("entry exists");

        // Initiate the minio bucket
        let mut bucket =
            Bucket::new(&bucket_name, region.clone(), credentials.clone())?.with_path_style();

        if !bucket.exists().await? {
            bucket = Bucket::create_with_path_style(
                &bucket_name,
                region,
                credentials.clone(),
                BucketConfiguration::default(),
            )
            .await?
            .bucket;
        };

        // Start a multi-part upload
        let mut parts = Vec::new();
        let mpu = bucket
            .initiate_multipart_upload(&hash.to_string(), "application/octet-stream")
            .await?;

        // Access the actual blob data and iterate over it's bytes in chunks
        let mut reader = entry.data_reader().await?;
        let size = reader.size().await?;
        for (index, offset) in (0..size).step_by(5 * 1024 * 1024).enumerate() {
            // Upload this chunk to the minio bucket
            let bytes = reader.read_at(offset, 5 * 1024 * 1024).await?;
            let part = bucket
                .put_multipart_chunk(
                    bytes.to_vec(),
                    &hash.to_string(),
                    { index + 1 } as u32,
                    &mpu.upload_id,
                    "application/octet-stream",
                )
                .await?;
            parts.push(part);
        }

        let response = bucket
            .complete_multipart_upload(&hash.to_string(), &mpu.upload_id, parts)
            .await?;

        if response.status_code() != 200 {
            error!("{response}");
            return Err(anyhow::anyhow!(response));
        }
        Ok(())
    }
}
