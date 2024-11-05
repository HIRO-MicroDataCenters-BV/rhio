use anyhow::{anyhow, bail, Context, Result};
use futures_util::stream::SelectAll;
use p2panda_core::{Extension, Hash, Operation};
use p2panda_net::ToBytes;
use rhio_core::{decode_operation, encode_operation, DeprecatedSubject, RhioExtensions, TopicId};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tracing::{debug, error, info};

use crate::blobs::Blobs;
use crate::nats::{JetStreamEvent, Nats};
use crate::network::Panda;
use crate::node::NodeControl;

pub enum ToNodeActor {
    Subscribe {
        stream_name: String,
        filter_subject: Option<String>,
        topic: TopicId,
        reply: oneshot::Sender<Result<()>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

pub struct NodeActor {
    inbox: mpsc::Receiver<ToNodeActor>,
    node_control_rx: mpsc::Receiver<NodeControl>,
    nats_consumer_rx: SelectAll<BroadcastStream<JetStreamEvent>>,
    p2panda_topic_rx: SelectAll<BroadcastStream<Operation<RhioExtensions>>>,
    nats: Nats,
    panda: Panda,
    blobs: Blobs,
}

impl NodeActor {
    pub fn new(
        nats: Nats,
        panda: Panda,
        blobs: Blobs,
        inbox: mpsc::Receiver<ToNodeActor>,
        node_control_rx: mpsc::Receiver<NodeControl>,
    ) -> Self {
        Self {
            nats,
            node_control_rx,
            nats_consumer_rx: SelectAll::new(),
            p2panda_topic_rx: SelectAll::new(),
            panda,
            blobs,
            inbox,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        // Take oneshot sender from external API awaited by `shutdown` call and fire it as soon as
        // shutdown completed to signal
        let shutdown_completed_signal = self.run_inner().await;
        if let Err(err) = self.shutdown().await {
            error!(?err, "error during shutdown");
        }

        drop(self);

        match shutdown_completed_signal {
            Ok(reply_tx) => {
                reply_tx.send(()).ok();
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    async fn run_inner(&mut self) -> Result<oneshot::Sender<()>> {
        loop {
            tokio::select! {
                biased;
                Some(msg) = self.inbox.recv() => {
                    match msg {
                        ToNodeActor::Shutdown { reply } => {
                            break Ok(reply);
                        }
                        msg => {
                            if let Err(err) = self.on_actor_message(msg).await {
                                break Err(err);
                            }
                        }
                    }
                },
                Some(command) = self.node_control_rx.recv() => {
                    if let Err(err) = self.on_control_command(command).await {
                        break Err(err);
                    }
                },
                Some(Ok(event)) = self.nats_consumer_rx.next() => {
                    if let Err(err) = self.on_nats_event(event).await {
                        break Err(err);
                    }
                },
                Some(Ok(operation)) = self.p2panda_topic_rx.next() => {
                    if let Err(err) = self.on_operation(operation).await {
                        break Err(err);
                    }
                },
                else => {
                    // Error occurred outside of actor and our select! loop got disabled. We exit
                    // here with an error which will probably be overriden by the external error
                    // which caused the problem in first hand.
                    break Err(anyhow!("all select! branches are disabled"));
                }
            }
        }
    }

    async fn on_actor_message(&mut self, msg: ToNodeActor) -> Result<()> {
        match msg {
            ToNodeActor::Subscribe {
                stream_name,
                filter_subject,
                topic,
                reply,
            } => {
                let result = self.on_subscribe(stream_name, filter_subject, topic).await;
                reply.send(result).ok();
            }
            ToNodeActor::Shutdown { .. } => {
                unreachable!("handled in run_inner");
            }
        }

        Ok(())
    }

    /// Callback when the application decided to subscribe to a new NATS JetStream.
    ///
    /// This will create a NATS consumer which will start downloading all persisted, past data from
    /// that given stream before it'll observe new messages coming in.
    async fn on_subscribe(
        &mut self,
        stream_name: String,
        filter_subject: Option<String>,
        topic: TopicId,
    ) -> Result<()> {
        debug!("subscribe to nats stream {} ..", stream_name);
        let nats_rx = self
            .nats
            .subscribe(stream_name, filter_subject, topic)
            .await?;
        // Wrap broadcast receiver stream into tokio helper, to make it implement the `Stream`
        // trait which is required by `SelectAll`
        self.nats_consumer_rx.push(BroadcastStream::new(nats_rx));
        Ok(())
    }

    /// Handler for incoming events from the NATS stream consumer.
    async fn on_nats_event(&mut self, event: JetStreamEvent) -> Result<()> {
        match event {
            JetStreamEvent::InitCompleted { topic, .. } => {
                debug!("completed initialisation of stream");
                self.on_nats_init_complete(topic).await?;
            }
            JetStreamEvent::InitFailed {
                stream_name,
                reason,
                ..
            } => {
                bail!(
                    "initialisation of stream '{}' failed: {}",
                    stream_name,
                    reason
                );
            }
            JetStreamEvent::StreamFailed {
                stream_name,
                reason,
                ..
            } => {
                bail!("stream '{}' failed: {}", stream_name, reason);
            }
            JetStreamEvent::Message {
                is_init,
                topic,
                payload,
                ..
            } => {
                self.on_nats_message(is_init, topic, payload).await?;
            }
        }

        Ok(())
    }

    /// Callback when a NATS consumer has successfully streamed all persisted, past messages.
    ///
    /// From this point on we can join the p2panda gossip overlay for that given (filtered) subject
    /// in this stream.
    ///
    /// p2panda will now find other nodes interested in the same "topic" and sync up with them.
    async fn on_nats_init_complete(&mut self, topic: TopicId) -> Result<()> {
        debug!("join gossip on topic {topic} ..");
        let panda_rx = self.panda.subscribe(topic).await?;

        // Wrap broadcast receiver stream into tokio helper, to make it implement the `Stream`
        // trait which is required by `SelectAll`
        self.p2panda_topic_rx.push(BroadcastStream::new(panda_rx));
        Ok(())
    }

    /// Handler for incoming messages from the NATS JetStream consumer.
    ///
    /// Messages should contain p2panda operations which are decoded and validated here. When these
    /// steps have been successful, the operation is stored in the in-memory cache of rhio.
    ///
    /// From here these operations are replicated further to other nodes via the sync protocol and
    /// gossip broadcast.
    async fn on_nats_message(
        &mut self,
        is_init: bool,
        topic: TopicId,
        payload: Vec<u8>,
    ) -> Result<()> {
        let (header, body) =
            decode_operation(&payload).context("decode incoming operation via nats")?;
        let operation = self
            .panda
            .ingest(header.clone(), body.clone())
            .await
            .context("ingest incoming operation via nats")?;

        // Only forward the messages to external nodes _after_ initialisation (that is, downloading
        // all persisted, past messages first)
        if !is_init {
            // @TODO(adz): For now we're naively just broadcasting the message further to other
            // nodes, without checking if nodes came in late. This should be changed as soon as
            // `p2panda-sync` is in place.
            self.panda
                .broadcast(header, body, topic)
                .await
                .context("broadcast incoming operation from nats")?;
        }

        // Check if operation contains interesting information for rhio, for example blob
        // announcements
        self.process_operation(&operation)
            .await
            .context("process incoming operation from nats")?;

        Ok(())
    }

    /// Handler for incoming p2panda operations from the p2p network.
    ///
    /// Operations at this stage are already validated and stored in the in-memory cache. In this
    /// method they get forwarded to the NATS server for persistence and communication to other
    /// processes.
    async fn on_operation(&mut self, operation: Operation<RhioExtensions>) -> Result<()> {
        // Check if operation contains interesting information for rhio, for example blob
        // announcements
        self.process_operation(&operation).await?;

        // Forward operation to NATS server for persistence and communication to other processes
        // subscribed to the same subject
        let subject: DeprecatedSubject = operation
            .header
            .extract()
            .ok_or(anyhow!("missing 'subject' field in header"))?;
        let payload = encode_operation(operation.header, operation.body)?;
        self.nats.publish(true, subject, payload).await?;

        Ok(())
    }

    /// Looks at operation to identify if it causes any side-effects in rhio, for example
    /// announcing new blobs.
    ///
    /// Every operation which passes rhio from either the NATS JetStream or p2panda network gets
    /// processed at least once.
    async fn process_operation(&mut self, operation: &Operation<RhioExtensions>) -> Result<()> {
        let blob: Option<Hash> = operation.header.extract();
        if let Some(hash) = blob {
            match self.blobs.download_blob(hash).await {
                // @TODO(adz): Would be nice here to identify if we already had that blob at this
                // stage. This message will also pop up if no download happened (we had it
                // already).
                Ok(_) => debug!("syncing blob {} completed", hash),
                Err(err) => {
                    error!("failed syncing storing blob {}", hash);
                    return Err(err);
                }
            }
        }
        Ok(())
    }

    async fn on_control_command(&self, command: NodeControl) -> Result<()> {
        match command {
            NodeControl::ImportBlob {
                file_path,
                reply_subject,
            } => {
                debug!(
                    "received control command to import '{}'",
                    file_path.display()
                );
                let hash = self.blobs.import_file(file_path.clone()).await?;
                info!(
                    "file import '{}' completed, the resulting hash is: {}",
                    file_path.display(),
                    hash
                );

                // If the control command requested a response via NATS Core, we will provide it!
                if let Some(subject) = reply_subject {
                    // Since NATS Core messages are never acknowledged ("fire and forget"), we set
                    // the flag to "false" to never wait for an ACK
                    self.nats
                        .publish(false, subject.to_string(), hash.to_bytes())
                        .await?;
                }
            }
        }

        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        self.nats.shutdown().await?;
        self.panda.shutdown().await?;
        self.blobs.shutdown().await?;
        Ok(())
    }
}
