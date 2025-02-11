use std::collections::HashMap;

use crate::{Node, StreamName};

use anyhow::{bail, Context as AnyhowContext, Result};
use p2panda_core::PublicKey;
use rhio_http_api::api::{HTTP_HEALTH_ROUTE, HTTP_METRICS_ROUTE};
use tokio::runtime::Runtime;
use tokio::signal;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    FilesSubscription, FilteredMessageStream, MessagesSubscription, Publication, Subscription,
};
use rhio_config::configuration::{Config, LocalNatsSubject, RemoteNatsSubject, RemoteS3Bucket};
use rhio_core::Subject;

pub struct Context {
    node: Node,
    config: Config,
    public_key: PublicKey,
    http_handle: JoinHandle<Result<()>>,
    http_runtime: Runtime,
    cancellation_token: CancellationToken,
    rhio_runtime: Runtime,
}

impl Context {
    pub fn new(
        node: Node,
        config: Config,
        public_key: PublicKey,
        http_handle: JoinHandle<Result<()>>,
        http_runtime: Runtime,
        cancellation_token: CancellationToken,
        rhio_runtime: Runtime,
    ) -> Self {
        Context {
            node,
            config,
            public_key,
            http_handle,
            http_runtime,
            cancellation_token,
            rhio_runtime,
        }
    }

    pub fn configure(&self) -> Result<()> {
        self.rhio_runtime.block_on(async {
            self.configure_inner()
                .await
                .context("failed to configure Node")
        })
    }

    async fn configure_inner(&self) -> Result<()> {
        if self.config.s3.is_some() {
            if let Some(publish) = &self.config.publish {
                self.publish_s3(publish.s3_buckets.clone()).await?;
            }
            if let Some(subscribe) = &self.config.subscribe {
                self.subscribe_s3(subscribe.s3_buckets.clone()).await?;
            }
        }
        if let Some(publish) = &self.config.publish {
            self.publish_nats(publish.nats_subjects.clone()).await?;
        }
        if let Some(subscribe) = &self.config.subscribe {
            self.subscribe_nats(subscribe.nats_subjects.clone()).await?;
        }
        Ok(())
    }

    async fn publish_s3(&self, s3_buckets: Vec<String>) -> Result<()> {
        for bucket_name in s3_buckets {
            // Assign our own public key to S3 bucket info.
            self.node
                .publish(Publication::Files {
                    bucket_name,
                    public_key: self.public_key,
                })
                .await?;
        }
        Ok(())
    }

    async fn subscribe_s3(&self, s3_buckets: Vec<RemoteS3Bucket>) -> Result<()> {
        for RemoteS3Bucket {
            local_bucket_name,
            remote_bucket_name,
            public_key: remote_public_key,
        } in s3_buckets
        {
            self.node
                .subscribe(Subscription::Files(FilesSubscription {
                    remote_bucket_name,
                    local_bucket_name,
                    public_key: remote_public_key,
                }))
                .await?;
        }

        Ok(())
    }

    async fn subscribe_nats(&self, subjects: Vec<RemoteNatsSubject>) -> Result<()> {
        // Multiple subjects can be used on top of a stream and we want to group them over one
        // public key and stream name pair.
        let mut stream_public_key_map = HashMap::<(StreamName, PublicKey), Vec<Subject>>::new();
        for RemoteNatsSubject {
            stream_name,
            subject,
            public_key: remote_public_key,
        } in subjects
        {
            stream_public_key_map
                .entry((stream_name, remote_public_key))
                .and_modify(|subjects| {
                    subjects.push(subject.clone());
                })
                .or_insert(vec![subject]);
        }

        // Finally we want to group these subscriptions by public key.
        let mut subscription_map = HashMap::<PublicKey, Vec<FilteredMessageStream>>::new();
        for ((stream_name, remote_public_key), subjects) in stream_public_key_map.into_iter() {
            let filtered_stream = FilteredMessageStream {
                subjects,
                stream_name,
            };
            subscription_map
                .entry(remote_public_key)
                .and_modify(|filtered_streams| filtered_streams.push(filtered_stream.clone()))
                .or_insert(vec![filtered_stream]);
        }

        for (remote_public_key, filtered_streams) in subscription_map.into_iter() {
            let subscription = Subscription::Messages(MessagesSubscription {
                filtered_streams,
                public_key: remote_public_key,
            });
            self.node.subscribe(subscription).await?;
        }
        Ok(())
    }

    async fn publish_nats(&self, nats_subjects: Vec<LocalNatsSubject>) -> Result<()> {
        // Multiple subjects can be used on top of a stream and we want to group them over one
        // public key and stream name pair. This leaves us with the following structure:
        //
        // Streams      Public Key       Subjects        Topic Id (for Gossip)
        // =======      ==========       ========        =====================
        // 1            I                A               a
        // 2            I                B               a
        // 3            II               A               b
        // 1            II               B               b
        // 1            II               C               b
        let mut stream_public_key_map = HashMap::<StreamName, Vec<Subject>>::new();
        for LocalNatsSubject {
            stream_name,
            subject,
        } in nats_subjects
        {
            stream_public_key_map
                .entry(stream_name)
                .and_modify(|subjects| {
                    subjects.push(subject.clone());
                })
                .or_insert(vec![subject.clone()]);
        }

        for (stream_name, subjects) in stream_public_key_map.into_iter() {
            self.node
                .publish(Publication::Messages {
                    filtered_stream: FilteredMessageStream {
                        subjects,
                        stream_name,
                    },
                    // Assign our own public key to NATS subject info.
                    public_key: self.public_key,
                })
                .await?;
        }
        Ok(())
    }

    pub fn log_configuration(&self) {
        let addresses: Vec<String> = self
            .node
            .direct_addresses()
            .iter()
            .map(|addr| addr.to_string())
            .collect();
        info!("‣ network id:");
        info!("  - {}", self.config.node.network_id);
        info!("‣ node public key:");
        info!("  - {}", self.public_key);
        info!("‣ node addresses:");
        for address in addresses {
            info!("  - {}", address);
        }
        info!("‣ health endpoint:");
        info!(
            "  - 0.0.0.0:{}{}",
            self.config.node.http_bind_port, HTTP_HEALTH_ROUTE
        );
        info!("‣ metrics endpoint:");
        info!(
            "  - 0.0.0.0:{}{}",
            self.config.node.http_bind_port, HTTP_METRICS_ROUTE
        );
    }

    pub fn wait_for_termination(&self) -> Result<()> {
        let cloned_token = self.cancellation_token.clone();
        self.http_runtime.block_on(async move {
            tokio::select! {
                _ = cloned_token.cancelled() => bail!("HTTP server was cancelled"),
                _ = signal::ctrl_c() => {},
            };
            Ok(())
        })
    }

    /// Shuts down the context, including the node and associated runtimes.
    ///
    /// This method performs the following steps:
    /// 1. Shuts down the node asynchronously.
    /// 2. Aborts the HTTP handle.
    /// 3. Shuts down the HTTP runtime in the background.
    /// 4. Shuts down the Rhio runtime in the background.
    ///
    /// Returns a `Result` indicating the success or failure of the shutdown process.
    pub fn shutdown(self) -> Result<()> {
        self.rhio_runtime.block_on(async move {
            self.node
                .shutdown()
                .await
                .context("Failure during node shutdown")
        })?;
        self.http_handle.abort();
        self.http_runtime.shutdown_background();
        self.rhio_runtime.shutdown_background();
        Ok(())
    }
}
