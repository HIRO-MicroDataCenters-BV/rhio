use std::path::PathBuf;

use anyhow::{anyhow, Result};
use iroh_blobs::store::bao_tree::io::fsm::AsyncSliceReader;
use iroh_blobs::store::{MapEntry, Store};
use p2panda_blobs::{Blobs as BlobsHandler, DownloadBlobEvent, ImportBlobEvent};
use p2panda_core::Hash;
use s3::creds::Credentials;
use s3::{Bucket, BucketConfiguration, Region};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tracing::error;

pub enum ToBlobsActor {
    ImportFile {
        file_path: PathBuf,
        reply: oneshot::Sender<Result<Hash>>,
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
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

pub struct BlobsActor<S>
where
    S: Store,
{
    inbox: mpsc::Receiver<ToBlobsActor>,
    blobs: BlobsHandler<S>,
}

impl<S> BlobsActor<S>
where
    S: Store,
{
    pub fn new(blobs: BlobsHandler<S>, inbox: mpsc::Receiver<ToBlobsActor>) -> Self {
        Self { inbox, blobs }
    }

    pub async fn run(mut self) -> Result<()> {
        // Take oneshot sender from external API awaited by `shutdown` call and fire it as soon as
        // shutdown completed
        let shutdown_completed_signal = self.run_inner().await;
        if let Err(err) = self.shutdown().await {
            error!(?err, "error during shutdown");
        }

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
                Some(msg) = self.inbox.recv() => {
                    match msg {
                        ToBlobsActor::Shutdown { reply } => {
                            break Ok(reply);
                        }
                        msg => {
                            if let Err(err) = self.on_actor_message(msg).await {
                                break Err(err);
                            }
                        }
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

    async fn on_actor_message(&mut self, msg: ToBlobsActor) -> Result<()> {
        match msg {
            ToBlobsActor::ImportFile { file_path, reply } => {
                let result = self.on_import_file(file_path).await;
                reply.send(result).ok();
            }
            ToBlobsActor::DownloadBlob { hash, reply } => {
                let result = self.on_download_blob(hash).await;
                reply.send(result).ok();
            }
            ToBlobsActor::ExportBlobMinio {
                hash,
                bucket_name,
                region,
                credentials,
                reply,
            } => {
                let result = self
                    .on_export_blob_minio(hash, bucket_name.clone(), region, credentials)
                    .await;
                reply.send(result).ok();
            }
            ToBlobsActor::Shutdown { .. } => {
                unreachable!("handled in run_inner");
            }
        }

        Ok(())
    }

    async fn on_import_file(&mut self, path: PathBuf) -> Result<Hash> {
        // @TODO: We're currently using the filesystem blob store to calculate the bao-tree hashes
        // for the file. This is the only way to retrieve the blob hash right now. In the future we
        // want to do all of this inside of MinIO and skip loading the file onto the file-system
        // first.
        let mut stream = Box::pin(self.blobs.import_blob(path.to_path_buf()).await);

        // @TODO(adz): Yes, we know this never loops as all enum cases are currently terminating,
        // but as soon as we're adding more this code becomes crucial
        #[allow(clippy::never_loop)]
        let hash = loop {
            match stream.next().await {
                Some(ImportBlobEvent::Done(hash)) => {
                    break Ok(hash);
                }
                Some(ImportBlobEvent::Abort(err)) => {
                    break Err(anyhow!("failed importing blob: {err}"));
                }
                None => {
                    break Err(anyhow!("failed importing blob"));
                }
            }
        }?;

        Ok(hash)
    }

    async fn on_download_blob(&mut self, hash: Hash) -> Result<()> {
        let mut stream = Box::pin(self.blobs.download_blob(hash).await);
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

    async fn on_export_blob_minio(
        &mut self,
        hash: Hash,
        bucket_name: String,
        region: Region,
        credentials: Credentials,
    ) -> Result<()> {
        let entry = self
            .blobs
            .get(hash)
            .await?
            .ok_or(anyhow!("requested blob hash was not found in blob store"))?;

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
            error!("uploading blob to minio bucket failed with: {response}");
            return Err(anyhow!(response));
        }

        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}
