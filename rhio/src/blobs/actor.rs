use std::io;
use std::path::{Path, PathBuf};

use anyhow::Result;
use iroh_blobs::store::bao_tree::io::fsm::AsyncSliceReader;
use iroh_blobs::store::{MapEntry, Store};
use p2panda_blobs::{Blobs as BlobsHandler, DownloadBlobEvent, ImportBlobEvent};
use p2panda_core::Hash;
use s3::creds::Credentials;
use s3::{Bucket, BucketConfiguration, Region};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tracing::{error, info};

pub enum ToBlobsActor {
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
        // Take oneshot sender from outside API awaited by `shutdown` call and fire it as soon as
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
            }
        }
    }

    async fn on_actor_message(&mut self, msg: ToBlobsActor) -> Result<()> {
        match msg {
            ToBlobsActor::ImportUrl { url, reply } => {
                let result = self.on_import_url(&url).await;
                if let Ok(hash) = result {
                    info!("imported blob: {hash} {url}");
                }
                reply.send(result).ok();
            }
            ToBlobsActor::ImportFile { path, reply } => {
                let result = self.on_import_blob(&path).await;
                if let Ok(hash) = result {
                    info!("imported blob: {hash} {path:?}");
                }
                reply.send(result).ok();
            }
            ToBlobsActor::ExportBlobFilesystem { path, reply, hash } => {
                let result = self.on_export_blob_filesystem(hash, &path).await;
                if result.is_ok() {
                    info!("exported blob to filesystem: {hash} {path:?}");
                }
                reply.send(result).ok();
            }
            ToBlobsActor::DownloadBlob { hash, reply } => {
                let result = self.on_download_blob(hash).await;
                if result.is_ok() {
                    info!("downloaded blob {hash}");
                }
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
                if result.is_ok() {
                    info!("exported blob to minio: {hash} {bucket_name}");
                }
                reply.send(result).ok();
            }
            ToBlobsActor::Shutdown { .. } => {
                unreachable!("handled in run_inner");
            }
        }

        Ok(())
    }

    async fn on_import_blob(&mut self, path: &Path) -> Result<Hash> {
        let mut stream = Box::pin(self.blobs.import_blob(path.to_path_buf()).await);

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
        let mut stream = Box::pin(self.blobs.import_blob_from_stream(stream).await);

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

    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}
