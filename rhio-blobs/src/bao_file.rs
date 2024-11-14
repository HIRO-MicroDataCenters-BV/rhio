use anyhow::Result;
use bytes::{Bytes, BytesMut};
use iroh_blobs::store::bao_tree::io::fsm::{BaoContentItem, CreateOutboard};
use iroh_blobs::store::bao_tree::io::outboard::PreOrderOutboard;
use iroh_blobs::store::bao_tree::BaoTree;
use iroh_blobs::Hash;

use iroh_blobs::IROH_BLOCK_SIZE;
use iroh_io::AsyncSliceReader;
use s3::Bucket;

use crate::paths::Paths;

use super::s3_file::S3File;

#[derive(Debug)]
pub struct BaoFileHandle {
    pub data: S3File,
    pub data_size: u64,
    pub outboard: S3File,
}

impl BaoFileHandle {
    /// Construct a BAO file handle.
    ///
    /// This method returns a handle onto an incomplete BAO file using the expected paths. It
    /// doesn't create any files yet or transfer any data.
    pub fn new(bucket: Bucket, paths: Paths, data_size: u64) -> Self {
        Self {
            data: S3File::new(bucket.clone(), paths.temp_data()),
            data_size,
            outboard: S3File::new(bucket, paths.outboard()),
        }
    }
}

impl BaoFileHandle {
    /// Process some existing data, create a BAO file and return the file handle and newly
    /// calculated hash.
    ///
    /// This method is for taking an existing blob and generating it's accompanying outboard file.
    /// It is useful when importing blobs from an s3 bucket directly into the store.
    pub async fn create_complete(
        bucket: Bucket,
        paths: Paths,
        size: u64,
    ) -> anyhow::Result<(Self, Hash)> {
        let data_file = S3File::new(bucket.clone(), paths.data());

        // Create an outboard for the file, using the current size
        let (hash, outboard) = {
            let outboard =
                PreOrderOutboard::<BytesMut>::create(&mut data_file.reader(), IROH_BLOCK_SIZE)
                    .await?;
            Ok::<_, anyhow::Error>((outboard.root, outboard.data))
        }?;

        let mut outboard_file = S3File::new(bucket, paths.outboard());
        outboard_file.write_all_at(0, outboard.to_vec()).await?;
        outboard_file.complete().await?;

        let res = Self {
            data: data_file,
            data_size: size,
            outboard: outboard_file,
        };

        Ok((res, hash.into()))
    }

    pub(super) async fn read_data_at(&self, offset: u64, len: usize) -> std::io::Result<Bytes> {
        copy_limited_slice_s3(&self.data, offset, len).await
    }

    pub(super) fn data_len(&self) -> u64 {
        self.data_size
    }

    pub(super) async fn read_outboard_at(&self, offset: u64, len: usize) -> std::io::Result<Bytes> {
        copy_limited_slice_s3(&self.outboard, offset, len).await
    }

    pub(super) async fn outboard_len(&self) -> Result<u64> {
        let size = self.outboard.reader().size().await?;
        Ok(size)
    }

    pub(super) async fn write_batch(
        &mut self,
        size: u64,
        batch: &[BaoContentItem],
    ) -> anyhow::Result<()> {
        let tree = BaoTree::new(size, IROH_BLOCK_SIZE);
        for item in batch {
            match item {
                BaoContentItem::Parent(parent) => {
                    if let Some(offset) = tree.pre_order_offset(parent.node) {
                        let o0 = offset
                            .checked_mul(64)
                            .expect("u64 overflow multiplying to hash pair offset");
                        let o1 = o0.checked_add(32).expect("u64 overflow");
                        let outboard = &mut self.outboard;
                        outboard
                            .write_all_at(o0 as usize, parent.pair.0.as_bytes().to_vec())
                            .await?;
                        outboard
                            .write_all_at(o1 as usize, parent.pair.1.as_bytes().to_vec())
                            .await?;
                    }
                }
                BaoContentItem::Leaf(leaf) => {
                    self.data
                        .write_all_at(leaf.offset as usize, leaf.data.to_vec())
                        .await?;
                }
            }
        }
        Ok(())
    }

    pub async fn complete(&mut self) -> Result<()> {
        self.data.complete().await?;
        self.outboard.complete().await
    }
}

/// copy a limited slice from a slice as a `Bytes`.
pub(crate) async fn copy_limited_slice_s3(
    file: &S3File,
    offset: u64,
    len: usize,
) -> std::io::Result<Bytes> {
    let bytes = file.reader().read_at(offset, len).await?;
    Ok(bytes)
}
