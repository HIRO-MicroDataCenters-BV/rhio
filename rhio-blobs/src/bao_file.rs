use anyhow::Result;
use bytes::{Bytes, BytesMut};
use iroh_blobs::store::bao_tree::io::fsm::{BaoContentItem, CreateOutboard};
use iroh_blobs::store::bao_tree::io::outboard::PreOrderOutboard;
use iroh_blobs::store::bao_tree::io::sync::WriteAt;
use iroh_blobs::store::bao_tree::BaoTree;
use iroh_blobs::util::SparseMemFile;
use iroh_blobs::{Hash, IROH_BLOCK_SIZE};
use iroh_io::AsyncSliceReader;
use s3::Bucket;
use serde::{Deserialize, Serialize};

use crate::s3_file::S3File;
use crate::utils::{put_meta, put_outboard};
use crate::Paths;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BaoMeta {
    pub hash: Hash,
    pub size: u64,
    pub complete: bool,
    #[serde(rename = "key")]
    pub path: String,
}

impl BaoMeta {
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("json encoding of meta data")
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

#[derive(Debug)]
pub struct BaoFileHandle {
    pub paths: Paths,
    pub bucket: Bucket,
    pub data: S3File,
    pub outboard: SparseMemFile,
    pub data_size: u64,
}

impl BaoFileHandle {
    /// Construct a BAO file handle.
    ///
    /// This method returns a handle onto an incomplete BAO file using the expected paths. It
    /// doesn't create any files yet or transfer any data. The provided in-memory outboard can be
    /// in an incomplete or complete state.
    pub fn new(bucket: Bucket, paths: Paths, outboard: SparseMemFile, data_size: u64) -> Self {
        Self {
            data: S3File::new(bucket.clone(), paths.data(), data_size),
            outboard,
            data_size,
            bucket,
            paths,
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
        path: String,
        size: u64,
    ) -> anyhow::Result<(Self, BaoMeta)> {
        let paths = Paths::new(path.clone());
        let data_file = S3File::new(bucket.clone(), paths.data(), size);

        let (hash, outboard) = {
            let outboard =
                PreOrderOutboard::<BytesMut>::create(&mut data_file.reader(), IROH_BLOCK_SIZE)
                    .await?;
            Ok::<_, anyhow::Error>((outboard.root, outboard.data))
        }?;

        let mut mem_file = SparseMemFile::new();
        mem_file.write_all_at(0, outboard.as_ref())?;

        put_outboard(&bucket, &paths, &outboard).await?;

        let meta = BaoMeta {
            hash: hash.into(),
            size,
            complete: true,
            path,
        };

        put_meta(&bucket, &paths, &meta).await?;

        let res = Self {
            data: data_file,
            data_size: size,
            outboard: mem_file,
            bucket,
            paths,
        };

        Ok((res, meta))
    }

    pub(super) async fn read_data_at(&self, offset: u64, len: usize) -> std::io::Result<Bytes> {
        self.data.reader().read_at(offset, len).await
    }

    pub(super) fn data_len(&self) -> u64 {
        self.data_size
    }

    pub(super) async fn read_outboard_at(&self, offset: u64, len: usize) -> std::io::Result<Bytes> {
        Ok(copy_limited_slice(&self.outboard, offset, len))
    }

    pub(super) async fn outboard_len(&self) -> Result<u64> {
        Ok(self.outboard.len() as u64)
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
                        outboard.write_all_at(o0, parent.pair.0.as_bytes().as_ref())?;
                        outboard.write_all_at(o1, parent.pair.1.as_bytes().as_ref())?;
                    }
                }
                BaoContentItem::Leaf(leaf) => {
                    self.data
                        .write_all_at(leaf.offset as usize, leaf.data.as_ref())
                        .await?;
                }
            }
        }
        Ok(())
    }

    pub async fn complete(&mut self) -> Result<()> {
        put_outboard(&self.bucket, &self.paths, self.outboard.as_ref()).await?;
        self.data.complete().await
    }
}

/// copy a limited slice from a slice as a `Bytes`.
pub(crate) fn copy_limited_slice(bytes: &[u8], offset: u64, len: usize) -> Bytes {
    bytes[limited_range(offset, len, bytes.len())]
        .to_vec()
        .into()
}

pub(crate) fn limited_range(offset: u64, len: usize, buf_len: usize) -> std::ops::Range<usize> {
    if offset < buf_len as u64 {
        let start = offset as usize;
        let end = start.saturating_add(len).min(buf_len);
        start..end
    } else {
        0..0
    }
}

#[cfg(test)]
mod tests {
    use iroh_blobs::Hash;

    use super::BaoMeta;

    #[test]
    fn meta_encode_decode() {
        let meta = BaoMeta {
            hash: Hash::from_bytes([0; 32]),
            size: 1048,
            complete: false,
            path: String::from("path/to/file.txt"),
        };

        let bytes = meta.to_bytes();
        let meta_again = BaoMeta::from_bytes(&bytes[..]).unwrap();

        assert_eq!(meta, meta_again)
    }
}
