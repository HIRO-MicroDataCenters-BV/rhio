use std::collections::HashMap;
use std::io::Write;
use std::str::FromStr;

use anyhow::Result;
use bytes::BufMut;
use iroh_blobs::IROH_BLOCK_SIZE;
use iroh_io::HttpAdapter;
use s3::serde_types::Part;
use s3::Bucket;

const PART_SIZE: usize = IROH_BLOCK_SIZE.bytes() * 1000;

#[derive(Debug)]
pub struct S3File {
    bucket: Bucket,
    path: String,
    size: u64,
    upload_id: Option<String>,
    chunks: HashMap<u64, Vec<(u64, [u8; IROH_BLOCK_SIZE.bytes()])>>,
    last_chunk: Option<Vec<u8>>,
    uploaded_parts: Vec<Part>,
}

impl S3File {
    pub fn new(bucket: Bucket, path: String, size: u64) -> Self {
        Self {
            bucket,
            path,
            size,
            chunks: Default::default(),
            last_chunk: Default::default(),
            upload_id: Default::default(),
            uploaded_parts: Default::default(),
        }
    }
    pub fn write_all_at(&mut self, offset: u64, bytes: &[u8]) -> Result<()> {
        // We need to batch all chunks into min 5MB parts which can then be uploaded to the
        // bucket. This is the minimum allowed part size. The exception being the final part which
        // has no minimum size.

        if bytes.len() < IROH_BLOCK_SIZE.bytes() {
            // @TODO: this is the last chunk we want to upload it on it's own
            self.last_chunk = Some(bytes.to_vec())
        }
        let chunk: [u8; IROH_BLOCK_SIZE.bytes()] = bytes.try_into()?;
        let chunk_batch_id = offset % IROH_BLOCK_SIZE.bytes() as u64;
        let entry = self
            .chunks
            .entry(chunk_batch_id)
            .and_modify(|entry| entry.push((offset, chunk)))
            .or_insert(vec![(offset, chunk)]);

        if entry.len() == PART_SIZE {
            let upload_id = match &self.upload_id {
                Some(id) => Ok::<_, anyhow::Error>(id.to_owned()),
                None => {
                    let mpu = self.bucket.initiate_multipart_upload_blocking(
                        &self.path,
                        "application/octet-stream",
                    )?;
                    self.upload_id = Some(mpu.upload_id.clone());
                    Ok(mpu.upload_id.to_owned())
                }
            }?;

            let part = self.bucket.put_multipart_chunk_blocking(
                bytes.to_vec(),
                &self.path,
                // @TODO: figure out the correct calculation here
                //
                // We want to write every offset chunk to the correct part number.
                (offset / IROH_BLOCK_SIZE.bytes() as u64) as u32,
                &upload_id,
                "application/octet-stream",
            )?;

            self.uploaded_parts.push(part);
        }
        Ok(())
    }

    pub fn reader(&self) -> Result<HttpAdapter> {
        let url = self
            .bucket
            .presign_get_blocking(&self.path, 60 * 60 * 24, None)?;
        Ok(HttpAdapter::new(url::Url::from_str(&url)?))
    }
}
