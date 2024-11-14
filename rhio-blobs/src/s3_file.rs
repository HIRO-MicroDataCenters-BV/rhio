use std::collections::HashMap;
use std::str::FromStr;

use anyhow::Result;
use iroh_blobs::IROH_BLOCK_SIZE;
use iroh_io::HttpAdapter;
use s3::serde_types::Part;
use s3::Bucket;
use tracing::{error, info};

/// The minimum size of a part in a multipart upload session.
const MIN_PART_SIZE: usize = IROH_BLOCK_SIZE.bytes() * 1000;

type PartNumber = usize;
type Offset = usize;

#[derive(Debug, Clone, thiserror::Error)]
pub enum MultiPartBufferError {
    #[error("attempted to extend an already drained part buffer")]
    PartBufferDrained,
}

pub enum MultiPartBufferResult {
    PartComplete(PartNumber, Offset, Vec<u8>),
    PartExtended(PartNumber, Offset),
}

/// Struct for managing a buffer which is split into many parts which have a configurable minimum
/// size. Each part has it's own internal buffer, when the minimum size is reached the buffer is
/// removed and returned.
#[derive(Debug)]
pub struct MultiPartBuffer {
    pub min_part_size: usize,
    pub processed_bytes: usize,
    pub parts: HashMap<usize, Option<Vec<u8>>>,
}

impl MultiPartBuffer {
    pub fn new(min_part_size: usize) -> Self {
        Self {
            min_part_size,
            processed_bytes: 0,
            parts: Default::default(),
        }
    }

    pub fn extend(
        &mut self,
        part_number: usize,
        bytes: Vec<u8>,
    ) -> Result<MultiPartBufferResult, MultiPartBufferError> {
        let part_buffer = self.parts.entry(part_number).or_insert(Some(Vec::new()));
        let (buffer_full, buffer_len) = match part_buffer {
            Some(buffer) => {
                self.processed_bytes += bytes.len();
                buffer.extend(bytes);
                let buffer_full = buffer.len() >= self.min_part_size;
                (buffer_full, buffer.len())
            }
            None => return Err(MultiPartBufferError::PartBufferDrained),
        };

        if buffer_full {
            let buffer = part_buffer.take().expect("buffer is Some");
            Ok(MultiPartBufferResult::PartComplete(
                part_number,
                buffer_len,
                buffer,
            ))
        } else {
            Ok(MultiPartBufferResult::PartExtended(part_number, buffer_len))
        }
    }

    pub fn drain(&mut self) -> Vec<(usize, Vec<u8>)> {
        self.parts
            .drain()
            .filter_map(|(part_number, buffer)| match buffer {
                Some(buffer) => Some((part_number, buffer)),
                None => None,
            })
            .collect()
    }
}

#[derive(Debug)]
pub struct S3File {
    bucket: Bucket,
    buffer: MultiPartBuffer,
    path: String,
    upload_id: Option<String>,
    uploaded_parts: Vec<Part>,
}

impl S3File {
    pub fn new(bucket: Bucket, path: String) -> Self {
        Self {
            bucket,
            path,
            buffer: MultiPartBuffer::new(MIN_PART_SIZE),
            upload_id: Default::default(),
            uploaded_parts: Default::default(),
        }
    }

    /// Write a byte buffer into the file at a particular offset.
    ///
    /// TODO: A current limitation of this implementation is that bytes are expected to be written
    /// _in order_, meaning no gaps are allowed in the buffer. It's possible to remove this
    /// restriction but it complicates the implementation and increases bug possibilities. This
    /// behavior would only occur when we download a blob from multiple peers in parallel, and as
    /// we can make sure this doesn't happen in the iroh blob download API I thought this was the
    /// pragmatic approach for now.  
    pub async fn write_all_at(&mut self, offset: usize, bytes: Vec<u8>) -> Result<()> {
        if self.buffer.processed_bytes != offset {
            return Err(anyhow::anyhow!("bytes mut be written to the file in order"));
        }
        let part_number = offset_to_part_number(MIN_PART_SIZE, offset);
        match self.buffer.extend(part_number, bytes) {
            Ok(result) => match result {
                MultiPartBufferResult::PartComplete(part_number, _, bytes) => {
                    self.upload_part(part_number, bytes).await?;
                }
                MultiPartBufferResult::PartExtended(_, _) => (),
            },
            // Panic as any error occurring here signals that we have a critical logic bug
            Err(err) => panic!("{err}"),
        }

        Ok(())
    }

    /// Upload a single part.
    async fn upload_part(&mut self, part_number: usize, bytes: Vec<u8>) -> Result<()> {
        let (part, upload_id) = upload_to_s3(
            &self.bucket,
            &self.path,
            bytes,
            part_number,
            self.upload_id.as_ref(),
        )
        .await?;

        if self.upload_id.is_none() {
            self.upload_id = Some(upload_id);
        };

        self.uploaded_parts.push(part);
        Ok(())
    }

    /// Complete the multipart upload.
    /// 
    /// This method _must_ be called after all bytes were uploaded via `write_all_at` in order to
    /// upload any remaining bytes and finalize the multipart upload.
    pub async fn complete(&mut self) -> Result<()> {
        let remaining_parts = self.buffer.drain();
        for (part_number, bytes) in remaining_parts {
            self.upload_part(part_number, bytes).await?;
        }

        let response = self
            .bucket
            .complete_multipart_upload(
                &self.path,
                &self.upload_id.take().expect("download id set"),
                self.uploaded_parts.clone(),
            )
            .await?;

        if response.status_code() != 200 {
            error!("uploading blob to minio bucket failed with: {response}");
            return Err(anyhow::anyhow!(response));
        };

        info!(
            "Upload complete: {:?} uploaded in {} parts ({} bytes)",
            self.path,
            self.uploaded_parts.len(),
            self.buffer.processed_bytes,
        );

        Ok(())
    }

    pub fn reader(&self) -> HttpAdapter {
        let url = self.bucket.url();
        HttpAdapter::new(url::Url::from_str(&format!("{url}/{}", &self.path)).unwrap())
    }
}

fn offset_to_part_number(min_part_size: usize, offset: usize) -> usize {
    (offset / min_part_size) + 1
}

async fn upload_to_s3(
    bucket: &Bucket,
    path: &String,
    bytes: Vec<u8>,
    part_number: usize,
    upload_id: Option<&String>,
) -> Result<(Part, String)> {
    let upload_id = match upload_id {
        Some(id) => Ok::<_, anyhow::Error>(id.to_owned()),
        None => {
            let mpu = bucket
                .initiate_multipart_upload(&path, "application/octet-stream")
                .await?;
            Ok(mpu.upload_id.to_owned())
        }
    }?;

    let part = bucket
        .put_multipart_chunk(
            bytes,
            &path,
            part_number as u32,
            &upload_id,
            "application/octet-stream",
        )
        .await?;
    Ok((part, upload_id))
}

#[cfg(test)]
mod tests {
    use iroh_blobs::IROH_BLOCK_SIZE;

    use super::{offset_to_part_number, MIN_PART_SIZE};

    #[test]
    fn calculate_part_number() {
        let part_number = offset_to_part_number(MIN_PART_SIZE, 0);
        assert_eq!(part_number, 0);

        let part_number = offset_to_part_number(MIN_PART_SIZE, IROH_BLOCK_SIZE.bytes());
        assert_eq!(part_number, 0);

        let part_number = offset_to_part_number(MIN_PART_SIZE, IROH_BLOCK_SIZE.bytes() * 1000 - 1);
        assert_eq!(part_number, 0);

        let part_number = offset_to_part_number(MIN_PART_SIZE, IROH_BLOCK_SIZE.bytes() * 1000);
        assert_eq!(part_number, 1);

        let part_number = offset_to_part_number(MIN_PART_SIZE, IROH_BLOCK_SIZE.bytes() * 1000 + 1);
        assert_eq!(part_number, 1);

        let part_number = offset_to_part_number(MIN_PART_SIZE, IROH_BLOCK_SIZE.bytes() * 1000 * 6);
        assert_eq!(part_number, 6);
    }
}
