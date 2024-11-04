use std::collections::HashMap;
use std::str::FromStr;

use anyhow::Result;
use iroh_blobs::IROH_BLOCK_SIZE;
use iroh_io::HttpAdapter;
use s3::serde_types::Part;
use s3::Bucket;

/// The minimum size of a part in a multipart upload session.
const PART_SIZE: usize = IROH_BLOCK_SIZE.bytes() * 1000;

#[derive(Debug)]
pub struct S3File {
    bucket: Bucket,
    path: String,
    size: u64,
    upload_id: Option<String>,
    parts: HashMap<u64, Vec<(u64, [u8; IROH_BLOCK_SIZE.bytes()])>>,
    last_chunk: Option<Vec<u8>>,
    uploaded_parts: Vec<Part>,
}

impl S3File {
    pub fn new(bucket: Bucket, path: String, size: u64) -> Self {
        Self {
            bucket,
            path,
            size,
            parts: Default::default(),
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
            // This is the last chunk it will be uploaded when `complete` is finally called.
            self.last_chunk = Some(bytes.to_vec());
            return Ok(());
        }

        let chunk: [u8; IROH_BLOCK_SIZE.bytes()] = bytes.try_into()?;

        // The part number for this group of chunks.
        let part_number = offset_to_part_number(offset);

        // Insert the chunk into the correct part.
        insert_to_part(&mut self.parts, part_number, (offset, chunk));

        // @TODO: as parts meet the minimum upload size then we should already upload them to
        // the minio store.
        //         if entry.len() == PART_SIZE {
        //             let upload_id = match &self.upload_id {
        //                 Some(id) => Ok::<_, anyhow::Error>(id.to_owned()),
        //                 None => {
        //                     let mpu = self.bucket.initiate_multipart_upload_blocking(
        //                         &self.path,
        //                         "application/octet-stream",
        //                     )?;
        //                     self.upload_id = Some(mpu.upload_id.clone());
        //                     Ok(mpu.upload_id.to_owned())
        //                 }
        //             }?;
        //
        //             let part = self.bucket.put_multipart_chunk_blocking(
        //                 bytes.to_vec(),
        //                 &self.path,
        //                 // @TODO: figure out the correct calculation here
        //                 //
        //                 // We want to write every offset chunk to the correct part number.
        //                 (offset / IROH_BLOCK_SIZE.bytes() as u64) as u32,
        //                 &upload_id,
        //                 "application/octet-stream",
        //             )?;
        //
        //             self.uploaded_parts.push(part);
        //         }
        Ok(())
    }

    /// Upload all remaining parts to the s3 bucket.
    pub fn complete(&mut self) -> Result<()> {
        let mut sorted_parts: Vec<(u64, Vec<(u64, [u8; IROH_BLOCK_SIZE.bytes()])>)> =
            self.parts.drain().collect();

        sorted_parts.sort();
        let len = sorted_parts.len();

        for (index, (part_number, chunks)) in sorted_parts.iter_mut().enumerate() {
            let mut bytes: Vec<u8> = Vec::new();
            concat_chunks(index, len, &mut bytes, chunks, &self.last_chunk);
            let (part, upload_id) = upload_to_s3(&self.bucket, &self.path, bytes, *part_number, self.upload_id.as_ref())?;
            self.upload_id = Some(upload_id);
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

fn offset_to_part_number(offset: u64) -> u64 {
    offset % IROH_BLOCK_SIZE.bytes() as u64
}

fn insert_to_part(
    parts: &mut HashMap<u64, Vec<(u64, [u8; IROH_BLOCK_SIZE.bytes()])>>,
    part_number: u64,
    item: (u64, [u8; IROH_BLOCK_SIZE.bytes()]),
) {
    parts
        .entry(part_number)
        .and_modify(|entry| entry.push(item))
        .or_insert(vec![item]);
}

fn concat_chunks(
    index: usize,
    len: usize,
    bytes: &mut Vec<u8>,
    chunks: &mut Vec<(u64, [u8; IROH_BLOCK_SIZE.bytes()])>,
    last_chunk: &Option<Vec<u8>>,
) {
    chunks.sort();
    for (_, chunk) in chunks {
        bytes.extend(&chunk[..]);
    }

    if index == len - 1 {
        if let Some(last_chunk) = last_chunk {
            bytes.extend(last_chunk);
        }
    }
}

fn upload_to_s3(
    bucket: &Bucket,
    path: &str,
    bytes: Vec<u8>,
    part_number: u64,
    upload_id: Option<&String>,
) -> Result<(Part, String)> {
    let upload_id = match upload_id {
        Some(id) => Ok::<_, anyhow::Error>(id.to_owned()),
        None => {
            let mpu =
                bucket.initiate_multipart_upload_blocking(&path, "application/octet-stream")?;
            Ok(mpu.upload_id.to_owned())
        }
    }?;

    let part = bucket.put_multipart_chunk_blocking(
        bytes,
        &path,
        part_number as u32,
        &upload_id,
        "application/octet-stream",
    )?;
    Ok((part, upload_id))
}
