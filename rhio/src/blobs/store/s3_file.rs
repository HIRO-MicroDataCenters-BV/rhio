use std::str::FromStr;

use anyhow::Result;
use iroh_io::HttpAdapter;
use s3::serde_types::Part;
use s3::Bucket;

#[derive(Debug)]
pub struct S3File {
    bucket: Bucket,
    path: String,
    upload_id: Option<String>,
    uploaded_parts: Vec<Part>,
}

impl S3File {
    pub fn new(bucket: Bucket, path: String) -> Self {
        Self {
            bucket,
            path,
            upload_id: Default::default(),
            uploaded_parts: Default::default(),
        }
    }
    pub fn write_all_at(&mut self, offset: u64, chunk_size: usize, bytes: &[u8]) -> Result<()> {
        let upload_id = match &self.upload_id {
            Some(id) => Ok::<_, anyhow::Error>(id.to_owned()),
            None => {
                let mpu = self
                    .bucket
                    .initiate_multipart_upload_blocking(&self.path, "application/octet-stream")?;
                self.upload_id = Some(mpu.upload_id.clone());
                Ok(mpu.upload_id.to_owned())
            }
        }?;

        let part = self.bucket.put_multipart_chunk_blocking(
            bytes.to_vec(),
            &self.path,
            (offset / chunk_size as u64) as u32,
            &upload_id,
            "application/octet-stream",
        )?;

        self.uploaded_parts.push(part);
        Ok(())
    }

    pub fn reader(&self) -> Result<HttpAdapter> {
        let url = self
            .bucket
            .presign_get_blocking(&self.path, 60 * 60 * 24, None)?;
        Ok(HttpAdapter::new(url::Url::from_str(&url)?))
    }
}
