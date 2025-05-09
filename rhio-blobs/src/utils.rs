use anyhow::{Result, anyhow};
use iroh_blobs::store::bao_tree::io::sync::WriteAt;
use iroh_blobs::util::SparseMemFile;
use s3::Bucket;
use tracing::trace;

use crate::Paths;
use crate::bao_file::{BaoMeta, META_CONTENT_TYPE};

/// Creates a meta file in S3 bucket.
pub async fn put_meta(bucket: &Bucket, paths: &Paths, meta: &BaoMeta) -> Result<()> {
    let response = bucket
        .put_object_with_content_type(paths.meta(), &meta.to_bytes(), META_CONTENT_TYPE)
        .await?;
    if response.status_code() != 200 {
        return Err(anyhow!("failed to create blob meta file in s3 bucket"));
    }
    trace!(
        key = %paths.meta(),
        complete = %meta.complete,
        bucket_name = bucket.name(),
        "created meta file in S3 bucket",
    );
    Ok(())
}

/// Creates an outboard file in S3 bucket.
pub async fn put_outboard(bucket: &Bucket, paths: &Paths, outboard: &[u8]) -> Result<()> {
    let response = bucket.put_object(paths.outboard(), outboard).await?;
    if response.status_code() != 200 {
        return Err(anyhow!("failed to create blob outboard file in s3 bucket"));
    }
    trace!(
        key = %paths.outboard(),
        bytes = %outboard.len(),
        bucket_name = bucket.name(),
        "created outboard file in S3 bucket",
    );
    Ok(())
}

/// Loads a meta file from S3 bucket.
pub async fn get_meta(bucket: &Bucket, paths: &Paths) -> Result<BaoMeta> {
    let response = bucket.get_object(paths.meta()).await?;
    if response.status_code() != 200 {
        return Err(anyhow!("Failed to get blob meta file to s3 bucket"));
    }
    let meta = BaoMeta::from_bytes(response.as_slice())?;
    Ok(meta)
}

/// Loads an outboard file from S3 bucket.
pub async fn get_outboard(bucket: &Bucket, paths: &Paths) -> Result<SparseMemFile> {
    let response = bucket.get_object(paths.outboard()).await?;
    if response.status_code() != 200 {
        return Err(anyhow!("Failed to get blob outboard file to s3 bucket"));
    }
    let mut outboard = SparseMemFile::new();
    outboard.write_all_at(0, response.as_slice())?;
    Ok(outboard)
}

/// Remove meta file from S3 bucket.
pub async fn remove_meta(bucket: &Bucket, paths: &Paths) -> Result<()> {
    let response = bucket.delete_object(paths.meta()).await?;
    if response.status_code() != 200 {
        return Err(anyhow!("failed to remove blob meta file from s3 bucket"));
    }
    Ok(())
}

/// Remove outboard file from S3 bucket.
pub async fn remove_outboard(bucket: &Bucket, paths: &Paths) -> Result<()> {
    let response = bucket.delete_object(paths.outboard()).await?;
    if response.status_code() != 200 {
        return Err(anyhow!(
            "failed to remove blob outboard file from s3 bucket"
        ));
    }
    Ok(())
}
