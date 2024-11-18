use anyhow::Result;
use iroh_blobs::store::bao_tree::io::sync::WriteAt;
use iroh_blobs::util::SparseMemFile;
use s3::Bucket;

use crate::bao_file::BaoMeta;
use crate::Paths;

pub async fn put_meta(bucket: &Bucket, paths: &Paths, meta: &BaoMeta) -> Result<()> {
    let response = bucket.put_object(paths.meta(), &meta.to_bytes()).await?;
    if response.status_code() != 200 {
        return Err(anyhow::anyhow!(
            "Failed to upload blob meta file to s3 bucket"
        ));
    }

    Ok(())
}

pub async fn put_outboard(bucket: &Bucket, paths: &Paths, outboard: &[u8]) -> Result<()> {
    let response = bucket.put_object(paths.outboard(), outboard).await?;
    if response.status_code() != 200 {
        return Err(anyhow::anyhow!(
            "Failed to upload blob outboard file to s3 bucket"
        ));
    }

    Ok(())
}

pub async fn get_meta(bucket: &Bucket, paths: &Paths) -> Result<BaoMeta> {
    let response = bucket.get_object(paths.meta()).await?;
    if response.status_code() != 200 {
        return Err(anyhow::anyhow!("Failed to get blob meta file to s3 bucket"));
    }
    let meta = BaoMeta::from_bytes(response.as_slice())?;
    Ok(meta)
}

pub async fn get_outboard(bucket: &Bucket, paths: &Paths) -> Result<SparseMemFile> {
    let response = bucket.get_object(paths.outboard()).await?;
    if response.status_code() != 200 {
        return Err(anyhow::anyhow!(
            "Failed to get blob outboard file to s3 bucket"
        ));
    }
    let mut outboard = SparseMemFile::new();
    outboard.write_all_at(0, response.as_slice())?;
    Ok(outboard)
}
