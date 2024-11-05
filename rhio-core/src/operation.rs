use std::time::SystemTime;

use anyhow::Result;
use p2panda_core::{Body, Hash, Header, Operation, PrivateKey};
use p2panda_store::{LogStore, OperationStore};

use crate::extensions::RhioExtensions;
use crate::LogId;

pub async fn create_blob_announcement<S>(
    store: &mut S,
    private_key: &PrivateKey,
    subject: &str,
    blob_hash: Hash,
) -> Result<Operation<RhioExtensions>>
where
    S: OperationStore<LogId, RhioExtensions> + LogStore<LogId, RhioExtensions>,
{
    create_operation(store, private_key, subject, Some(blob_hash), None).await
}

pub async fn create_message<S>(
    store: &mut S,
    private_key: &PrivateKey,
    subject: &str,
    body: &[u8],
) -> Result<Operation<RhioExtensions>>
where
    S: OperationStore<LogId, RhioExtensions> + LogStore<LogId, RhioExtensions>,
{
    create_operation(store, private_key, subject, None, Some(body)).await
}

pub async fn create_operation<S>(
    store: &mut S,
    private_key: &PrivateKey,
    subject: &str,
    blob_hash: Option<Hash>,
    body: Option<&[u8]>,
) -> Result<Operation<RhioExtensions>>
where
    S: OperationStore<LogId, RhioExtensions> + LogStore<LogId, RhioExtensions>,
{
    let body = body.map(Body::new);

    let public_key = private_key.public_key();
    let log_id = LogId::new(subject);

    // let latest_operation = store.latest_operation(&public_key, &log_id).await?;
    //
    // let (seq_num, backlink) = match latest_operation {
    //     Some(operation) => (operation.header.seq_num + 1, Some(operation.hash)),
    //     None => (0, None),
    // };

    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_secs();

    let extensions = RhioExtensions {
        subject: Some(subject.to_owned()),
        blob_hash,
        ..Default::default()
    };

    let mut header = Header {
        version: 1,
        public_key,
        signature: None,
        payload_size: body.as_ref().map_or(0, |body| body.size()),
        payload_hash: body.as_ref().map(|body| body.hash()),
        timestamp,
        seq_num: 0,
        backlink: None,
        previous: vec![],
        extensions: Some(extensions),
    };
    header.sign(private_key);

    let operation = Operation {
        hash: header.hash(),
        header,
        body,
    };

    // store.insert_operation(&operation, &log_id).await?;

    Ok(operation)
}

pub fn decode_operation(bytes: &[u8]) -> Result<(Header<RhioExtensions>, Option<Body>)> {
    let header_and_body =
        ciborium::from_reader::<(Header<RhioExtensions>, Option<Body>), _>(bytes)?;
    Ok(header_and_body)
}

pub fn encode_operation(header: Header<RhioExtensions>, body: Option<Body>) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(&(header, body), &mut bytes)?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use p2panda_core::PrivateKey;
    use p2panda_store::MemoryStore;

    use super::{create_operation, decode_operation, encode_operation};

    #[tokio::test]
    async fn operation_roundtrips() {
        let private_key = PrivateKey::new();
        let mut store = MemoryStore::new();
        let subject = "icecreams.vanilla.dropped".into();
        for i in 0..16 {
            let body = format!("Oh, no! {i}");
            let operation = create_operation(
                &mut store,
                &private_key,
                subject,
                None,
                Some(body.as_bytes()),
            )
            .await
            .unwrap();
            let encoded_operation =
                encode_operation(operation.header.clone(), operation.body.clone()).unwrap();
            let decoded_operation = decode_operation(&encoded_operation).unwrap();
            assert_eq!(operation.header, decoded_operation.0);
            assert_eq!(operation.body, decoded_operation.1);
        }
    }
}
