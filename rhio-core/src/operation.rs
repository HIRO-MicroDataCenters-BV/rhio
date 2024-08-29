use std::time::SystemTime;

use anyhow::{anyhow, Context, Result};
use p2panda_core::{
    validate_backlink, validate_operation, Body, Extension, Header, Operation, PrivateKey,
};
use p2panda_store::{LogStore, OperationStore};

use crate::extensions::{RhioExtensions, Subject};
use crate::log_id::LogId;

pub fn create_operation<S>(
    store: &mut S,
    private_key: &PrivateKey,
    subject: &str,
    body: &[u8],
) -> Result<Operation<RhioExtensions>>
where
    S: OperationStore<LogId, RhioExtensions> + LogStore<LogId, RhioExtensions>,
{
    let body = Body::new(body);

    let public_key = private_key.public_key();
    let log_id = LogId::new(&subject);

    let latest_operation = store.latest_operation(public_key, log_id.clone())?;

    let (seq_num, backlink) = match latest_operation {
        Some(operation) => (operation.header.seq_num + 1, Some(operation.hash)),
        None => (0, None),
    };

    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_secs();

    let extensions = RhioExtensions {
        subject: Some(subject.to_owned()),
    };

    let mut header = Header {
        version: 1,
        public_key,
        signature: None,
        payload_size: body.size(),
        payload_hash: Some(body.hash()),
        timestamp,
        seq_num,
        backlink,
        previous: vec![],
        extensions: Some(extensions),
    };
    header.sign(private_key);

    let operation = Operation {
        hash: header.hash(),
        header: header.clone(),
        body: Some(body.clone()),
    };

    store.insert_operation(operation.clone(), log_id)?;

    Ok(operation)
}

pub fn ingest_operation<S>(
    store: &mut S,
    header: Header<RhioExtensions>,
    body: Option<Body>,
) -> Result<Operation<RhioExtensions>>
where
    S: OperationStore<LogId, RhioExtensions> + LogStore<LogId, RhioExtensions>,
{
    let operation = Operation {
        hash: header.hash(),
        header: header.clone(),
        body,
    };
    validate_operation(&operation)?;

    let subject: Subject = header
        .extract()
        .ok_or(anyhow!("missing 'subject' field in header"))?;

    let log_id = LogId::new(&subject);

    let latest_operation = store
        .latest_operation(operation.header.public_key, log_id.clone())
        .context("critical store failure")?;

    if let Some(latest_operation) = latest_operation {
        validate_backlink(&latest_operation.header, &operation.header)?;
    }

    store
        .insert_operation(operation.clone(), log_id)
        .context("critical store failure")?;

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

    #[test]
    fn operation_roundtrips() {
        let private_key = PrivateKey::new();
        let mut store = MemoryStore::new();
        let subject = "icecreams.vanilla.dropped".into();
        for i in 0..16 {
            let body = format!("Oh, no! {i}");
            let operation =
                create_operation(&mut store, &private_key, subject, body.as_bytes()).unwrap();
            let encoded_operation =
                encode_operation(operation.header.clone(), operation.body.clone()).unwrap();
            let decoded_operation = decode_operation(&encoded_operation).unwrap();
            assert_eq!(operation.header, decoded_operation.0);
            assert_eq!(operation.body, decoded_operation.1);
        }
    }
}
