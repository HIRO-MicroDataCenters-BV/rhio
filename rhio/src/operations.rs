use std::time::SystemTime;

use anyhow::Result;
use p2panda_core::{
    validate_backlink, validate_operation, Body, Extension, Header, Operation, PrivateKey,
};
use p2panda_store::{LogStore, OperationStore};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::extensions::{LogId, RhioExtensions};
use crate::messages::{Message, ToBytes};

pub fn ingest<S>(
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

    // Validate operation format.
    validate_operation(&operation)?;

    // Get latest operation.
    let log_id: LogId = header
        .extract()
        .unwrap_or_else(|| header.clone().public_key.to_string());
    let latest_operation = store
        .latest_operation(operation.header.public_key, log_id.clone())
        .expect("memory store does not error");

    // Validate that it matches the backlink.
    if let Some(latest_operation) = latest_operation {
        validate_backlink(&latest_operation.header, &operation.header)?;
    }

    // Persist the operation.
    store
        .insert_operation(operation.clone(), log_id.to_owned())
        .expect("no errors from memory store");

    Ok(operation)
}

pub fn create<S, T>(
    store: &mut S,
    private_key: &PrivateKey,
    log_id: &LogId,
    message: &Message<T>,
) -> Result<Operation<RhioExtensions>>
where
    S: OperationStore<LogId, RhioExtensions> + LogStore<LogId, RhioExtensions>,
    T: Serialize + DeserializeOwned + Clone,
{
    // Encode body.
    let body = Body::new(&message.to_bytes());

    // Sign and encode header.
    let public_key = private_key.public_key();
    let latest_operation = store.latest_operation(public_key, log_id.clone())?;

    let (seq_num, backlink) = match latest_operation {
        Some(operation) => (operation.header.seq_num + 1, Some(operation.hash)),
        None => (0, None),
    };

    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_secs();

    let extensions = RhioExtensions {
        log_id: Some(log_id.clone()),
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

    // Persist operation in our memory store
    let operation = Operation {
        hash: header.hash(),
        header: header.clone(),
        body: Some(body.clone()),
    };

    store.insert_operation(operation.clone(), log_id.to_owned())?;

    Ok(operation)
}

pub fn decode_header_and_body(bytes: &[u8]) -> Result<(Option<Body>, Header<RhioExtensions>)> {
    let header_and_body =
        ciborium::from_reader::<(Option<Body>, Header<RhioExtensions>), _>(bytes)?;
    Ok(header_and_body)
}

pub fn encode_header_and_body(
    header: Header<RhioExtensions>,
    body: Option<Body>,
) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(&(Some(body), header), &mut bytes)?;
    Ok(bytes)
}
