use std::time::SystemTime;

use anyhow::Result;
use p2panda_core::{
    validate_backlink, validate_operation, Body, Extension, Header, Operation, PrivateKey,
};
use p2panda_store::{LogId, LogStore, OperationStore};

use crate::extensions::RhioExtensions;
use crate::messages::FileSystemEvent;

pub fn ingest<S>(
    store: &mut S,
    header: Header<RhioExtensions>,
    body: Option<Body>,
) -> Result<Operation<RhioExtensions>>
where
    S: OperationStore<RhioExtensions> + LogStore<RhioExtensions>,
{
    let operation = Operation {
        hash: header.hash(),
        header,
        body,
    };

    // Validate operation format.
    validate_operation(&operation)?;

    // Get latest operation.
    let log_id: LogId = RhioExtensions::extract(&operation);
    let latest_operation = store
        .latest_operation(operation.header.public_key, log_id)
        .expect("memory store does not error");

    // Validate that it matches the backlink.
    if let Some(latest_operation) = latest_operation {
        validate_backlink(&latest_operation.header, &operation.header)?;
    }

    // Persist the operation.
    store
        .insert_operation(operation.clone())
        .expect("no errors from memory store");

    Ok(operation)
}

pub fn create<S>(
    store: &mut S,
    private_key: &PrivateKey,
    fs_event: &FileSystemEvent,
) -> Result<Operation<RhioExtensions>>
where
    S: OperationStore<RhioExtensions> + LogStore<RhioExtensions>,
{
    // Encode body.
    let body = Body::new(&fs_event.to_bytes());

    // Sign and encode header.
    let public_key = private_key.public_key();
    let latest_operation = store.latest_operation(public_key, public_key.to_string().into())?;

    let (seq_num, backlink) = match latest_operation {
        Some(operation) => (operation.header.seq_num + 1, Some(operation.hash)),
        None => (0, None),
    };

    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_secs();

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
        extensions: Some(RhioExtensions::default()),
    };
    header.sign(private_key);

    // Persist operation in our memory store
    let operation = Operation {
        hash: header.hash(),
        header: header.clone(),
        body: Some(body.clone()),
    };

    store.insert_operation(operation.clone())?;

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
