use anyhow::Result;
use p2panda_core::{validate_backlink, validate_operation, Body, Extension, Header, Operation};
use p2panda_store::{LogStore, OperationStore};

use crate::panda::extensions::{LogId, RhioExtensions};

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
