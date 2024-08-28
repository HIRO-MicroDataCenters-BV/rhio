use anyhow::{Context, Result};
use p2panda_core::{validate_backlink, validate_operation, Body, Extension, Header, Operation};
use p2panda_store::{LogStore, OperationStore};

use crate::panda::extensions::{LogId, RhioExtensions};

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

    let log_id: LogId = header
        .extract()
        .unwrap_or_else(|| header.clone().public_key.to_string());
    let latest_operation = store
        .latest_operation(operation.header.public_key, log_id.clone())
        .context("critical store failure")?;

    if let Some(latest_operation) = latest_operation {
        validate_backlink(&latest_operation.header, &operation.header)?;
    }

    store
        .insert_operation(operation.clone(), log_id.to_owned())
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
