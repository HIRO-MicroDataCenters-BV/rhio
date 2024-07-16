use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use anyhow::Result;
use futures_lite::future::Boxed as BoxedFuture;
use futures_util::future::join_all;
use iroh_net::endpoint::{get_remote_node_id, Connecting};
use p2panda_net::ProtocolHandler;
use tracing::info;

#[derive(Debug)]
pub struct BlobsProtocol<S> {
    rt: tokio_util::task::LocalPoolHandle,
    store: S,
}

impl<S: iroh_blobs::store::Store> BlobsProtocol<S> {
    pub fn new(store: S, rt: tokio_util::task::LocalPoolHandle) -> Self {
        Self { rt, store }
    }
}

impl<S: iroh_blobs::store::Store> ProtocolHandler for BlobsProtocol<S> {
    fn accept(self: Arc<Self>, conn: Connecting) -> BoxedFuture<Result<()>> {
        Box::pin(async move {
            iroh_blobs::provider::handle_connection(
                conn.await?,
                self.store.clone(),
                MockEventSender,
                self.rt.clone(),
            )
            .await;
            Ok(())
        })
    }
}

#[derive(Debug, Clone)]
struct MockEventSender;

impl iroh_blobs::provider::EventSender for MockEventSender {
    fn send(&self, _event: iroh_blobs::provider::Event) -> futures_lite::future::Boxed<()> {
        Box::pin(std::future::ready(()))
    }
}
