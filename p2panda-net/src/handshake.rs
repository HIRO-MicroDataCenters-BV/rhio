// SPDX-License-Identifier: AGPL-3.0-or-later

use std::sync::Arc;

use anyhow::Result;
use futures_lite::future::Boxed as BoxedFuture;
use iroh_gossip::net::Gossip;
use iroh_net::endpoint::{Connecting, Connection};
use tracing::debug_span;

use crate::protocols::ProtocolHandler;

pub const HANDSHAKE_ALPN: &[u8] = b"/p2panda-net-handshake/0";

#[derive(Debug)]
pub struct Handshake {
    gossip: Gossip,
}

impl Handshake {
    pub fn new(gossip: Gossip) -> Self {
        Self { gossip }
    }

    async fn handle_connection(&self, connection: Connection) -> Result<()> {
        let remote_addr = connection.remote_address();
        let connection_id = connection.stable_id() as u64;
        let span = debug_span!("connection", connection_id, %remote_addr);

        // @TODO
        // async {
        //     while let Ok((writer, reader)) = connection.accept_bi().await {
        //     }
        // }
        // .instrument(span)
        // .await;

        Ok(())
    }
}

impl ProtocolHandler for Handshake {
    fn accept(self: Arc<Self>, connecting: Connecting) -> BoxedFuture<Result<()>> {
        Box::pin(async move { self.handle_connection(connecting.await?).await })
    }
}
