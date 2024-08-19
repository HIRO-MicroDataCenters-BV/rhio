// SPDX-License-Identifier: AGPL-3.0-or-later

#![feature(trait_alias)]
mod blobs;
mod download;
mod export;
mod import;
mod protocol;

use iroh_blobs::store;

pub use blobs::Blobs;
pub use download::DownloadBlobEvent;
pub use import::ImportBlobEvent;
pub use protocol::{BlobsProtocol, BLOBS_ALPN};

pub type MemoryStore = store::mem::Store;

pub type FilesystemStore = store::fs::Store;

pub trait Store = iroh_blobs::store::Store;
pub trait MapEntry = iroh_blobs::store::MapEntry;
pub trait AsyncSliceReader = iroh_blobs::store::bao_tree::io::fsm::AsyncSliceReader;