# p2panda + rhio

Towards a p2p blob syncing solution for HIRO with p2panda modules.

## Crates

> All `p2panda-*` crates will eventually be moved into the official `p2panda` repository, currently they are kept here to experiment with the APIs, directly connected to `rhio`.

* `rhio`: Node implementation to coordinate announcements and sync of blobs from minio backends in a decentralised overlay network
* `rhio-py`: FFI bindings allowing `rhio` to be used in (experimental) Python environments
* `p2panda-net`: IPv8 inspired interface to build p2p overlays for any protocol with customizable bootstrap, sync and discovery strategies based on `iroh-net` and `iroh-gossip`
* `p2panda-blobs`: Wrapper around `iroh-blobs`, a BAO-tree based efficient syncing solution for very large blobs, to be plugged into `p2panda-net`
* `p2panda-sync`: Set Reconciliation algorithm for efficient syncing of data between two nodes, can be plugged into `p2panda-net`
* `minio-store`: Blob storage interface to minio databases

## Usage

Read the regarding `README.md` files for each crate in this mono-repository for further usage instructions. The most high-level application is `rhio` and thus probably the best starting point.
