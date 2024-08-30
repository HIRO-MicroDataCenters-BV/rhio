# rhio

> ℹ️ rhio is currently in an experimental development phase and (not yet) intended to be used in production setups. See the [issue tracker](https://github.com/HIRO-MicroDataCenters-BV/rhio/issues) for missing features.

rhio is a peer-to-peer message stream and blob storage solution allowing processes to rapidly exchange messages and efficiently replicate large blobs without any centralised coordination.

rhio has been designed to be integrated into a Kubernetes cluster where _internal_ cluster messaging and persistence is handled centrally via [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) while _external_ cluster messaging is decentralised and handled via [p2panda](https://p2panda.org). Blobs of any size are replicated separately with efficient [bao encoding](https://github.com/oconnor663/bao) and stored in a [MinIO](https://min.io/) database.

Similar to NATS JetStream, any number of streams can be subscribed to and filtered by "subjects".

<details>
<summary>Show diagram</summary>

```
                                                   .. other clusters ..

                                                           ▲  │
 Cluster                                                   │  │
┌──────────────────────────────────────────────────────────┼──┼──────┐
│                                                          │  │      │
│ ┌─────────────────┐         ┌──────────┐   Publish   ┌───┼──▼───┐  │
│ │                 ┼─────────►          ┼─────────────►          │  │
│ │   .. other      │         │   NATS   │             │   rhio   │  │
│ │   processes ..  │         │  Server  │  Subscribe  │ p2p node │  │
│ │                 ◄─────────┼          ◄─────────────┼          │  │
│ └─────────────────┘         └──────────┘             └───▲──┬───┘  │
│                                                          │  │      │
│                                                          │  │      │
│                                                     ┌────┼──▼────┐ │
│                                                     │  MinIO S3  │ │
│                                                     │ Blob Store │ │
│                                                     └────────────┘ │
└────────────────────────────────────────────────────────────────────┘
```
</details>

## Usage

1. Copy the [configuration file](/rhio/config.example.toml) and adjust it to your setup: `$ cp config.example.toml config.toml`
2. Run the `rhio` process via `rhio -c config.toml`
3. The process can be further configured via ENV vars or command line arguments:

```
Peer-to-peer message and blob streaming with MinIO and NATS JetStream support

Usage: rhio [OPTIONS]

Options:
  -c, --config <PATH>
          Path to "config.toml" file for further configuration.

          When not set the program will try to find a `config.toml` file in the same folder the program is
          executed in and otherwise in the regarding operation systems XDG config directory
          ("$HOME/.config/rhio/config.toml" on Linux).

  -p, --bind-port <PORT>
          Bind port of rhio node

  -k, --private-key <PATH>
          Path to file containing hexadecimal-encoded Ed25519 private key

  -b, --blobs-dir <PATH>
          Path to file-system blob store to temporarily load blobs into when importing to MinIO database.

          WARNING: When left empty, an in-memory blob store is used instead which might lead to data corruption
          as blob hashes are not kept between restarts. Use the in-memory store only for testing purposes.

  -l, --log-level <LEVEL>
          Set log verbosity. Use this for learning more about how your node behaves or for debugging.

          Possible log levels are: ERROR, WARN, INFO, DEBUG, TRACE. They are scoped to "rhio" by default.

          If you want to adjust the scope for deeper inspection use a filter value, for example
          "=TRACE" for logging _everything_ or "rhio=INFO,async_nats=DEBUG" etc.

  -h, --help
          Print help (see a summary with '-h')

  -V, --version
          Print version
```

### Publish

#### Messages

rhio does not create or publish any messages by itself and serves merely as a "router" coordinating streams inside and outside the cluster. To publish messages into the stream the regular NATS Core or JetStream API is used. Other processes inside the cluster can independently publish messages to the NATS Server which will then be automatically picked up, processed and forwarded to other nodes by rhio.

Messages need to be encoded based on the p2panda [Operation](https://p2panda.org/specifications/namakemono/#operations) specification and contain the custom rhio headers.

With `rhio-client` library (Rust and Python) it is possible to create messages encoded in the right format. You can also use the interactive demo to send messages to any NATS server for the given subject:

```bash
rhio-client --subject foo.bar --endpoint localhost:4222
# Type: any message which should be received by all nodes ..
```

#### Blobs

Large files of any size can be imported into the local MinIO database and then announced on the network for other nodes to download them into their regarding MinIO databases. For this to take place in an efficient manner, the blob needs to be first encoded in the bao format. The resulting hash of this process can be used as a unique identifier to announce the blob on the network.

1. Instruct rhio to import and encode a file from the file system into the MinIO database. Send its path by publishing to the NATS Core subject `rhio.import`. The resulting hash is displayed in the server's logs. A [reply subject](https://docs.nats.io/nats-concepts/core-nats/reqreply) can also be specified for the resulting hash to be sent to.
```bash
nats request rhio.import /home/user/images/sloth.jpg
```
2. Publish a message which announces the blob hash on the network. Other peers will be made aware of this new blob and request to download it from the node. See "Messages" to understand how to publish these messages or use the interactive demo instead:
```bash
rhio-client --subject foo.bar --endpoint localhost:4222
# Type: blob <hash>
```

### Stream

rhio does not offer any direct APIs to subscribe to message streams. To consume data the regular NATS JetStream API is used and messages need to be validated as they are encoded in the p2panda Operation format.

## Development

### Prerequisites

* Rust 1.80.1+
* [NATS Server](https://docs.nats.io/running-a-nats-service/introduction) with [JetStream](https://docs.nats.io/running-a-nats-service/configuration/resource_management) enabled
* [`nats`](https://docs.nats.io/using-nats/nats-tools/nats_cli)
* [MinIO](https://min.io/download)

### Installation and running

1. Launch `rhio` node:
```bash
# Run with default configurations
cargo run

# Pass additional arguments to `rhio`
cargo run -- --config config.toml
```
2. Configure log level
```bash
# Enable additional logging
cargo run -- --log-level "DEBUG"

# Enable logging for specific target
cargo run -- --log-level "async_nats=DEBUG"

# Enable logging for _all_ targets
cargo run -- --log-level "=TRACE"
```
3. Launch `rhio-client` demo client
```bash
cargo run --bin rhio-client -- --subject foo.bar
```
4. Run tests, linters and format checkers
```bash
cargo test
cargo clippy
cargo fmt
```
5. Build `rhio` for production
```bash
cargo build --release
```
