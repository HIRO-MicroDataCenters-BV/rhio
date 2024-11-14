# rhio

rhio is a peer-to-peer message stream and blob storage solution allowing processes to rapidly exchange messages and efficiently replicate large blobs without any centralised coordination.

rhio has been designed to be integrated into a Kubernetes cluster where _internal_ cluster messaging and persistence is handled centrally via [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) while _external_ cluster messaging is decentralised and handled via [p2panda](https://p2panda.org). Blobs of any size are replicated separately with efficient [bao encoding](https://github.com/oconnor663/bao) and stored in a [MinIO](https://min.io/) or any other S3-compatible database.

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

1. Copy the [configuration file](/rhio/config.example.yaml) and adjust it to your setup: `cp config.example.yaml config.yaml`
2. Run the `rhio` process via `rhio -c config.yaml`
3. The process can be further configured via ENV vars or command line arguments:

```
Peer-to-peer message and blob streaming with MinIO and NATS JetStream support

Usage: rhio [OPTIONS]

Options:
  -c, --config <PATH>
          Path to "config.yaml" file for further configuration.

          When not set the program will try to find a `config.yaml` file in the same folder the program is
          executed in and otherwise in the regarding operation systems XDG config directory
          ("$HOME/.config/rhio/config.yaml" on Linux).

  -p, --bind-port <PORT>
          Bind port of rhio node

  -k, --private-key-path <PATH>
          Path to file containing hexadecimal-encoded Ed25519 private key

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

### Messages

rhio does not create or publish any messages by itself and serves merely as a "router" coordinating streams inside and outside the cluster. To publish messages into the stream the regular NATS Core or JetStream API is used. Other processes inside the cluster can independently publish messages to the NATS Server which will then be automatically picked up, processed and forwarded to other nodes by rhio.

Since NATS streams are also used for persistance with their own wide range of limit configurations, rhio does not create any streams automatically but merely consumes them. This allows rhio operators to have full flexibility over the nature of the stream. This is why for every published subject a "stream name" needs to be mentioned.

To consume messages the regular NATS JetStream API is used.

### Blobs

Large files of any size can be imported into the local MinIO database which will then be automatically announced on the network for other nodes to download them into their regarding MinIO databases. For this to take place in an efficient manner, the blob will be first encoded in the bao format. The resulting hash of this process is used as an unique identifier to announce the blob on the p2p network.

## Development

### Prerequisites

* Rust 1.82.0+
* [NATS Server](https://docs.nats.io/running-a-nats-service/introduction) with [JetStream](https://docs.nats.io/running-a-nats-service/configuration/resource_management) enabled
* [NATS Command Line Tool](https://docs.nats.io/using-nats/nats-tools/nats_cli)
* [MinIO](https://min.io/download)

### Installation and running

1. Launch `rhio` node
```bash
# Run with default configurations
cargo run

# Pass additional arguments to `rhio`
cargo run -- --config config.yaml
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

## License

[MIT](LICENSE)
