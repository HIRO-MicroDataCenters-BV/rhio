# rhio

> MINIO + HIRO + IROH = RHIO

## `minio` container

Run a local `minio` instance for testing purposes.

```bash
# Start minio instance (run from repository root directory)
docker-compose up
```

## Usage

### CLI

```bash
# Run the cli application
cargo run -- -c "rhio:rhio_password"
```

For extra logging:
```bash
# Run the cli application
RUST_LOG=rhio=info cargo run -- -c "rhio:rhio_password"
```

```bash
     Running `/home/sandreae/Code/rhio/target/debug/rhio --help`
p2p blob syncing node for minio databases

Usage: rhio [OPTIONS]

Options:
  -p, --bind-port <PORT>                     node bind port
  -t, --ticket [<TICKET>...]                 connection ticket string
  -k, --private-key <PATH>                   path to private key
  -s, --sync-dir <PATH>                      path to sync directory (for use with example/sync)
  -b, --blobs-dir <PATH>                     path to blob store and database
  -c, --credentials <ACCESS_KEY:SECRET_KEY>  minio credentials
  -a, --bucket-address <ENDPOINT:REGION>     minio bucket address string
  -n, --bucket-name <NAME>                   minio bucket name
  -r, --relay <URL>                          relay addresses
  -h, --help                                 Print help
  -V, --version                              Print version
```