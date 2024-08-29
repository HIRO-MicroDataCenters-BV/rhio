# rhio-ffi

> :warning: `rhio` has been undergoing some major changes and `rhio-ffi` needs
> to be updated to properly function again. See related issue:
> https://github.com/HIRO-MicroDataCenters-BV/rhio/issues/63

Use `uniffi` and `maturin` to build ffi bindings for `rhio` and compile a `Python` package.

## Python development

```bash
# Create and activate a virtual env
virtualenv .
source ./bin/activate
# Install dependencies
pip install -r requirements.txt
# Build wheel
maturin develop
# Run the example (run this in two terminals)
python python/hello.py
```

## MinIO container

Run a local `minio` instance for testing purposes.

```bash
# Start two minio instance (run from repository root directory)
docker-compose up
```

## Python usage

### CLI

```shell
# run the main python cli application
python python/main.py -c "rhio:rhio_password"
```

```shell
python python/main.py --help
usage: main.py [-h] [-p PORT] [-t TICKET] [-k PRIVATE_KEY] [-s SYNC_DIR] [-b BLOBS_DIR] [-n MINIO_BUCKET_NAME] [-e MINIO_ENDPOINT] [-g MINIO_REGION] [-c MINIO_CREDENTIALS]
               [-r RELAY]

p2p blob syncing node for minio databases

options:
  -h, --help            show this help message and exit
  -p PORT, --port PORT  node bind port
  -t TICKET, --ticket TICKET
                        connection ticket string
  -k PRIVATE_KEY, --private-key PRIVATE_KEY
                        path to private key
  -s SYNC_DIR, --sync-dir SYNC_DIR
                        path to sync directory (for use with example/sync)
  -b BLOBS_DIR, --blobs-dir BLOBS_DIR
                        path to blob store and database
  -n MINIO_BUCKET_NAME, --minio-bucket-name MINIO_BUCKET_NAME
                        minio bucket name
  -e MINIO_ENDPOINT, --minio-endpoint MINIO_ENDPOINT
                        minio instance endpoint address
  -g MINIO_REGION, --minio-region MINIO_REGION
                        minio instance region
  -c MINIO_CREDENTIALS, --minio-credentials MINIO_CREDENTIALS
                        minio credentials in the format <ACCESS_KEY>:<SECRET_KEY>
  -r RELAY, --relay RELAY
                        relay addresses
```

### Examples

There are two example python scripts in the `python` directory. 

`python python/hello_world.py` simple node which says hello to discovered peers  
`python python/sync.py --sync-dir="path_to_sync_dir"` syncs files in the provided directory with other peers
