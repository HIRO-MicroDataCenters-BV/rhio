# rhio-ffi

Use `uniffi` and `maturin` to build ffi bindings for `rhio` and compile a `Python` package.

## `Python` development

```bash
# Create and activate a virtual env
virtualenv .
source ./bin/activate
# Install dependencies
pip install -r requirements.txt
# Build wheel
maturin develop
# Run the example (run this in two terminals)
python3 python/hello_world.py
```

## `minio` container

Run a local `minio` instance for testing purposes.

```bash
# Start minio instance (run from repository root directory)
docker-compose up
```

## `Python` usage

### CLI

```bash
# run the main python cli application
python3 python/main.py -c "rhio:rhio_password"
```

```bash
python3 python/main.py --help

usage: main.py [-h] [-p PORT] [-t TICKET] [-k PRIVATE_KEY] [-s SYNC_DIR] [-b BLOBS_DIR] [-i IMPORT_PATH] [-n BUCKET_NAME] [-a BUCKET_ADDRESS] [-c CREDENTIALS] [-r RELAY]

Python Rhio Node

options:
  -h, --help            show this help message and exit
  -p PORT, --port PORT  node bind port
  -t TICKET, --ticket TICKET
                        connection ticket string
  -k PRIVATE_KEY, --private-key PRIVATE_KEY
                        path to private key
  -s SYNC_DIR, --sync-dir SYNC_DIR
                        path to sync directory
  -b BLOBS_DIR, --blobs-dir BLOBS_DIR
                        path to blob store and database
  -i IMPORT_PATH, --import-path IMPORT_PATH
                        path or URL to file which should be imported to the blob store
  -n BUCKET_NAME, --bucket-name BUCKET_NAME
                        bucket name
  -a BUCKET_ADDRESS, --bucket-address BUCKET_ADDRESS
                        bucket address in the format <ENDPOINT>:<REGION>
  -c CREDENTIALS, --credentials CREDENTIALS
                        minio credentials in the format <ACCESS_KEY>:<SECRET_KEY>
  -r RELAY, --relay RELAY
                        relay addresses
```

### examples

There are two example python scripts in the `python` directory. 

`python3 python/hello_world.py` simple node which says hello to discovered peers  
`python3 python/sync.py -sync-path="path_to_sync_dir"` syncs files in the provided directory with other peers
