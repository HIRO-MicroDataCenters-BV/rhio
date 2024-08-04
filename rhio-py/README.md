# rhio-py

Use `uniffi` and `maturin` to build ffi bindings for `rhio` and compile a `Python` package.

## Development

```bash
# Create and activate a virtual env
virtualenv .
source ./bin/activate
# Install dependencies
pip install uniffi-bindgen asyncio argparse watchfiles loguru
# Build wheel
maturin develop
# Run the example (run this in two terminals)
python3 python/hello_world.py
```

## Use

There are two example python scripts in the `python` directory. 

`python3 python/hello_world.py` simple node which says hello to discovered peers  
`python3 python/main.py -blobs-path="blobs"` syncs files in the provided directory with other peers

### CLI

```bash
usage: main.py [-h] [-p PORT] [-n DIRECT_NODE_ADDRESSES] [-k PRIVATE_KEY] [-b BLOBS_PATH] [-r RELAY_ADDRESSES]

Python Rhio Node

options:
  -h, --help            show this help message and exit
  -p PORT, --port PORT  node bind port
  -n DIRECT_NODE_ADDRESSES, --direct-node-addresses DIRECT_NODE_ADDRESSES direct node addresses NODE_ID|IP_ADDR
  -k PRIVATE_KEY, --private-key PRIVATE_KEY path to private key
  -b BLOBS_PATH, --blobs-path BLOBS_PATH path to blobs dir
  -r RELAY_ADDRESSES, --relay-addresses RELAY_ADDRESSES relay addresses
```