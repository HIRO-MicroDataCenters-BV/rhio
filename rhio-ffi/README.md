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

## `Python` use

There are two example python scripts in the `python` directory. 

`python3 python/hello_world.py` simple node which says hello to discovered peers  
`python3 python/main.py -blobs-path="blobs"` syncs files in the provided directory with other peers

### CLI

```bash
> python3 python/main.py --help                
usage: main.py [-h] [-p PORT] [-t TICKET] [-k PRIVATE_KEY] [-b BLOBS_PATH] [-r RELAY]

Python Rhio Node

options:
  -h, --help            show this help message and exit
  -p PORT, --port PORT  node bind port
  -t TICKET, --ticket TICKET
                        connection ticket string
  -k PRIVATE_KEY, --private-key PRIVATE_KEY
                        path to private key
  -b BLOBS_PATH, --blobs-path BLOBS_PATH
                        path to blobs dir
  -r RELAY, --relay RELAY
                        relay addresses
```