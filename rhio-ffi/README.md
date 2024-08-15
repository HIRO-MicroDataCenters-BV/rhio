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
`python3 python/sync.py -sync-path="path_to_sync_dir"` syncs files in the provided directory with other peers

### CLI

@TODO