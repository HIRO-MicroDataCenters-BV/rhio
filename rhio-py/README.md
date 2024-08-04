# rhio-py

Use `uniffi` and `maturin` to build ffi bindings for `rhio` and compile a `Python` package.

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
