#! /bin/bash

cargo run --features="uniffi-cli" \
    --bin uniffi-bindgen generate \
    --library ../target/release/librhio_py.so \
    --language python \
    --out-dir ./builds --no-format
