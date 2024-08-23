# File-system sync

**1. Start first node**

```bash
# Create "blobs" folder
mkdir blobs

# Run node pointing at folder
RUST_LOG=rhio=info cargo run -- -b ./blobs
```

**2. Start second node**

```bash
# Create another "blobs" folder (for example on a second machine, if on the same machine, make sure it has a different name)
mkdir blobs-2

# The first node will spawn and a connection ticket will be printed in stdout, launch a second node and use these addresses to connect to it
RUST_LOG=rhio=info cargo run -- -b ./blobs-2 -t "<TICKET>"

# Feel free to add more nodes ..
```

**3. Add files into one blobs folder**

All nodes will eventually sync up!
