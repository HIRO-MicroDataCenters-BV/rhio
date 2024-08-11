# File-system sync

**1. Start first node**

```
# Create "blobs" folder
mkdir blobs

# Run node pointing at folder
cargo run -- -b ./blobs
```

**2. Start second node**

```
# Create another "blobs" folder (for example on a second machine, if on the same machine, make sure it has a different name)
mkdir blobs-2

# The first node will spawn and "direct addresses" will be printed in stdout, launch a second node and use these addresses to connect to it
cargo run -- -b ./blobs-2 -n "<paste direct address here>"

# Feel free to add more nodes ..
```

**3. Add files into one blobs folder**

All nodes will eventually sync up!
