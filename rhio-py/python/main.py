from rhio import rhio_ffi, Node, GossipMessageCallback, Config, Message, FileSystemEvent

import asyncio
import argparse
from watchfiles import awatch, Change

class Callback(GossipMessageCallback):
    def __init__(self, name):
        self.name = name
        self.chan = asyncio.Queue()

    async def on_message(self, msg):
        print("received message ", msg.content(), " from ", msg.delivered_from())
        await self.chan.put(msg)

async def main():
    # setup event loop, to ensure async callbacks work
    loop = asyncio.get_running_loop()
    rhio_ffi.uniffi_set_event_loop(loop)

    # parse arguments
    parser = argparse.ArgumentParser(description='Python Rhio Node')
    parser.add_argument('-p', '--port', type=int, default=2024, help='node bind port')
    parser.add_argument('-n', '--direct-node-addresses', type=str, action='append', default=[], help='direct node addresses NODE_ID|IP_ADDR')
    parser.add_argument('-k', '--private-key', type=str, help='path to private key')
    parser.add_argument('-b', '--blobs-path', type=str, help='path to blobs dir')
    parser.add_argument('-r', '--relay-addresses', type=str, action='append', default=[], help='relay addresses')

    args = parser.parse_args()

    config = Config()
    config.bind_port = args.port
    config.direct_node_addresses = args.direct_node_addresses
    config.private_key = args.private_key
    config.blobs_path = args.blobs_path
    config.relay_addresses = args.relay_addresses

    print(config)

    node = await Node.spawn(config)
    print("Node ID: ", node.id())

    topic = "test"
    cb = Callback("n0")

    print("subscribing to gossip topic")
    sender = await node.subscribe(topic, cb)

    print("gossip topic ready")

    if config.blobs_path:        
        async for changes in awatch(config.blobs_path):
            print(changes)
            for (change_type, path) in changes:
                # we only handle "added" events
                if change_type == 1:
                    hash = await node.import_blob(path)

if __name__ == "__main__":
    asyncio.run(main())
