from rhio import rhio_ffi, Node, GossipMessageCallback, Config, Message

import asyncio
import argparse

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
    parser.add_argument('-p', '--port', type=int, help='node bind port')
    parser.add_argument('-n', '--direct-node-addresses', type=str, action='append', help='direct node addresses')
    parser.add_argument('-k', '--private-key', type=str, help='path to private key')
    parser.add_argument('-b', '--blobs-path', type=str, help='path to blobs dir')
    parser.add_argument('-r', '--relay-addresses', type=str, action='append', help='relay addresses')

    args = parser.parse_args()

    print(args)

    config = Config()
    node = await Node.spawn(config)
    print("Node ID: ", node.id())

    topic = "test"
    cb = Callback("n0")

    print("subscribing to gossip topic")
    sender = await node.subscribe(topic, cb)

    print("gossip topic ready")

    while True:
        await asyncio.sleep(1)
        await sender.send(Message(bytearray("hello".encode("utf-8"))))

if __name__ == "__main__":
    asyncio.run(main())
