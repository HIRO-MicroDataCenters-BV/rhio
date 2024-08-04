import os

import argparse
import asyncio
from loguru import logger
from rhio import rhio_ffi, Node, GossipMessageCallback, Config, Message, MessageType, TopicId
from watchfiles import awatch, Change

EXPORTED_BLOBS = {}

class Callback(GossipMessageCallback):
    def __init__(self, name):
        self.name = name
        self.chan = asyncio.Queue()

    async def on_message(self, msg, meta):
        await self.chan.put((msg, meta))

async def say_hello(sender):
    while True:
        await asyncio.sleep(1)
        msg = Message.application(bytearray("hello!", encoding='utf-8'))
        await sender.send(msg)

async def print_hello(cb):
    while True:
        (message, meta) = await cb.chan.get()
        msg = message.as_application()
        logger.info("received {} from {}", msg, meta.delivered_from())

async def main():
    # setup event loop, to ensure async callbacks work
    loop = asyncio.get_running_loop()
    rhio_ffi.uniffi_set_event_loop(loop)

    # parse argument
    parser = argparse.ArgumentParser(description='Python Rhio Node')
    parser.add_argument('-p', '--port', type=int, default=2024, help='node bind port')
    parser.add_argument('-n', '--direct-node-addresses', type=str, action='append', default=[], help='direct node addresses NODE_ID|IP_ADDR')
    parser.add_argument('-k', '--private-key', type=str, help='path to private key')
    parser.add_argument('-b', '--blobs-path', type=str, help='path to blobs dir')
    parser.add_argument('-r', '--relay-addresses', type=str, action='append', default=[], help='relay addresses')

    args = parser.parse_args()

    # construct node config
    config = Config()
    config.bind_port = args.port
    config.direct_node_addresses = args.direct_node_addresses
    config.private_key = args.private_key
    config.blobs_path = args.blobs_path
    config.relay_addresses = args.relay_addresses

    # spawn the rhio node
    node = await Node.spawn(config)
    logger.info("Node ID: {}", node.id())

    # subscribe to a topic, providing a callback method which will be run on each 
    # topic event we receive
    topic = TopicId.new_from_str("rhio/hello_world")
    cb = Callback("hello_world_handler")

    logger.info("subscribing to gossip topic: {}", topic)
    sender = await node.subscribe(topic, cb)
    await sender.ready()

    logger.info("gossip topic ready")

    send_task = asyncio.create_task(
        say_hello(sender)
    )

    receive_task = asyncio.create_task(
        print_hello(cb)
    )

    await send_task
    await receive_task

if __name__ == "__main__":
    asyncio.run(main())
