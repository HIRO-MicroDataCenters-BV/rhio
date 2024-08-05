import os

import argparse
import asyncio
from loguru import logger
from rhio import (
    rhio_ffi,
    Node,
    GossipMessageCallback,
    Config,
    Message,
    MessageType,
    TopicId,
)
from watchfiles import awatch, Change


class HelloWorld(GossipMessageCallback):
    """A simple callback implementation which prints all events received on a gossip topic"""

    async def on_message(self, msg, meta):
        msg = msg.as_application()
        logger.info("received {} from {}", msg, meta.delivered_from())


async def main():
    # setup event loop, to ensure async callbacks work
    loop = asyncio.get_running_loop()
    rhio_ffi.uniffi_set_event_loop(loop)

    # parse arguments
    parser = argparse.ArgumentParser(description="Python Rhio Node")
    parser.add_argument("-p", "--port", type=int, default=2024, help="node bind port")
    parser.add_argument(
        "-t",
        "--ticket",
        type=str,
        action="append",
        default=[],
        help="connection ticket string",
    )
    parser.add_argument("-k", "--private-key", type=str, help="path to private key")
    parser.add_argument("-r", "--relay", type=str, help="relay addresses")

    args = parser.parse_args()

    # construct node config
    config = Config()
    config.bind_port = args.port
    config.ticket = args.ticket
    config.private_key = args.private_key
    config.relay = args.relay

    # spawn the rhio node
    node = await Node.spawn(config)
    logger.info("Node ID: {}", node.id())

    # subscribe to a topic, providing a callback method which will be run on each
    # topic event we receive
    topic = TopicId.new_from_str("rhio/hello_world")

    logger.info("subscribing to gossip topic: {}", topic)
    sender = await node.subscribe(topic, HelloWorld())
    await sender.ready()

    logger.info("gossip topic ready")

    while True:
        await asyncio.sleep(1)
        msg = Message.application(bytearray("hello!", encoding="utf-8"))
        await sender.send(msg)


if __name__ == "__main__":
    asyncio.run(main())
