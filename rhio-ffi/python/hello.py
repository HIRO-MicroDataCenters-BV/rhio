import os, asyncio

from config import parse_args, config_from_args
from loguru import logger
from rhio import (
    rhio_ffi,
    Node,
    GossipMessageCallback,
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
    args = parse_args()
    config = config_from_args(args)

    # spawn the rhio node
    node = await Node.spawn(config)
    logger.info("Node ID: {}", node.id())

    # subscribe to a topic, providing a callback method which will be run on each
    # topic event we receive
    topic = TopicId.from_str("rhio/hello_world")

    logger.info("subscribing to gossip topic: {}", topic)
    sender = await node.subscribe(topic, HelloWorld())
    await sender.ready()

    logger.info("gossip topic ready")

    while True:
        await asyncio.sleep(1)
        msg = Message.application(bytearray("Hello from Python!", encoding="utf-8"))
        await sender.send(msg)


if __name__ == "__main__":
    asyncio.run(main())
