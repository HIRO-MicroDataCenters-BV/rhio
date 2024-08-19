import os, asyncio

from config import parse_args, config_from_args
from loguru import logger
from rhio import (
    rhio_ffi,
    Node,
    GossipMessageCallback,
    Message,
    MessageType,
    RhioError,
    TopicId,
)


class HandleAnnouncement(GossipMessageCallback):
    """Download an announced blob to the blob store and then export it to minio bucket"""

    def __init__(self, node, config):
        self.node = node
        self.minio_bucket_name = config.minio_bucket_name()
        self.minio_region = config.minio_region()
        self.minio_endpoint = config.minio_endpoint()

    async def on_message(self, msg, meta):
        hash = msg.as_blob_announcement()
        logger.info("received {} from {}", msg, meta.delivered_from())
        await self.node.download_blob(hash)
        logger.info("blob downloaded: {}", hash)
        await self.node.export_blob_minio(
            hash, self.minio_region, self.minio_endpoint, self.minio_bucket_name
        )
        logger.info("blob exported to minio: {}", hash)


async def import_file(node, config, import_path):
    hash = await node.import_blob(import_path)
    logger.info("file imported: {} {}", hash, import_path)
    await node.export_blob_minio(hash, config.minio_region(), config.minio_endpoint(), config.minio_bucket_name())
    logger.info("blob exported to minio: {}", hash)
    return hash


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
    topic = TopicId.new_from_str("rhio/blob_announce")

    logger.info("subscribing to gossip topic: {}", topic)
    sender = await node.subscribe(topic, HandleAnnouncement(node, config))

    await sender.ready()
    logger.info("gossip topic ready")

    # Import and announce files from path or URL (provided via stdin)
    while True:
        import_path = await asyncio.to_thread(input, "Enter file path or URL: ")
        try:
            hash = await import_file(node, config, import_path)
            logger.info("announce blob: {}", hash)
            await sender.announce_blob(hash)
        except RhioError as e:
            logger.error(e.message())


if __name__ == "__main__":
    asyncio.run(main())
