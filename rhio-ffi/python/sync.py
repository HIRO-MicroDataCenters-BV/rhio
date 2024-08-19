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


class Watcher:
    """A watcher which monitors a directory on the file-system and broadcasts `FileSystem` gossip
    events whenever it is notified that a file has been added"""

    def __init__(self, sender, node, file_system, sync_dir):
        self.sender = sender
        self.node = node
        self.sync_dir = sync_dir
        self.file_system = file_system

    def relative_path(self, path):
        """Get the relative path to a file added to the base directory"""
        return os.path.relpath(path, self.sync_dir)

    async def handle_change(self, change_type, path):
        """handle a change event"""
        # we only handle "added" events
        if change_type == 1:
            # calculate path of blob relative to blobs dir
            rel_path = self.relative_path(path)

            # we don't want to import blobs we just exported, so catch this case here
            if rel_path in self.file_system.exported_blobs:
                self.file_system.exported_blobs.pop(rel_path)
                return

            logger.info("new file added: {}", path)

            # import the blob and get it's hash
            hash = await self.node.import_blob(path)
            logger.info("blob imported: {}", hash)

            # send a file system event to announce a new file was created
            msg = Message.file_system(rel_path, hash)
            meta = await self.sender.send(msg)

            # we don't receive messages we ourselves broadcast on a gossip channel, so pass this
            # new event to our file-system aggregator manually.
            await self.file_system.on_message(msg, meta)

    async def watch(self):
        """Run the watcher"""
        async for changes in awatch(self.sync_dir):
            for change_type, path in changes:
                await self.handle_change(change_type, path)


class FileSystemSync(GossipMessageCallback):
    """Aggregator class which uses last-write-wins logic to maintain a deterministic mapping of
    paths to blob hashes. Consumes FileSystem events and uses the Node api to download blobs from
    the network and export them to the file-system"""

    def __init__(self, node, sync_dir):
        self.node = node
        self.sync_dir = sync_dir
        self.paths = dict()
        self.blobs = set()
        self.exported_blobs = dict()

    async def on_message(self, message, meta):
        """Process FileSystem events"""
        if message.type() == MessageType.FILE_SYSTEM:
            create_event = message.as_file_system_create()
            rel_path = create_event.path
            hash = create_event.hash

            # check if there is already a file at this path, if there is, compare it's timestamp
            # against the incoming events timestamp and only proceed if it is more recent (we
            # fall-back to comparing hashes if the timestamp is equal)
            timestamp_and_hash = self.paths.get(rel_path)
            if (
                timestamp_and_hash is not None
                and (meta.operation_timestamp(), hash) < timestamp_and_hash
            ):
                logger.info(
                    "ignoring file addition at existing path which contains a lower timestamp: {} {}",
                    meta.operation_timestamp(),
                    hash,
                )
                return

            # download the blob from the network, unless we already have done
            if self.blobs.issuperset([hash]) == False:
                await self.node.download_blob(hash)
                self.blobs.add(hash)
                logger.info("downloaded blob: {}", hash)

            # join the blobs dir and relative file path
            path = os.path.join(self.sync_dir, rel_path)

            # export blob to filesystem
            await self.node.export_blob(hash, path)
            logger.info("exported blob to file-system: {}", path)

            # add the file to our file-system aggregate
            self.paths[rel_path] = (meta.operation_timestamp(), hash)

            # record that we just exported a blob to this path
            self.exported_blobs[rel_path] = hash
        else:
            print("received unsupported event type: {}", event.type())


async def main():
    # setup event loop, to ensure async callbacks work
    loop = asyncio.get_running_loop()
    rhio_ffi.uniffi_set_event_loop(loop)

    # parse arguments
    args = parse_args()
    config = config_from_args(args)
    sync_dir = args.sync_dir

    # spawn the rhio node
    node = await Node.spawn(config)
    logger.info("Node ID: {}", node.id())

    # subscribe to a topic, providing a callback method which will be run on each
    # topic event we receive
    topic = TopicId.new_from_str("rhio/file_system_sync")
    file_system_aggregate = FileSystemSync(node, sync_dir)

    logger.info("subscribing to gossip topic: {}", topic)
    sender = await node.subscribe(topic, file_system_aggregate)
    await sender.ready()

    logger.info("gossip topic ready")
    await Watcher(sender, node, file_system_aggregate, sync_dir).watch()


if __name__ == "__main__":
    asyncio.run(main())
