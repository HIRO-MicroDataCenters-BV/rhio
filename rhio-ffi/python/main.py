import os

import argparse
import asyncio
from loguru import logger
from rhio import rhio_ffi, Node, GossipMessageCallback, Config, Message, MessageType, TopicId
from watchfiles import awatch, Change

class Watcher():
    """A watcher which monitors a directory on the file-system and broadcasts `FileSystem` gossip
    events whenever it is notified that a file has been added"""
    def __init__(self, sender, node, file_system, blobs_dir_path):
        self.sender = sender
        self.node = node
        self.blobs_dir_path = blobs_dir_path
        self.file_system = file_system

    def relative_path(self, path):
        """Get the relative path to a file added to the base directory"""
        return os.path.relpath(path, self.blobs_dir_path)

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
        async for changes in awatch(self.blobs_dir_path):
            for (change_type, path) in changes:
                await self.handle_change(change_type, path)

class FileSystemSync(GossipMessageCallback):
    """Aggregator class which uses last-write-wins logic to maintain a deterministic mapping of
    paths to blob hashes. Consumes FileSystem events and uses the Node api to download blobs from
    the network and export them to the file-system"""
    def __init__(self, node, blobs_dir_path):
        self.node = node
        self.blobs_dir_path = blobs_dir_path
        self.paths = dict()
        self.blobs = set()
        self.exported_blobs = dict()

    async def on_message(self, message, meta):
        """Process FileSystem events"""
        if (message.type() == MessageType.FILE_SYSTEM):
            create_event = message.as_file_system_create()
            rel_path = create_event.path
            hash = create_event.hash

            # check if there is already a file at this path, if there is, compare it's timestamp
            # against the incoming events timestamp and only proceed if it is more recent (we
            # fall-back to comparing hashes if the timestamp is equal)
            timestamp_and_hash = self.paths.get(rel_path)
            if timestamp_and_hash is not None and (meta.operation_timestamp(), hash) < timestamp_and_hash:
                logger.info("ignoring file addition at existing path which contains a lower timestamp: {} {}", meta.operation_timestamp(), hash)
                return

            # download the blob from the network, unless we already have done
            if self.blobs.issuperset([hash]) == False:
                await self.node.download_blob(hash)
                self.blobs.add(hash)
                logger.info("downloaded blob: {}", hash)

            # join the blobs dir and relative file path
            path = os.path.join(self.blobs_dir_path, rel_path)

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
    parser = argparse.ArgumentParser(description='Python Rhio Node')
    parser.add_argument('-p', '--port', type=int, default=2024, help='node bind port')
    parser.add_argument('-t', '--ticket', type=str, action='append', default=[], help='connection ticket string')
    parser.add_argument('-k', '--private-key', type=str, help='path to private key')
    parser.add_argument('-b', '--blobs-path', type=str, help='path to blobs dir')
    parser.add_argument('-r', '--relay', type=str, help='relay addresses')

    args = parser.parse_args()

    # construct node config
    config = Config()
    config.bind_port = args.port
    config.ticket = args.ticket
    config.private_key = args.private_key
    config.blobs_path = args.blobs_path
    config.relay = args.relay

    # spawn the rhio node
    node = await Node.spawn(config)
    logger.info("Node ID: {}", node.id())

    # subscribe to a topic, providing a callback method which will be run on each 
    # topic event we receive
    topic = TopicId.new_from_str("rhio/file_system_sync")
    file_system_aggregate = FileSystemSync(node, config.blobs_path)

    logger.info("subscribing to gossip topic: {}", topic)
    sender = await node.subscribe(topic, file_system_aggregate)
    await sender.ready()

    logger.info("gossip topic ready")
    await Watcher(sender, node, file_system_aggregate, config.blobs_path).watch()

if __name__ == "__main__":
    asyncio.run(main())
