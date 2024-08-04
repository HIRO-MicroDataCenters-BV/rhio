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

class Watcher():
    def __init__(self, sender, node, blobs_dir_path):
        self.sender = sender
        self.node = node
        self.blobs_dir_path = blobs_dir_path

    def relative_path(self, path):
        return os.path.relpath(path, self.blobs_dir_path)

    async def handle_change(self, change_type, path):
        # we only handle "added" events
        if change_type == 1:
            # calculate path of blob relative to blobs dir
            rel_path = self.relative_path(path)

            # we don't want to import blobs we just exported, so catch this case here
            if rel_path in EXPORTED_BLOBS:
                EXPORTED_BLOBS.pop(rel_path)
                return

            logger.info("new file added: {}", path)

            # import the blob and get it's hash
            hash = await self.node.import_blob(path)
            logger.info("blob imported: {}", hash)

            # send a file system event to announce a new file was created
            msg = Message.file_system(rel_path, hash)
            await self.sender.send(msg)

    async def watch(self):
        async for changes in awatch(self.blobs_dir_path):
            for (change_type, path) in changes:
                await self.handle_change(change_type, path)

class FileSystemSync():
    def __init__(self, cb, node, blobs_dir_path):
        self.cb = cb
        self.node = node
        self.blobs_dir_path = blobs_dir_path

    async def handle_event(self, message, meta):
        if (message.type() == MessageType.FILE_SYSTEM):
            create_event = message.as_file_system_create()
            rel_path = create_event.path
            hash = create_event.hash

            # download the blob from the network
            await self.node.download_blob(hash)
            logger.info("downloaded blob: {}", hash)

            # join the blobs dir and relative file path
            path = os.path.join(self.blobs_dir_path, rel_path)

            # export blob to filesystem
            await self.node.export_blob(hash, path)
            logger.info("exported blob to file-system: {}", path)

            # record that we just exported a blob to this path
            EXPORTED_BLOBS[rel_path] = hash
        else:
            print("received unsupported event type: {}", event.type())

    async def run(self):
        while (True):
            (message, meta) = await self.cb.chan.get()
            await self.handle_event(message, meta)

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
    topic = TopicId.new_from_str("rhio/file_system_sync")
    cb = Callback("file_system_sync_handler")

    logger.info("subscribing to gossip topic: {}", topic)
    sender = await node.subscribe(topic, cb)
    await sender.ready()

    logger.info("gossip topic ready")

    if config.blobs_path:
        watch_task = asyncio.create_task(
            Watcher(sender, node, config.blobs_path).watch()
        )

        sync_task = asyncio.create_task(
            FileSystemSync(cb, node, config.blobs_path).run()
        )

        await watch_task
        await sync_task

if __name__ == "__main__":
    asyncio.run(main())
