import os

from rhio import rhio_ffi, Node, GossipMessageCallback, Config, Message, MessageType
import asyncio
import argparse
from watchfiles import awatch, Change

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
            print(change_type, path)

            # import the blob and get it's hash
            hash = await self.node.import_blob(path)

            # send a file system event to announce a new file was created
            path = self.relative_path(path)
            await self.sender.send(Message.file_system(path, hash))

    async def watch(self):
        async for changes in awatch(self.blobs_dir_path):
            for (change_type, path) in changes:
                await self.handle_change(change_type, path)

class FileSystemSync():
    def __init__(self, cb, node, blobs_dir_path):
        self.cb = cb
        self.node = node
        self.blobs_dir_path = blobs_dir_path

    async def handle_event(self, event, meta):
        (event, meta) = await cb.chan.get()

        if (event.type() == MessageType.FILE_SYSTEM):
            create_event = event.as_file_system_create()

            # download the blob from the network
            await node.download_blob(fs_create_event.hash)

            # get path relative to blobs directory
            blob_file_path = os.path.join(blobs_dir_path, fs_create_event.path)

            # export blob to filesystem
            await node.export_blob(fs_create_event.hash, blob_file_path)
        else:
            print("received unsupported event type: ", event.type())

    async def run(self):
        while (True):
            (msg, meta) = await cb.chan.get()
            await self.handle_event(msg, meta)

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
    print("Node ID: ", node.id())

    # subscribe to a topic, providing a callback method which will be run on each 
    # topic event we receive
    topic = "rhio/file_system_sync"
    cb = Callback("file_system_sync_handler")

    print("subscribing to gossip topic")
    sender = await node.subscribe(topic, cb)

    print("gossip topic ready")

    if config.blobs_path:
        watch_task = asyncio.create_task(
            Watcher(sender, node, config.blobs_path).watch()
        )

        event_handler_task = asyncio.create_task(
            FileSystemSync(cb, node, config.blobs_path).run()
        )

        await watch_task
        await event_handler_task

if __name__ == "__main__":
    asyncio.run(main())
