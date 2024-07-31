import asyncio

from rhio import rhio_ffi, Node, GossipMessageCallback, Config, Message

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
