import rhio

import asyncio

async def main():
    # setup event loop, to ensure async callbacks work
    rhio.rhio_ffi.uniffi_set_event_loop(asyncio.get_running_loop())

    config = rhio.Config()

    node = await rhio.Node.spawn(config)
    print(node.id())

if __name__ == "__main__":
    asyncio.run(main())
