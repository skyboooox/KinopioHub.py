import asyncio
from kinopio_hub import KinopioHub
from _options import options


async def main() -> None:
    async with KinopioHub(**options("watch")) as hub:
        hub.scope("devices").var("battery").watch(lambda value, meta: print("battery:", value))
        print("Run basic.py in another terminal. Press Ctrl+C to stop.")
        await asyncio.Event().wait()


try:
    asyncio.run(main())
except KeyboardInterrupt:
    pass
