import asyncio
from kinopio_hub import KinopioHub
from _options import options


async def main() -> None:
    async with KinopioHub(**options("status")) as hub:
        hub.watch(lambda status: print(status["connection"], status["mesh"]))
        print("SDK reports are automatic. Press Ctrl+C to stop.")
        await asyncio.Event().wait()


try:
    asyncio.run(main())
except KeyboardInterrupt:
    pass
