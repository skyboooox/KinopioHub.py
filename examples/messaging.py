"""State, events and request/reply on the same stable reference."""
import asyncio

from kinopio_hub import KinopioHub

from _options import options


async def main():
    async with KinopioHub(**options()) as hub:
        await hub.connected()
        battery = hub.var('battery')
        print('Local value:', battery.get(0))
        stop = battery.watch_value(lambda value: print('State:', value))
        subscription = await battery.sub(lambda value: print('Event:', value))
        responder = await battery.handle(lambda _: battery.get(0))
        await battery.set(80)
        await battery.pub(81)
        print('Reply:', await battery.req())
        await subscription.drain()
        await responder.drain()
        stop()
        await hub.drain()


if __name__ == '__main__':
    asyncio.run(main())
