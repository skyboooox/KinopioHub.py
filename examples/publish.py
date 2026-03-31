from __future__ import annotations

import asyncio
from time import time

from kinopio_hub import KinopioHub


async def main() -> None:
    async with KinopioHub(servers=["nats://demo.nats.io:4222"], debug=True) as hub:
        messages = hub.chat.messages

        # Message 1: Simple message (matches JS example)
        await messages.publish({"user": "Alice", "message": "Hello World!", "timestamp": int(time() * 1000)})
        print("Published message 1")

        # Message 2: Another message
        await messages.publish({"user": "Bob", "message": "How are you?", "timestamp": int(time() * 1000)})
        print("Published message 2")

        # Message 3: System message with extra fields
        await messages.publish({
            "user": "System",
            "message": "Server restart in 5 minutes",
            "type": "warning",
            "priority": "high",
            "timestamp": int(time() * 1000)
        })
        print("Published system message")

        print("All messages published successfully!")


if __name__ == "__main__":
    asyncio.run(main())
