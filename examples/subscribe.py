from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

from kinopio_hub import KinopioHub


async def main() -> None:
    async with KinopioHub(servers=["nats://demo.nats.io:4222"], debug=True) as hub:
        async def callback(data: Any, _message: Any) -> None:
            from_user = data.get("user")
            msg_content = data.get("message")
            timestamp = data.get("timestamp")

            if from_user and msg_content and timestamp:
                print(
                    "Received message: {\n"
                    f"  from: {from_user},\n"
                    f"  content: {msg_content},\n"
                    f"  time: {timestamp}\n"
                    "}"
                )

            msg_type = data.get("type")
            if msg_type == "warning":
                print("Warning message detected!")

        await hub.chat.messages.subscribe(callback)
        print("Subscription active. Waiting for messages...")
        print("Run publish.py in another terminal")
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
