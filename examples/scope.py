from __future__ import annotations

import asyncio
from time import time

from kinopio_hub import KinopioHub


async def main() -> None:
    async with KinopioHub(servers=["nats://demo.nats.io:4222"], debug=True) as hub:
        # Method 1: Using get_scope
        user_scope = hub.get_scope("users")
        online_users = user_scope.get_variable("online")
        user_count = user_scope.get_variable("count")

        # Publish to user scope
        await online_users.publish(["Alice", "Bob", "Charlie"])
        print("Published online users")

        await user_count.publish({
            "total": 150,
            "online": 3,
            "registered_today": 5
        })
        print("Published user count")

        # Method 2: Direct scope.variable access
        await hub.chat.messages.publish({
            "room": "general",
            "user": "Alice",
            "message": "Hello everyone!",
            "timestamp": int(time() * 1000)
        })
        print("Published chat message")

        await hub.system.health.publish({
            "cpu_usage": 45.2,
            "memory_usage": 68.1,
            "disk_usage": 23.8,
            "status": "healthy",
            "last_check": int(time() * 1000)
        })
        print("Published system health")

        await hub.system.logs.publish({
            "level": "error",
            "message": "Failed to connect to database",
            "service": "user-service",
            "timestamp": int(time() * 1000),
            "stack_trace": "Error: Connection timeout..."
        })
        print("Published system log")

        print("All data published successfully!")


if __name__ == "__main__":
    asyncio.run(main())
