from __future__ import annotations

import asyncio
from time import time
from typing import Any

from kinopio_hub import KinopioHub


async def main() -> None:
    async with KinopioHub(servers=["nats://demo.nats.io:4222"], debug=True) as hub:
        async def handler(request: dict[str, Any], _message: Any) -> dict[str, Any]:
            operation = request["operation"]
            a = request["a"]
            b = request["b"]
            result = None

            if operation == "add":
                result = a + b
            elif operation == "subtract":
                result = a - b
            elif operation == "multiply":
                result = a * b
            elif operation == "divide":
                result = a / b if b else "Error: Division by zero"
            else:
                raise ValueError(f"Unknown operation: {operation}")

            return {
                "result": result,
                "operation": operation,
                "inputs": {"a": a, "b": b},
                "timestamp": int(time() * 1000),
            }

        await hub.math.calculator.serve(handler)
        await asyncio.sleep(1)
        print(
            "Addition result:",
            await hub.math.calculator.request({"operation": "add", "a": 10, "b": 5}),
        )
        print(
            "Multiplication result:",
            await hub.math.calculator.request({"operation": "multiply", "a": 7, "b": 3}),
        )
        print(
            "Division result:",
            await hub.math.calculator.request({"operation": "divide", "a": 20, "b": 4}),
        )

        try:
            await hub.math.calculator.request({"operation": "invalid", "a": 1, "b": 2})
        except Exception as e:
            print("Expected error:", e)


if __name__ == "__main__":
    asyncio.run(main())
