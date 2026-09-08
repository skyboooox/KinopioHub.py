import os
from typing import Any


def options(name: str) -> dict[str, Any]:
    result: dict[str, Any] = {"name": f"example-{name}"}
    if endpoints := os.getenv("KINOPIO_EXAMPLE_SERVERS"):
        result["servers"] = [url.strip() for url in endpoints.split(",") if url.strip()]
    if endpoints := os.getenv("KINOPIO_LEAF_SERVERS"):
        result["mesh"] = {"upstreams": [url.strip() for url in endpoints.split(",") if url.strip()]}
    if os.getenv("KINOPIO_TOKEN"):
        result["token"] = os.environ["KINOPIO_TOKEN"]
    if os.getenv("KINOPIO_EXAMPLE_TLS_FIRST") == "1":
        result["tls"] = {"handshake_first": True}
    if os.getenv("KINOPIO_MESH") == "0":
        result["mesh"] = False
    return result
