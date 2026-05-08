from __future__ import annotations

import time

from kinopio_hub.leaf import AutoLeafOptions, enable_auto_leaf

# Equivalent CLI: kinopio-hub leaf auto --discovery-namespace demo-room --backbone-server nats://127.0.0.1:7422
handle = enable_auto_leaf(
    AutoLeafOptions(
        discovery_namespace="demo-room",
        backbone_servers=("nats://127.0.0.1:7422",),
    )
)

try:
    for _ in range(20):
        status = handle.status()
        print(
            status.state,
            status.role,
            status.current_leader["nodeId"] if status.current_leader else None,
        )
        time.sleep(1)
finally:
    handle.stop()
