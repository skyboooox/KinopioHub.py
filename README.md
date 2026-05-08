# KinopioHub

Cloud-native communication middleware for working with remote variables and functions as scoped local objects.

![License: GPL-3.0-or-later](https://img.shields.io/badge/license-GPL--3.0--or--later-green)

[中文](./README_CN.md)

## Features

- Automatic value tracking and local caching
- Hierarchical scope system with dynamic attribute access
- Publish/subscribe and request/reply in one API surface
- Automatic reconnection handling with retry backoff
- Native support for TCP, TLS, WebSocket, and secure WebSocket NATS endpoints
- Pluggable codecs for custom serialization strategies

## Installation

```bash
pip install kinopio-hub
```

The default install now also includes the runtime dependencies used by `kinopio_hub.leaf`.
The local `nats-server` binary is still resolved lazily at runtime and is never downloaded during
`pip install`. The package also installs the `kinopio-hub` console script for local leaf runtime
operations.

## Quick Start

```python
import asyncio

from kinopio_hub import KinopioHub


async def main() -> None:
    hub = KinopioHub(
        servers=["wss://demo.nats.io:8443"],
        debug=True,
    )

    await hub.wait_connected()

    chat_messages = hub.chat.messages

    await chat_messages.publish({"user": "alice", "message": "hello"})

    async def on_message(data, _message) -> None:
        print("Received:", data)

    await chat_messages.subscribe(on_message)


asyncio.run(main())
```

## Core Concepts

### `KinopioHub`

`KinopioHub` manages the NATS connection lifecycle and gives access to scopes.

```python
hub = KinopioHub(
    servers=["wss://demo.nats.io:8443"],
    debug=True,
    no_echo=False,
    reconnect_timeout=5.0,
)
```

### Scopes

Scopes group related variables under a shared subject prefix.

```python
users = hub.get_scope("users")
online = users.get_variable("online")
count = users.get_variable("count")

same_online = hub.users.online
```

### Variables

Variables are subject-backed objects that support publishing, subscribing, request/reply, and service handlers.

```python
await hub.system.health.publish({"cpu": 42.1})

async def on_update(data, _message) -> None:
    print("Updated value:", data)

await hub.system.health.subscribe(on_update)

response = await hub.math.calculator.request({"operation": "add", "a": 3, "b": 4})

await hub.math.calculator.serve(lambda request, _message: {"result": request["a"] + request["b"]})
```

Every variable exposes `value`, which contains the latest known value seen locally.

## Configuration

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `servers` | `Sequence[str]` | `["wss://demo.nats.io:8443", "wss://demo.nats.io:4443"]` | NATS server URLs |
| `debug` | `bool` | `False` | Enable debug logging |
| `no_echo` | `bool` | `False` | Do not receive messages published by the same client |
| `server_selection_mode` | `"ordered" \| "random" \| "latency"` | `None` | Explicit multi-server selection strategy |
| `no_randomize` | `bool` | `True` | Keep the server list order unchanged |
| `max_reconnect_attempts` | `int` | `-1` | Maximum reconnect attempts for the underlying client |
| `wait_on_first_connect` | `bool` | `True` | Start connecting as soon as an event loop is available |
| `reconnect_timeout` | `float` | `5.0` | Initial connection timeout in seconds |
| `reconnect_time_wait` | `float` | `0.5` | Delay between reconnect attempts in seconds |
| `ping_interval` | `int` | `3` | Ping interval for the underlying NATS client |
| `max_ping_out` | `int` | `3` | Maximum unanswered pings before the client reconnects |
| `timeout` | `float` | `3.0` | Default request timeout in seconds |
| `health_report` | `float` | `5.0` | Debug health report interval in seconds |
| `auto_retry` | `bool` | `True` | Retry initial connection failures automatically |
| `retry_delay` | `float` | `1.0` | Initial retry delay for the connection loop |
| `retry_backoff_factor` | `float` | `1.5` | Exponential backoff factor for retries |
| `max_retry_delay` | `float` | `30.0` | Upper bound for retry backoff |
| `codec` | `KinopioCodec` | `None` | Custom codec with `encode()` and `decode()` |
| `json_default` | `Callable` | `None` | Fallback hook for `json.dumps()` |
| `json_object_hook` | `Callable` | `None` | Fallback hook for `json.loads()` |
| `tls` | `ssl.SSLContext` | `None` | Custom TLS context |
| `tls_hostname` | `str` | `None` | Explicit TLS hostname override |
| `tls_handshake_first` | `bool` | `False` | Use a TLS-first handshake for servers configured with `handshake_first: true` |
| `ws_connection_headers` | `Mapping[str, Sequence[str]]` | `None` | Extra headers for WebSocket transport |
| `name` | `str` | `None` | NATS client name |

`no_randomize` is kept for compatibility. If `server_selection_mode` is not set, `no_randomize=True`
maps to `"ordered"` and `no_randomize=False` maps to `"random"`. If
`server_selection_mode` is set explicitly, it takes precedence. `"latency"` mode opens a short-lived
connection to each candidate server, measures a `flush()` round-trip, and then orders candidates by
`healthy first -> lower RTT first -> original input order`. If every probe fails, the original
server order is preserved and the normal connection error flow continues. After connecting in
`"latency"` mode with multiple candidates, the client re-probes every 10 minutes and hot-switches
only when another healthy server is at least 30ms faster. During the brief double-subscription
window of a hot switch, a small number of duplicate callback deliveries is possible, but logical
subscriptions, services, and `value` tracking continue to be maintained.

If your NATS servers are configured with TLS-first handshakes (`tls.handshake_first: true` on the
server side), also set `tls_handshake_first=True`. Those endpoints do not send the initial `INFO`
line in clear text before the TLS upgrade.

## Connection Management

```python
await hub.wait_connected()
await hub.reconnect()
await hub.aclose()
```

To track connection state:

```python
from kinopio_hub import ConnectionState


def handle_state(state: ConnectionState) -> None:
    print("state:", state.value)


stop = hub.on_state_change(handle_state)
stop()
```

## Local Leaf Runtime

Stage 4 adds a separate `kinopio_hub.leaf` module for manually starting a local leaf runtime
without changing the main `KinopioHub` export surface.

```python
import ssl

from kinopio_hub import KinopioHub
from kinopio_hub.leaf import LeafNodeOptions, start_leaf_node


with start_leaf_node(
    LeafNodeOptions(
        backbone_servers=("nats://127.0.0.1:7422",),
    )
) as leaf:
    print(leaf.client_url)
    print(leaf.wss_url)
    print(leaf.discovery_url)
    print(leaf.monitor_url)
    print(leaf.status().bridge_state)

    tls_context = ssl.create_default_context(cafile=str(leaf.ca_cert_file))
    # Local TCP listener stays on loopback; WSS and discovery bind to the LAN address by default.
    hub = KinopioHub(servers=[leaf.wss_url], tls=tls_context)
```

`start_leaf_node()` returns a `LeafNodeHandle` with `status()` and `stop()` plus the stable URLs
`client_url`, `wss_url`, `discovery_url`, and `monitor_url`. Binary resolution happens in this
order: explicit `binary_path`, system `PATH`, an existing user cache entry, then the latest stable
official `nats-server` release. If you do not provide PEM files, KinopioHub generates a local CA
and a runtime leaf certificate automatically, but it does not modify the system trust store for
you. When `backbone_servers` is configured but unavailable, the local node still starts and reports
`bridge_state == "connecting"` while remaining usable for local traffic.

The default cache root is `~/Library/Caches/kinopio-hub` on macOS,
`%LOCALAPPDATA%\kinopio-hub` on Windows, and `$XDG_CACHE_HOME/kinopio-hub` or
`~/.cache/kinopio-hub` on Linux. Set `KINOPIO_HUB_CACHE_DIR` to override it. The default install
already includes the Python dependencies used by the leaf runtime, so there is no separate `leaf`
extra to install. The local leaf runtime is intended for environments that can spawn a local
`nats-server` subprocess and open local TCP or UDP sockets; restricted browser and serverless
runtimes are outside this support boundary.

The manual leaf discovery manifest also includes JS-compatible lease metadata such as
`expiresAt`, `leaseExpiresAt`, `leaderEpoch`, `isLeader`, and `candidateRole`, so browser-side
consumers can normalize it without requiring the auto-election runtime.

## Auto Leaf

Stage 5 adds `enable_auto_leaf()` on top of the manual runtime. It uses UDP multicast heartbeats as
the base coordination layer, optionally advertises the leader via mDNS when `zeroconf` is
available, keeps a stable `node_id` under the KinopioHub user cache, and exposes a high-level
status API for leader discovery and failover.

```python
import time

from kinopio_hub.leaf import AutoLeafOptions, enable_auto_leaf


handle = enable_auto_leaf(
    AutoLeafOptions(
        discovery_namespace="studio-a",
        backbone_servers=("nats://127.0.0.1:7422",),
        leader_missing_grace_ms=10_000,
    )
)

try:
    while True:
        status = handle.status()
        print(status.state, status.role, status.current_leader)
        time.sleep(1)
finally:
    handle.stop()
```

The public state machine is:

- `discovering`
- `following-leader`
- `leader-missing-grace`
- `electing`
- `starting-leaf`
- `leader`
- `stopped`

`AutoLeafHandle` also exposes `state()`, `role()`, `current_leader()`, `status()`, and `stop()`.
Leader discovery manifests now include the JS-facing fields `version`, `expiresAt`,
`leaderEpoch`, `advertisedHostname`, `wssUrl`, `fallbackServers`, `backboneRttMs`,
`discoveryUrl`, `leaseExpiresAt`, `nodeId`, `discoveryNamespace`, `isLeader`, and
`candidateRole`. A healthier leader will not be preempted immediately; another node must stay at
least 50ms ahead in measured backbone RTT for a sustained window before it can take over.
KinopioHub itself does not implement the browser-side local probe used by the JS package, but it
does emit a compatible discovery manifest that browser clients can consume.

## CLI

Stage 6 also exposes a `kinopio-hub` console script for the two public leaf workflows:

```bash
kinopio-hub leaf start \
  --backbone-server nats://127.0.0.1:7422 \
  --lan-bind-address 192.168.1.20 \
  --json

kinopio-hub leaf auto \
  --discovery-namespace studio-a \
  --backbone-server nats://127.0.0.1:7422
```

Both commands support `--backbone-server` (repeatable), `--binary-path`, `--lan-bind-address`,
`--client-port`, `--websocket-port`, `--discovery-port`, `--monitor-port`, and `--json`.
`kinopio-hub leaf auto` also adds `--discovery-namespace` and `--leader-missing-grace-ms`.
`--lan-bind-address` maps to the advertised WSS and discovery address; the local TCP client and
monitor listeners still stay on loopback unless you use the Python API to override them explicitly.
`--json` emits machine-readable startup or status snapshots for automation and shell tooling.

## Serialization

The default serialization rules are:

- `None` becomes an empty payload
- `bytes`, `bytearray`, and `memoryview` are passed through as bytes
- `str` is encoded as UTF-8
- everything else is serialized as JSON

If a custom codec is supplied, its `encode()` and `decode()` methods are used first.

## Service Errors

If a service handler raises an exception, KinopioHub sends a response payload shaped like this:

```python
{"error": True, "message": "details"}
```

This keeps request/reply flows explicit and observable on the wire.

## Examples

See the [examples](./examples) directory for complete runnable programs:

- [connection.py](./examples/connection.py)
- [publish.py](./examples/publish.py)
- [subscribe.py](./examples/subscribe.py)
- [request_reply.py](./examples/request_reply.py)
- [scope.py](./examples/scope.py)
- [leaf_runtime.py](./examples/leaf_runtime.py)
- [auto_leaf.py](./examples/auto_leaf.py)

## Development

See [How_To_Dev.md](./How_To_Dev.md) for development setup and verification steps.

## License

GPL-3.0-or-later
