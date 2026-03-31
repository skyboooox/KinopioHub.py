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
| `ws_connection_headers` | `Mapping[str, Sequence[str]]` | `None` | Extra headers for WebSocket transport |
| `name` | `str` | `None` | NATS client name |

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

## Development

See [How_To_Dev.md](./How_To_Dev.md) for development setup and verification steps.

## License

GPL-3.0-or-later
