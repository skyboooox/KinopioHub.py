# KinopioHub.py

[简体中文](README_CN.md) · [Wiki](https://github.com/skyboooox/KinopioHub/wiki/Python)

An asyncio SDK for sharing current JSON values across languages and devices. Automatically selects and hosts a shared LAN node; no Node.js runtime is needed.

Python 3.10+. Use the matching source checkout.

## Get started

Run from this repository:

```sh
uv sync --extra dev
uv run python examples/watch.py
# In another terminal:
uv run python examples/basic.py
```

Install this source checkout with `python -m pip install -e /path/to/KinopioHub.py`.

The namespace is read-only; omitting it generates a separate UUID for each instance. To share data, use the same explicit namespace, for example `KinopioHub("demo")`, then access `hub.var("battery")`.

Variables live in SDK memory. A running instance retains state while offline; when the last copy exits, its state disappears.

Installation, API, configuration and test instructions are in the [central manual](https://github.com/skyboooox/KinopioHub/blob/main/docs/python.md) and [Wiki](https://github.com/skyboooox/KinopioHub/wiki/Python).

## Events and requests

Inside a connected Hub, use the same stable reference for state, events and requests:

```python
battery = hub.var("battery")
stop = battery.watch_value(lambda value: print("State:", value))
subscription = await battery.sub(lambda value: print("Event:", value))
responder = await battery.handle(lambda _: battery.get(0))
await battery.set(80)
await battery.pub(81)  # Independent event; local state remains 80.
print(await battery.req())  # Omitted request data is JSON null.
await hub.drain(timeout=5)
stop()
```

`pub/sub/req` alias `publish/subscribe/request`. Message callbacks accept data by default; use `with_context=True` for topic, Headers and explicit replies. Events require an active connection and are not replayed. `request_many` collects bounded replies; `handle` automatically replies with its return value. See [messaging API](https://github.com/skyboooox/KinopioHub/blob/main/docs/python-api.md#messaging) and [the runnable example](examples/messaging.py).

Issues and feedback: [GitHub Issues](https://github.com/skyboooox/KinopioHub.py/issues) · [License](LICENSE)
