# KinopioHub.py

Project version: **3.0.0** (not yet released).

[简体中文](README_CN.md) · [Wiki](https://github.com/skyboooox/KinopioHub/wiki/Python)

An asyncio SDK for sharing current JSON values across languages and devices. Automatically selects and hosts a shared LAN node; no Node.js runtime is needed.

Python 3.10+. This checkout is the unpublished v3 rewrite; install from source.

## Get started

Run from this repository:

```sh
uv sync --extra dev
uv run python examples/watch.py
# In another terminal:
uv run python examples/basic.py
```

Variables live in SDK memory. A running instance retains state while offline; when the last copy exits, its state disappears.

Installation, API, configuration and test instructions are in the [Wiki](https://github.com/skyboooox/KinopioHub/wiki/Python).

Issues and feedback：[GitHub Issues](https://github.com/skyboooox/KinopioHub.py/issues) · [License](LICENSE)
