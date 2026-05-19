# KinopioHub Development Guide

This document describes the local development workflow for KinopioHub.

## Requirements

- Python 3.10 or newer
- Docker or a local `nats-server` binary for integration tests
- The default install includes `cryptography` and `zeroconf` because the leaf runtime depends on
  them
- Installing the package also exposes the `kinopio-hub` console script

## Environment Setup

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -U pip
pip install -e '.[dev]'
```

## Project Layout

```text
src/kinopio_hub/   package source
tests/             integration and behavior tests
examples/          runnable examples
README.md          English documentation
README_CN.md       Chinese documentation
```

## Running Checks

```bash
. .venv/bin/activate
ruff check .
mypy .
pytest
```

For a CLI smoke check after installation, `kinopio-hub --help` should list the `leaf start` and
`leaf auto` subcommands.

For quicker local loops, `pytest -m "not slow"` skips the heavier leaf-runtime and hot-switch
coverage, while `pytest -m integration` focuses on tests that exercise real NATS processes.

## Test Strategy

- Tests run against a real NATS server.
- If `nats-server` is installed locally, the suite uses it directly.
- Otherwise the suite first tries the shared `nats-server` downloader used by the leaf runtime, and
  only falls back to Docker if a local binary still is not available.
- `tests/conftest.py` separates the main real-topology fixtures into `nats_server`,
  `nats_server_pool`, `nats_cluster`, `nats_leaf_backbone`, `leaf_node_factory`,
  `auto_leaf_factory`, and `nats_server_binary`.
- Leaf runtime tests start a real local `nats-server` process, generate temporary config and TLS
  materials, and expose monitor / discovery endpoints in addition to the client listener.
- Auto-leaf tests also exercise the UDP heartbeat election path, leader handoff, and manifest
  compatibility fields on top of the local leaf runtime.
- `tests/test_leaf.py` and `tests/test_auto_leaf.py` are marked with `integration` and `slow` to
  make the heavier runtime coverage easy to target.
- Real NATS behavior tests in `tests/test_hub.py` and `tests/test_bug_fixes.py` are marked with
  `integration`; cluster hot-switch and leaf-runtime flows are additionally marked as `slow`.

## Notes

- Keep public APIs Pythonic: `get_scope()`, `get_variable()`, `publish()`, `subscribe()`, `request()`, `serve()`, `wait_connected()`, `reconnect()`, `aclose()`.
- The leaf runtime lives under `kinopio_hub.leaf`; avoid changing root exports unless the user
  explicitly asks for that compatibility decision.
- `enable_auto_leaf()` lives in the same submodule; treat the election state machine, persisted
  `node_id`, and JS-facing manifest fields as public behavior.
- Keep `kinopio-hub` console script option names in sync with both READMEs and the underlying
  `LeafNodeOptions` / `AutoLeafOptions` fields.
- Cache, TLS, and binary download behavior are user-facing docs concerns:
  `KINOPIO_HUB_CACHE_DIR`, auto-generated CA files, and lazy `nats-server` download should stay
  aligned across code and documentation.
- Keep mixed JS/Python transport examples explicit: KinopioHub.JS uses WebSocket or WSS endpoints,
  while this Python package uses native NATS TCP or TCP/TLS endpoints.
- Keep examples and docs aligned with the shipped API.
- Prefer adding behavior tests before changing reconnection, serialization, or service semantics.
