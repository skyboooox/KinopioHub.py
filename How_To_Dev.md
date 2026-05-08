# KinopioHub Development Guide

This document describes the local development workflow for KinopioHub.

## Requirements

- Python 3.10 or newer
- Docker or a local `nats-server` binary for integration tests
- The default install now also includes `cryptography` and `zeroconf` because later leaf-runtime
  phases depend on them
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
- Auto-leaf tests also exercise the phase-5 UDP heartbeat election path, leader handoff, and
  manifest compatibility fields on top of the local leaf runtime.
- `tests/test_leaf.py` and `tests/test_auto_leaf.py` are marked with `integration` and `slow` to
  make the heavier runtime coverage easy to target.
- Real NATS behavior tests in `tests/test_hub.py` and `tests/test_bug_fixes.py` are marked with
  `integration`; cluster hot-switch and leaf-runtime flows are additionally marked as `slow`.

## Phase 7 Interop Notes

- The fixed remote TLS-first servers currently used for cross-implementation checks are
  `home.skyboooox.com:14222`, `hf.skyboooox.com:14222`, and `hub.skyboooox.com:14222`.
- Python must use `tls_handshake_first=True` for these endpoints. Without it, `nats-py` waits for a
  clear-text `INFO` line and times out against servers configured with `tls.handshake_first: true`.
- As of `2026-05-08`, raw TLS probes from this repo could reach `home` and `hub`, while
  `hf.skyboooox.com:14222` timed out during the TLS handshake. Treat that host as a server /
  network-path volatility signal, not an automatic Python client regression.
- The remote WSS entrypoint for the same cluster is `:55588`. As of `2026-05-08`, both Python and
  JS could use the fixed WSS set `wss://home.skyboooox.com:55588`,
  `wss://hf.skyboooox.com:55588`, and `wss://hub.skyboooox.com:55588` in multi-server mode, with
  `home` and `hub` reachable and `hf` still timing out.
- Python now ships a reusable remote WSS regression script at
  `scripts/phase7_remote_wss_smoke.py`. It covers `ordered` / `random` / `latency` server
  selection, remote pub/sub, request/reply, and a reconnect-after-rebind verification pass.
- Python also now applies an extra initial-connect recovery step for multi-server WSS candidate
  lists. If the first shuffled candidate is a dead node like `hf`, the library times out that
  attempt quickly, probes the remaining WSS candidates individually, rotates the first healthy one
  to the front, and then reconnects with the full ordered fallback list.
- `KinopioHub.JS` still uses `@nats-io/nats-core` `wsconnect()` in `kinopio.mjs`, so raw remote
  `nats://...:14222` TLS-first interop remains a JS transport limitation. For current phase-7
  remote cross-implementation checks, use the WSS `:55588` endpoints instead.
- Remote Python <-> JS pub/sub and request/reply interop over the WSS server set now works. The
  latest verified path selected `home.skyboooox.com:55588`, with fallback ordering
  `home -> hub -> hf`.
- Python leaf runtime and JS leaf runtime can still be cross-checked locally over WSS:
  - Python leaf -> JS client: start `start_leaf_node()`, trust `leaf.ca_cert_file` via
    `NODE_EXTRA_CA_CERTS`, then point the JS client at `leaf.wss_url`.
  - JS leaf -> Python client: start `startLeafNode()`, read `leaf.status().tls.caCertFile`, build a
    Python `ssl.create_default_context(cafile=...)`, then point `KinopioHub` at the JS `wssUrl`.
- Python manual leaf manifests now include JS-facing compatibility fields such as `expiresAt`,
  `leaseExpiresAt`, `leaderEpoch`, `isLeader`, and `candidateRole`. This keeps the manifest shape
  aligned with the JS-side normalization logic even without auto-leaf election.

## Notes

- Keep public APIs Pythonic: `get_scope()`, `get_variable()`, `publish()`, `subscribe()`, `request()`, `serve()`, `wait_connected()`, `reconnect()`, `aclose()`.
- The stage-4 leaf runtime lives under `kinopio_hub.leaf`; avoid changing root exports unless the
  user explicitly asks for that compatibility decision.
- Stage 5 adds `enable_auto_leaf()` in the same submodule; treat the election state machine,
  persisted `node_id`, and JS-facing manifest fields as public behavior.
- Stage 6 adds the `kinopio-hub` console script. Keep its option names in sync with both READMEs
  and the underlying `LeafNodeOptions` / `AutoLeafOptions` fields.
- Cache, TLS, and binary download behavior now matter for user-facing docs:
  `KINOPIO_HUB_CACHE_DIR`, auto-generated CA files, and lazy `nats-server` download should stay
  aligned across code and documentation.
- Keep examples and docs aligned with the shipped API.
- Prefer adding behavior tests before changing reconnection, serialization, or service semantics.
