# KinopioHub Development Guide

This document describes the local development workflow for KinopioHub.

## Requirements

- Python 3.10 or newer
- Docker or a local `nats-server` binary for integration tests

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

## Test Strategy

- Tests run against a real NATS server.
- If `nats-server` is installed locally, the suite uses it directly.
- Otherwise the suite starts a disposable Docker container with TCP and WebSocket listeners enabled.

## Notes

- Keep public APIs Pythonic: `get_scope()`, `get_variable()`, `publish()`, `subscribe()`, `request()`, `serve()`, `wait_connected()`, `reconnect()`, `aclose()`.
- Keep examples and docs aligned with the shipped API.
- Prefer adding behavior tests before changing reconnection, serialization, or service semantics.
