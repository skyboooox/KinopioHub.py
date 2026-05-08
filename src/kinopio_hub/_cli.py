from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict, is_dataclass
from typing import Any, Sequence

from ._leaf_election import AutoLeafHandle, AutoLeafOptions, enable_auto_leaf
from ._leaf_runtime import LeafNodeHandle, LeafNodeOptions, start_leaf_node

_AUTO_STATUS_POLL_SECONDS = 1.0


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    handler = getattr(args, "_handler", None)
    if handler is None:
        parser.print_help()
        return 0
    return int(handler(args))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kinopio-hub",
        description="KinopioHub utilities for local leaf runtime and auto-leaf election.",
    )
    command_parsers = parser.add_subparsers(dest="command")

    leaf_parser = command_parsers.add_parser(
        "leaf",
        help="Start or coordinate a local KinopioHub leaf runtime.",
    )
    leaf_parsers = leaf_parser.add_subparsers(dest="leaf_command", required=True)

    start_parser = leaf_parsers.add_parser(
        "start",
        help="Start a local leaf runtime and keep it running until interrupted.",
    )
    _add_common_leaf_arguments(start_parser)
    start_parser.set_defaults(_handler=_run_leaf_start)

    auto_parser = leaf_parsers.add_parser(
        "auto",
        help="Start auto-leaf discovery and leader election until interrupted.",
    )
    _add_common_leaf_arguments(auto_parser)
    auto_parser.add_argument(
        "--discovery-namespace",
        required=True,
        help="Shared discovery namespace used for LAN election.",
    )
    auto_parser.add_argument(
        "--leader-missing-grace-ms",
        type=int,
        default=10_000,
        help="Grace window before taking over when the current leader disappears.",
    )
    auto_parser.set_defaults(_handler=_run_leaf_auto)

    return parser


def _add_common_leaf_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--backbone-server",
        action="append",
        dest="backbone_servers",
        default=[],
        help="Repeatable upstream NATS or leafnode server URL.",
    )
    parser.add_argument(
        "--binary-path",
        help="Explicit local nats-server binary path.",
    )
    parser.add_argument(
        "--cache-dir",
        help="Override the KinopioHub cache root used for TLS and downloaded binaries.",
    )
    parser.add_argument(
        "--runtime-dir",
        help="Override the runtime working directory. It must be empty when provided.",
    )
    parser.add_argument(
        "--lan-bind-address",
        help="Concrete LAN address or hostname used for WSS/discovery bind and advertisement.",
    )
    parser.add_argument(
        "--client-port",
        type=int,
        help="Override the local TCP client listener port.",
    )
    parser.add_argument(
        "--websocket-port",
        type=int,
        help="Override the secure WebSocket listener port.",
    )
    parser.add_argument(
        "--discovery-port",
        type=int,
        help="Override the HTTP discovery manifest port.",
    )
    parser.add_argument(
        "--monitor-port",
        type=int,
        help="Override the local NATS monitoring port.",
    )
    parser.add_argument(
        "--cert-file",
        help="Use an existing PEM certificate instead of generating a runtime certificate.",
    )
    parser.add_argument(
        "--key-file",
        help="Use an existing PEM private key instead of generating a runtime key.",
    )
    parser.add_argument(
        "--ca-file",
        help=(
            "Use an existing PEM CA certificate for clients. Defaults to the cert path "
            "when certs are supplied."
        ),
    )
    parser.add_argument(
        "--name",
        help="Override the local nats-server name.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON instead of human-friendly text.",
    )


def _run_leaf_start(args: argparse.Namespace) -> int:
    handle = start_leaf_node(_build_leaf_options(args))
    try:
        payload = _leaf_start_payload(handle)
        _emit_snapshot(payload, json_output=bool(args.json))
        _wait_forever()
    except KeyboardInterrupt:
        pass
    finally:
        handle.stop()
    return 0


def _run_leaf_auto(args: argparse.Namespace) -> int:
    handle = enable_auto_leaf(_build_auto_leaf_options(args))
    try:
        _stream_auto_leaf_status(handle, json_output=bool(args.json))
    except KeyboardInterrupt:
        pass
    finally:
        handle.stop()
    return 0


def _build_leaf_options(args: argparse.Namespace) -> LeafNodeOptions:
    lan_bind_address = getattr(args, "lan_bind_address", None)
    backbone_servers = tuple(str(server) for server in args.backbone_servers)
    return LeafNodeOptions(
        backbone_servers=backbone_servers,
        binary_path=args.binary_path,
        cache_dir=args.cache_dir,
        runtime_dir=args.runtime_dir,
        advertised_host=lan_bind_address,
        websocket_host=lan_bind_address,
        discovery_host=lan_bind_address,
        client_port=args.client_port,
        websocket_port=args.websocket_port,
        discovery_port=args.discovery_port,
        monitor_port=args.monitor_port,
        cert_file=args.cert_file,
        key_file=args.key_file,
        ca_file=args.ca_file,
        name=args.name,
    )


def _build_auto_leaf_options(args: argparse.Namespace) -> AutoLeafOptions:
    lan_bind_address = getattr(args, "lan_bind_address", None)
    backbone_servers = tuple(str(server) for server in args.backbone_servers)
    return AutoLeafOptions(
        discovery_namespace=str(args.discovery_namespace),
        backbone_servers=backbone_servers,
        leader_missing_grace_ms=int(args.leader_missing_grace_ms),
        binary_path=args.binary_path,
        cache_dir=args.cache_dir,
        runtime_dir=args.runtime_dir,
        advertised_host=lan_bind_address,
        websocket_host=lan_bind_address,
        discovery_host=lan_bind_address,
        client_port=args.client_port,
        websocket_port=args.websocket_port,
        discovery_port=args.discovery_port,
        monitor_port=args.monitor_port,
        cert_file=args.cert_file,
        key_file=args.key_file,
        ca_file=args.ca_file,
        name=args.name,
    )


def _leaf_start_payload(handle: LeafNodeHandle) -> dict[str, Any]:
    return {
        "mode": "leaf-start",
        "nodeId": handle.node_id,
        "clientUrl": handle.client_url,
        "wssUrl": handle.wss_url,
        "discoveryUrl": handle.discovery_url,
        "monitorUrl": handle.monitor_url,
        "caCertFile": str(handle.ca_cert_file),
        "status": _serialize_payload(handle.status()),
    }


def _auto_leaf_payload(handle: AutoLeafHandle) -> dict[str, Any]:
    return {
        "mode": "leaf-auto",
        "status": _serialize_payload(handle.status()),
    }


def _serialize_payload(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    return value


def _emit_snapshot(payload: dict[str, Any], *, json_output: bool) -> None:
    if json_output:
        print(json.dumps(payload, sort_keys=True), flush=True)
        return

    if payload["mode"] == "leaf-start":
        status = payload["status"]
        print(
            "\n".join(
                [
                    "leaf runtime started",
                    f"node_id: {payload['nodeId']}",
                    f"client_url: {payload['clientUrl']}",
                    f"wss_url: {payload['wssUrl']}",
                    f"discovery_url: {payload['discoveryUrl']}",
                    f"monitor_url: {payload['monitorUrl']}",
                    f"bridge_state: {status['bridge_state']}",
                ]
            ),
            flush=True,
        )
        return

    status = payload["status"]
    leader = status["current_leader"]
    leader_id = leader["nodeId"] if isinstance(leader, dict) and "nodeId" in leader else None
    print(
        "\n".join(
            [
                "auto leaf status changed",
                f"state: {status['state']}",
                f"role: {status['role']}",
                f"node_id: {status['node_id']}",
                f"current_leader: {leader_id}",
            ]
        ),
        flush=True,
    )


def _stream_auto_leaf_status(handle: AutoLeafHandle, *, json_output: bool) -> None:
    last_snapshot: str | None = None
    while True:
        payload = _auto_leaf_payload(handle)
        snapshot = json.dumps(payload, sort_keys=True)
        if snapshot != last_snapshot:
            _emit_snapshot(payload, json_output=json_output)
            last_snapshot = snapshot
        time.sleep(_AUTO_STATUS_POLL_SECONDS)


def _wait_forever() -> None:
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    raise SystemExit(main())
