from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from kinopio_hub._cli import main
from kinopio_hub._leaf_runtime import LeafNodeListenerStatus, LeafNodeStatus
from kinopio_hub.leaf import AutoLeafStatus


class FakeLeafHandle:
    def __init__(self) -> None:
        self.node_id = "leaf-node-1234"
        self.client_url = "nats://127.0.0.1:4222"
        self.wss_url = "wss://192.168.1.8:9222"
        self.discovery_url = "http://192.168.1.8:8022/manifest.json"
        self.monitor_url = "http://127.0.0.1:8222"
        self.ca_cert_file = Path("/tmp/leaf-ca-cert.pem")
        self.stopped = False

    def status(self) -> LeafNodeStatus:
        return LeafNodeStatus(
            client=LeafNodeListenerStatus(
                url=self.client_url,
                host="127.0.0.1",
                port=4222,
                ready=True,
            ),
            websocket=LeafNodeListenerStatus(
                url=self.wss_url,
                host="192.168.1.8",
                port=9222,
                ready=True,
            ),
            discovery=LeafNodeListenerStatus(
                url=self.discovery_url,
                host="192.168.1.8",
                port=8022,
                ready=True,
            ),
            monitor=LeafNodeListenerStatus(
                url=self.monitor_url,
                host="127.0.0.1",
                port=8222,
                ready=True,
            ),
            bridge_state="connected",
            backbone_servers=("nats://127.0.0.1:7422",),
            server_ready=True,
            process_id=3210,
        )

    def stop(self) -> None:
        self.stopped = True


class FakeAutoLeafHandle:
    def __init__(self) -> None:
        self.stopped = False

    def status(self) -> AutoLeafStatus:
        return AutoLeafStatus(
            state="following-leader",
            role="follower",
            node_id="auto-node-5678",
            discovery_namespace="studio-a",
            current_leader={
                "nodeId": "leader-1234",
                "discoveryUrl": "http://192.168.1.9:8022/manifest.json",
            },
            backbone_rtt_ms=14.5,
            leaf_status=None,
            mdns_enabled=True,
        )

    def stop(self) -> None:
        self.stopped = True


def test_cli_leaf_start_invokes_runtime_and_prints_json(
    monkeypatch: Any,
    capsys: Any,
) -> None:
    fake_handle = FakeLeafHandle()
    captured: dict[str, Any] = {}

    def fake_start(options: Any) -> FakeLeafHandle:
        captured["options"] = options
        return fake_handle

    def stop_immediately() -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr("kinopio_hub._cli.start_leaf_node", fake_start)
    monkeypatch.setattr("kinopio_hub._cli._wait_forever", stop_immediately)

    exit_code = main(
        [
            "leaf",
            "start",
            "--backbone-server",
            "nats://127.0.0.1:7422",
            "--backbone-server",
            "nats://127.0.0.1:7423",
            "--lan-bind-address",
            "192.168.1.8",
            "--client-port",
            "4222",
            "--websocket-port",
            "9222",
            "--discovery-port",
            "8022",
            "--monitor-port",
            "8222",
            "--json",
        ]
    )

    assert exit_code == 0
    assert fake_handle.stopped is True
    options = captured["options"]
    assert options.backbone_servers == (
        "nats://127.0.0.1:7422",
        "nats://127.0.0.1:7423",
    )
    assert options.advertised_host == "192.168.1.8"
    assert options.websocket_host == "192.168.1.8"
    assert options.discovery_host == "192.168.1.8"
    assert options.client_port == 4222
    assert options.websocket_port == 9222
    assert options.discovery_port == 8022
    assert options.monitor_port == 8222

    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "leaf-start"
    assert payload["clientUrl"] == fake_handle.client_url
    assert payload["wssUrl"] == fake_handle.wss_url
    assert payload["status"]["bridge_state"] == "connected"


def test_cli_leaf_auto_invokes_election_and_streams_json(
    monkeypatch: Any,
    capsys: Any,
) -> None:
    fake_handle = FakeAutoLeafHandle()
    captured: dict[str, Any] = {}

    def fake_auto_leaf(options: Any) -> FakeAutoLeafHandle:
        captured["options"] = options
        return fake_handle

    sleep_calls = {"count": 0}

    def fake_sleep(_: float) -> None:
        sleep_calls["count"] += 1
        raise KeyboardInterrupt

    monkeypatch.setattr("kinopio_hub._cli.enable_auto_leaf", fake_auto_leaf)
    monkeypatch.setattr("kinopio_hub._cli.time.sleep", fake_sleep)

    exit_code = main(
        [
            "leaf",
            "auto",
            "--discovery-namespace",
            "studio-a",
            "--backbone-server",
            "nats://127.0.0.1:7422",
            "--lan-bind-address",
            "192.168.1.8",
            "--leader-missing-grace-ms",
            "250",
            "--json",
        ]
    )

    assert exit_code == 0
    assert sleep_calls["count"] == 1
    assert fake_handle.stopped is True
    options = captured["options"]
    assert options.discovery_namespace == "studio-a"
    assert options.backbone_servers == ("nats://127.0.0.1:7422",)
    assert options.advertised_host == "192.168.1.8"
    assert options.websocket_host == "192.168.1.8"
    assert options.discovery_host == "192.168.1.8"
    assert options.leader_missing_grace_ms == 250

    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "leaf-auto"
    assert payload["status"]["state"] == "following-leader"
    assert payload["status"]["current_leader"]["nodeId"] == "leader-1234"
