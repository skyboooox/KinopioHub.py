from __future__ import annotations

import json
import socket
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.request import urlopen

import pytest

from kinopio_hub.leaf import AutoLeafOptions

pytestmark = [pytest.mark.integration, pytest.mark.slow]


def unused_nats_url() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        port = int(sock.getsockname()[1])
    return f"nats://127.0.0.1:{port}"


def wait_for(predicate: Any, *, timeout: float = 6.0, interval: float = 0.05) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError("condition was not met in time")


@pytest.fixture(autouse=True)
def fast_auto_leaf_timers(monkeypatch: Any) -> None:
    monkeypatch.setattr("kinopio_hub._leaf_election._HEARTBEAT_INTERVAL_SECONDS", 0.1)
    monkeypatch.setattr("kinopio_hub._leaf_election._HEARTBEAT_SOCKET_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr("kinopio_hub._leaf_election._LEASE_DURATION_SECONDS", 0.4)
    monkeypatch.setattr("kinopio_hub._leaf_election._DISCOVERY_SETTLE_SECONDS", 0.15)
    monkeypatch.setattr("kinopio_hub._leaf_election._LEADERSHIP_ADVANTAGE_WINDOW_SECONDS", 0.5)
    monkeypatch.setattr("kinopio_hub._leaf_election._BACKBONE_RTT_REPROBE_SECONDS", 0.2)


@pytest.mark.parametrize("leader_rtt,follower_rtt", [(12.0, None)])
def test_auto_leaf_elects_single_leader_and_emits_js_friendly_manifest(
    auto_leaf_pair_factory: Any,
    monkeypatch: Any,
    nats_server_binary: str,
    tmp_path: Path,
    leader_rtt: float | None,
    follower_rtt: float | None,
) -> None:
    leader_backbone = unused_nats_url()
    follower_backbone = unused_nats_url()
    rtt_map = {
        (leader_backbone,): leader_rtt,
        (follower_backbone,): follower_rtt,
    }
    monkeypatch.setattr(
        "kinopio_hub._leaf_election._probe_backbone_rtt_ms",
        lambda servers: rtt_map.get(tuple(servers)),
    )

    namespace = f"phase5-{uuid.uuid4().hex[:8]}"
    leader_options = AutoLeafOptions(
        discovery_namespace=namespace,
        backbone_servers=(leader_backbone,),
        binary_path=nats_server_binary,
        cache_dir=tmp_path / "leader-cache",
        runtime_dir=tmp_path / "leader-runtime",
        leader_missing_grace_ms=400,
    )
    follower_options = AutoLeafOptions(
        discovery_namespace=namespace,
        backbone_servers=(follower_backbone,),
        binary_path=nats_server_binary,
        cache_dir=tmp_path / "follower-cache",
        runtime_dir=tmp_path / "follower-runtime",
        leader_missing_grace_ms=400,
    )
    leader, follower = auto_leaf_pair_factory(leader_options, follower_options)

    wait_for(lambda: leader.role() == "leader" and follower.role() == "follower")

    leader_status = leader.status()
    follower_status = follower.status()
    assert leader_status.leaf_status is not None
    assert follower_status.current_leader is not None
    assert follower_status.current_leader["nodeId"] == leader_status.node_id

    with urlopen(follower_status.current_leader["discoveryUrl"], timeout=2.0) as response:
        manifest = json.load(response)

    required_fields = {
        "version",
        "expiresAt",
        "leaderEpoch",
        "advertisedHostname",
        "wssUrl",
        "fallbackServers",
        "backboneRttMs",
        "discoveryUrl",
        "leaseExpiresAt",
        "nodeId",
        "discoveryNamespace",
        "isLeader",
        "candidateRole",
    }
    assert required_fields.issubset(manifest)
    assert manifest["nodeId"] == leader_status.node_id
    assert manifest["discoveryNamespace"] == namespace
    assert manifest["isLeader"] is True
    assert manifest["candidateRole"] == "leader"


def test_auto_leaf_follower_takes_over_after_leader_missing_grace(
    auto_leaf_pair_factory: Any,
    monkeypatch: Any,
    nats_server_binary: str,
    tmp_path: Path,
) -> None:
    leader_backbone = unused_nats_url()
    follower_backbone = unused_nats_url()
    monkeypatch.setattr(
        "kinopio_hub._leaf_election._probe_backbone_rtt_ms",
        lambda servers: 10.0 if tuple(servers) == (leader_backbone,) else 20.0,
    )

    namespace = f"phase5-takeover-{uuid.uuid4().hex[:8]}"
    leader_options = AutoLeafOptions(
        discovery_namespace=namespace,
        backbone_servers=(leader_backbone,),
        binary_path=nats_server_binary,
        cache_dir=tmp_path / "leader-cache",
        runtime_dir=tmp_path / "leader-runtime",
        leader_missing_grace_ms=250,
    )
    follower_options = AutoLeafOptions(
        discovery_namespace=namespace,
        backbone_servers=(follower_backbone,),
        binary_path=nats_server_binary,
        cache_dir=tmp_path / "follower-cache",
        runtime_dir=tmp_path / "follower-runtime",
        leader_missing_grace_ms=250,
    )
    leader, follower = auto_leaf_pair_factory(leader_options, follower_options)

    wait_for(lambda: leader.role() == "leader" and follower.role() == "follower")
    leader.stop()
    wait_for(lambda: follower.role() == "leader", timeout=8.0)
    assert follower.status().leaf_status is not None


def test_auto_leaf_requires_sustained_advantage_before_preempting(
    auto_leaf_factory: Any,
    monkeypatch: Any,
    nats_server_binary: str,
    tmp_path: Path,
) -> None:
    slow_backbone = unused_nats_url()
    fast_backbone = unused_nats_url()
    monkeypatch.setattr(
        "kinopio_hub._leaf_election._probe_backbone_rtt_ms",
        lambda servers: 80.0 if tuple(servers) == (slow_backbone,) else 10.0,
    )

    namespace = f"phase5-preempt-{uuid.uuid4().hex[:8]}"
    incumbent_options = AutoLeafOptions(
        discovery_namespace=namespace,
        backbone_servers=(slow_backbone,),
        binary_path=nats_server_binary,
        cache_dir=tmp_path / "incumbent-cache",
        runtime_dir=tmp_path / "incumbent-runtime",
        leader_missing_grace_ms=300,
    )
    challenger_options = AutoLeafOptions(
        discovery_namespace=namespace,
        backbone_servers=(fast_backbone,),
        binary_path=nats_server_binary,
        cache_dir=tmp_path / "challenger-cache",
        runtime_dir=tmp_path / "challenger-runtime",
        leader_missing_grace_ms=300,
    )
    incumbent = auto_leaf_factory(incumbent_options)

    wait_for(lambda: incumbent.role() == "leader")

    challenger = auto_leaf_factory(challenger_options)
    time.sleep(0.2)
    assert incumbent.role() == "leader"
    assert challenger.role() != "leader"

    wait_for(
        lambda: challenger.role() == "leader" and incumbent.role() == "follower",
        timeout=8.0,
    )
