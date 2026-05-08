from __future__ import annotations

import json
import logging
import os
import socket
import struct
import threading
import time
import zlib
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Literal, Sequence
from urllib.parse import urlparse

from ._leaf_mdns import LeafMDNSPublisher
from ._leaf_runtime import LeafNodeHandle, LeafNodeOptions, LeafNodeStatus, start_leaf_node
from ._nats_server_binary import kinopio_cache_root

AutoLeafState = Literal[
    "discovering",
    "following-leader",
    "leader-missing-grace",
    "electing",
    "starting-leaf",
    "leader",
    "stopped",
]
AutoLeafRole = Literal["candidate", "follower", "leader"]
LeaderManifest = dict[str, Any]

_HEARTBEAT_INTERVAL_SECONDS: Final[float] = 0.5
_HEARTBEAT_SOCKET_TIMEOUT_SECONDS: Final[float] = 0.2
_LEASE_DURATION_SECONDS: Final[float] = 2.0
_DISCOVERY_SETTLE_SECONDS: Final[float] = 1.0
_LEADERSHIP_ADVANTAGE_WINDOW_SECONDS: Final[float] = 3.0
_BACKBONE_RTT_REPROBE_SECONDS: Final[float] = 5.0
_PREEMPTION_THRESHOLD_MS: Final[float] = 50.0
_MULTICAST_GROUP: Final[str] = "239.255.71.71"
_MULTICAST_PORT_BASE: Final[int] = 44710
_MULTICAST_PORT_SPAN: Final[int] = 1000
_LOCAL_BUS_REGISTRY_LOCK = threading.Lock()
_LOCAL_BUS_REGISTRY: dict[str, list["_MulticastHeartbeatBus"]] = {}


@dataclass(frozen=True)
class AutoLeafOptions:
    discovery_namespace: str
    backbone_servers: tuple[str, ...] = ()
    leader_missing_grace_ms: int = 10_000
    binary_path: str | os.PathLike[str] | None = None
    cache_dir: str | os.PathLike[str] | None = None
    runtime_dir: str | os.PathLike[str] | None = None
    advertised_host: str | None = None
    client_host: str = "127.0.0.1"
    client_port: int | None = None
    websocket_host: str | None = None
    websocket_port: int | None = None
    discovery_host: str | None = None
    discovery_port: int | None = None
    monitor_host: str = "127.0.0.1"
    monitor_port: int | None = None
    cert_file: str | os.PathLike[str] | None = None
    key_file: str | os.PathLike[str] | None = None
    ca_file: str | os.PathLike[str] | None = None
    name: str | None = None


@dataclass(frozen=True)
class AutoLeafStatus:
    state: AutoLeafState
    role: AutoLeafRole
    node_id: str
    discovery_namespace: str
    current_leader: LeaderManifest | None
    backbone_rtt_ms: float | None
    leaf_status: LeafNodeStatus | None
    mdns_enabled: bool


@dataclass
class _PeerHeartbeat:
    payload: LeaderManifest
    last_seen_monotonic: float


class AutoLeafHandle:
    def __init__(self, options: AutoLeafOptions) -> None:
        self._options = options
        self._node_id = _load_or_create_node_id(options.cache_dir)
        self._logger = logging.getLogger("kinopio_hub.auto_leaf")
        self._state_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._mdns = LeafMDNSPublisher(options.discovery_namespace, self._node_id)
        self._state: AutoLeafState = "discovering"
        self._role: AutoLeafRole = "candidate"
        self._current_leader: LeaderManifest | None = None
        self._backbone_rtt_ms: float | None = None
        self._leaf_handle: LeafNodeHandle | None = None
        self._leader_epoch = 0
        self._advantage_since: float | None = None
        self._leader_missing_since: float | None = None
        self._thread = threading.Thread(
            target=self._run,
            name=f"kinopio.auto_leaf.{options.discovery_namespace}",
            daemon=True,
        )
        self._thread.start()

    def state(self) -> AutoLeafState:
        with self._state_lock:
            return self._state

    def role(self) -> AutoLeafRole:
        with self._state_lock:
            return self._role

    def current_leader(self) -> LeaderManifest | None:
        with self._state_lock:
            if self._current_leader is None:
                return None
            return dict(self._current_leader)

    def status(self) -> AutoLeafStatus:
        with self._state_lock:
            leader = dict(self._current_leader) if self._current_leader is not None else None
            leaf_status = self._leaf_handle.status() if self._leaf_handle is not None else None
            return AutoLeafStatus(
                state=self._state,
                role=self._role,
                node_id=self._node_id,
                discovery_namespace=self._options.discovery_namespace,
                current_leader=leader,
                backbone_rtt_ms=self._backbone_rtt_ms,
                leaf_status=leaf_status,
                mdns_enabled=self._mdns.enabled,
            )

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=5)
        with self._state_lock:
            self._state = "stopped"
            self._role = "follower"
            self._current_leader = None
            leaf_handle = self._leaf_handle
            self._leaf_handle = None
        self._mdns.stop()
        if leaf_handle is not None:
            leaf_handle.stop()

    def _run(self) -> None:
        bus = _MulticastHeartbeatBus(self._options.discovery_namespace)
        peers: dict[str, _PeerHeartbeat] = {}
        started_at = time.monotonic()
        next_send_at = 0.0
        next_probe_at = 0.0

        try:
            while not self._stop_event.is_set():
                now = time.monotonic()
                if now >= next_probe_at:
                    self._probe_backbone_rtt()
                    next_probe_at = now + _BACKBONE_RTT_REPROBE_SECONDS

                for payload in bus.receive_available():
                    node_id = str(payload.get("nodeId", ""))
                    namespace = str(payload.get("discoveryNamespace", ""))
                    if (
                        not node_id
                        or node_id == self._node_id
                        or namespace != self._options.discovery_namespace
                    ):
                        continue
                    peers[node_id] = _PeerHeartbeat(payload=payload, last_seen_monotonic=now)

                self._prune_stale_peers(peers, now)
                self._tick_state_machine(
                    peers=peers,
                    now=now,
                    started_at=started_at,
                )

                if now >= next_send_at:
                    bus.send(self._heartbeat_payload(now))
                    next_send_at = now + _HEARTBEAT_INTERVAL_SECONDS

                self._stop_event.wait(_HEARTBEAT_SOCKET_TIMEOUT_SECONDS)
        finally:
            bus.close()
            self._mdns.stop()
            with self._state_lock:
                leaf_handle = self._leaf_handle
                self._leaf_handle = None
                self._state = "stopped"
            if leaf_handle is not None:
                leaf_handle.stop()

    def _tick_state_machine(
        self,
        *,
        peers: dict[str, _PeerHeartbeat],
        now: float,
        started_at: float,
    ) -> None:
        best_leader = _select_best_leader(peers, self._node_id, self._local_leader_manifest(now))

        if self.role() == "leader":
            if best_leader is not None and best_leader["nodeId"] != self._node_id:
                if _leader_beats_local(best_leader, self._leader_epoch, self._self_rank()):
                    self._step_down(best_leader)
                    return

            self._publish_leader_manifest(now)
            return

        if best_leader is not None:
            self._set_current_leader(best_leader)
            self._leader_missing_since = None
            if self._should_preempt(best_leader, now):
                self._set_state("electing", "candidate")
                self._try_become_leader(
                    peers,
                    best_leader_epoch=int(best_leader.get("leaderEpoch", 0)),
                )
            else:
                self._set_state("following-leader", "follower")
            return

        if now - started_at < _DISCOVERY_SETTLE_SECONDS and self.state() == "discovering":
            return

        if self._current_leader is not None:
            if self._leader_missing_since is None:
                self._leader_missing_since = now
                self._set_state("leader-missing-grace", "follower")
                return
            grace_seconds = self._options.leader_missing_grace_ms / 1000.0
            if now - self._leader_missing_since < grace_seconds:
                return

        self._leader_missing_since = None
        self._set_state("electing", "candidate")
        self._try_become_leader(peers, best_leader_epoch=self._leader_epoch)

    def _try_become_leader(
        self,
        peers: dict[str, _PeerHeartbeat],
        *,
        best_leader_epoch: int,
    ) -> None:
        if not _self_is_best_candidate(self._node_id, self._self_rank(), peers):
            return

        self._set_state("starting-leaf", "candidate")
        epoch = max(self._leader_epoch, best_leader_epoch) + 1
        try:
            leaf_handle = start_leaf_node(self._build_leaf_options(epoch))
        except Exception as exc:
            self._logger.warning("auto leaf start failed", exc_info=exc)
            self._set_state("electing", "candidate")
            return

        with self._state_lock:
            previous = self._leaf_handle
            self._leaf_handle = leaf_handle
            self._leader_epoch = epoch
            self._state = "leader"
            self._role = "leader"
            self._current_leader = self._build_leader_manifest(time.monotonic())
        if previous is not None:
            previous.stop()
        leaf_handle.set_discovery_manifest_provider(
            lambda: self._build_leader_manifest(time.monotonic())
        )
        self._mdns.publish(self._build_leader_manifest(time.monotonic()))

    def _step_down(self, leader_manifest: LeaderManifest) -> None:
        self._mdns.stop()
        with self._state_lock:
            leaf_handle = self._leaf_handle
            self._leaf_handle = None
            self._state = "following-leader"
            self._role = "follower"
            self._current_leader = dict(leader_manifest)
            self._leader_epoch = max(self._leader_epoch, int(leader_manifest.get("leaderEpoch", 0)))
        if leaf_handle is not None:
            leaf_handle.stop()

    def _publish_leader_manifest(self, now: float) -> None:
        manifest = self._build_leader_manifest(now)
        with self._state_lock:
            self._current_leader = manifest
        self._mdns.publish(manifest)

    def _probe_backbone_rtt(self) -> None:
        self._backbone_rtt_ms = _probe_backbone_rtt_ms(self._options.backbone_servers)

    def _set_state(self, state: AutoLeafState, role: AutoLeafRole) -> None:
        with self._state_lock:
            self._state = state
            self._role = role

    def _set_current_leader(self, leader_manifest: LeaderManifest) -> None:
        with self._state_lock:
            self._current_leader = dict(leader_manifest)
            self._leader_epoch = max(self._leader_epoch, int(leader_manifest.get("leaderEpoch", 0)))

    def _should_preempt(self, leader_manifest: LeaderManifest, now: float) -> bool:
        if not _is_significantly_better(
            self._backbone_rtt_ms,
            _coerce_float(leader_manifest.get("backboneRttMs")),
        ):
            self._advantage_since = None
            return False

        if self._advantage_since is None:
            self._advantage_since = now
            return False

        if now - self._advantage_since < _LEADERSHIP_ADVANTAGE_WINDOW_SECONDS:
            return False
        return True

    def _heartbeat_payload(self, now: float) -> LeaderManifest:
        if self.role() == "leader":
            return self._build_leader_manifest(now)

        lease_expires_at = _lease_expiry_timestamp(now)
        return {
            "version": 1,
            "nodeId": self._node_id,
            "leaderEpoch": self._leader_epoch,
            "advertisedHostname": self._options.advertised_host or "",
            "wssUrl": None,
            "fallbackServers": [],
            "backboneRttMs": self._backbone_rtt_ms,
            "discoveryUrl": None,
            "leaseExpiresAt": lease_expires_at,
            "expiresAt": lease_expires_at,
            "discoveryNamespace": self._options.discovery_namespace,
            "isLeader": False,
            "candidateRole": self.role(),
            "state": self.state(),
        }

    def _build_leader_manifest(self, now: float) -> LeaderManifest:
        leaf_handle = self._leaf_handle
        if leaf_handle is None:
            raise RuntimeError("leader manifest requested before local leaf runtime exists")

        lease_expires_at = _lease_expiry_timestamp(now)
        return {
            "version": 1,
            "nodeId": self._node_id,
            "leaderEpoch": self._leader_epoch,
            "advertisedHostname": leaf_handle.advertised_hostname,
            "wssUrl": leaf_handle.wss_url,
            "clientUrl": leaf_handle.client_url,
            "monitorUrl": leaf_handle.monitor_url,
            "fallbackServers": [leaf_handle.wss_url],
            "backboneRttMs": self._backbone_rtt_ms,
            "discoveryUrl": leaf_handle.discovery_url,
            "leaseExpiresAt": lease_expires_at,
            "expiresAt": lease_expires_at,
            "discoveryNamespace": self._options.discovery_namespace,
            "isLeader": True,
            "candidateRole": "leader",
            "state": "leader",
        }

    def _local_leader_manifest(self, now: float) -> LeaderManifest | None:
        if self._leaf_handle is None or self.role() != "leader":
            return None
        return self._build_leader_manifest(now)

    def _build_leaf_options(self, epoch: int) -> LeafNodeOptions:
        runtime_dir = None
        if self._options.runtime_dir is not None:
            runtime_root = Path(self._options.runtime_dir).expanduser()
            runtime_root.mkdir(parents=True, exist_ok=True)
            runtime_dir = runtime_root / f"epoch-{epoch}"

        return LeafNodeOptions(
            backbone_servers=self._options.backbone_servers,
            binary_path=self._options.binary_path,
            cache_dir=self._options.cache_dir,
            runtime_dir=runtime_dir,
            advertised_host=self._options.advertised_host,
            client_host=self._options.client_host,
            client_port=self._options.client_port,
            websocket_host=self._options.websocket_host,
            websocket_port=self._options.websocket_port,
            discovery_host=self._options.discovery_host,
            discovery_port=self._options.discovery_port,
            monitor_host=self._options.monitor_host,
            monitor_port=self._options.monitor_port,
            cert_file=self._options.cert_file,
            key_file=self._options.key_file,
            ca_file=self._options.ca_file,
            name=self._options.name,
        )

    def _self_rank(self) -> tuple[int, float, str]:
        return _candidate_rank(self._backbone_rtt_ms, self._node_id)

    def _prune_stale_peers(
        self,
        peers: dict[str, _PeerHeartbeat],
        now: float,
    ) -> None:
        stale = [
            node_id
            for node_id, peer in peers.items()
            if now - peer.last_seen_monotonic > _LEASE_DURATION_SECONDS
        ]
        for node_id in stale:
            peers.pop(node_id, None)


def enable_auto_leaf(options: AutoLeafOptions) -> AutoLeafHandle:
    return AutoLeafHandle(options)


class _MulticastHeartbeatBus:
    def __init__(self, discovery_namespace: str) -> None:
        self._namespace = discovery_namespace
        self._group = _MULTICAST_GROUP
        self._port = _namespace_multicast_port(discovery_namespace)
        self._local_queue: deque[LeaderManifest] = deque()
        self._local_queue_lock = threading.Lock()
        self._recv_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self._recv_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            try:
                self._recv_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except OSError:
                pass
        self._recv_socket.bind(("", self._port))
        membership = struct.pack(
            "4s4s",
            socket.inet_aton(self._group),
            socket.inet_aton("0.0.0.0"),
        )
        self._recv_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)
        self._recv_socket.settimeout(_HEARTBEAT_SOCKET_TIMEOUT_SECONDS)
        self._membership = membership

        self._send_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self._send_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
        self._send_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
        with _LOCAL_BUS_REGISTRY_LOCK:
            _LOCAL_BUS_REGISTRY.setdefault(self._namespace, []).append(self)

    def send(self, payload: LeaderManifest) -> None:
        data = json.dumps(payload).encode("utf-8")
        self._fan_out_locally(payload)
        self._send_socket.sendto(data, (self._group, self._port))

    def receive_available(self) -> list[LeaderManifest]:
        payloads = self._drain_local_queue()
        while True:
            try:
                data, _ = self._recv_socket.recvfrom(64 * 1024)
            except socket.timeout:
                break
            except OSError:
                break

            try:
                payload = json.loads(data.decode("utf-8"))
            except Exception:
                continue
            if isinstance(payload, dict):
                payloads.append(payload)
        return payloads

    def close(self) -> None:
        with _LOCAL_BUS_REGISTRY_LOCK:
            namespace_buses = _LOCAL_BUS_REGISTRY.get(self._namespace, [])
            if self in namespace_buses:
                namespace_buses.remove(self)
            if not namespace_buses:
                _LOCAL_BUS_REGISTRY.pop(self._namespace, None)
        try:
            self._recv_socket.setsockopt(
                socket.IPPROTO_IP,
                socket.IP_DROP_MEMBERSHIP,
                self._membership,
            )
        except OSError:
            pass
        self._recv_socket.close()
        self._send_socket.close()

    def _fan_out_locally(self, payload: LeaderManifest) -> None:
        with _LOCAL_BUS_REGISTRY_LOCK:
            peers = list(_LOCAL_BUS_REGISTRY.get(self._namespace, ()))
        for peer in peers:
            if peer is self:
                continue
            with peer._local_queue_lock:
                peer._local_queue.append(dict(payload))

    def _drain_local_queue(self) -> list[LeaderManifest]:
        with self._local_queue_lock:
            payloads = list(self._local_queue)
            self._local_queue.clear()
        return payloads


def _select_best_leader(
    peers: dict[str, _PeerHeartbeat],
    local_node_id: str,
    local_leader_manifest: LeaderManifest | None,
) -> LeaderManifest | None:
    candidates: list[LeaderManifest] = []
    if local_leader_manifest is not None:
        candidates.append(local_leader_manifest)
    for peer in peers.values():
        payload = peer.payload
        if payload.get("isLeader") is True:
            candidates.append(payload)

    if not candidates:
        return None

    candidates.sort(
        key=lambda payload: (
            -int(payload.get("leaderEpoch", 0)),
            *_candidate_rank(
                _coerce_float(payload.get("backboneRttMs")),
                str(payload.get("nodeId", "")),
            ),
        )
    )
    best = candidates[0]
    if str(best.get("nodeId", "")) == local_node_id and local_leader_manifest is not None:
        return dict(local_leader_manifest)
    return dict(best)


def _self_is_best_candidate(
    node_id: str,
    self_rank: tuple[int, float, str],
    peers: dict[str, _PeerHeartbeat],
) -> bool:
    best_rank = self_rank
    best_node_id = node_id

    for peer_node_id, peer in peers.items():
        if peer.payload.get("isLeader") is True:
            continue
        candidate_rank = _candidate_rank(
            _coerce_float(peer.payload.get("backboneRttMs")),
            peer_node_id,
        )
        if candidate_rank < best_rank:
            best_rank = candidate_rank
            best_node_id = peer_node_id

    return best_node_id == node_id


def _candidate_rank(rtt_ms: float | None, node_id: str) -> tuple[int, float, str]:
    if rtt_ms is None:
        return (1, float("inf"), node_id)
    return (0, rtt_ms, node_id)


def _leader_beats_local(
    leader_manifest: LeaderManifest,
    local_epoch: int,
    local_rank: tuple[int, float, str],
) -> bool:
    leader_epoch = int(leader_manifest.get("leaderEpoch", 0))
    if leader_epoch > local_epoch:
        return True
    if leader_epoch < local_epoch:
        return False
    leader_rank = _candidate_rank(
        _coerce_float(leader_manifest.get("backboneRttMs")),
        str(leader_manifest.get("nodeId", "")),
    )
    return leader_rank < local_rank


def _is_significantly_better(self_rtt_ms: float | None, leader_rtt_ms: float | None) -> bool:
    if self_rtt_ms is None:
        return False
    if leader_rtt_ms is None:
        return True
    return (leader_rtt_ms - self_rtt_ms) >= _PREEMPTION_THRESHOLD_MS


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _probe_backbone_rtt_ms(backbone_servers: Sequence[str]) -> float | None:
    best_rtt: float | None = None
    for server in backbone_servers:
        parsed = urlparse(server)
        host = parsed.hostname
        if host is None:
            continue
        if parsed.port is not None:
            port = parsed.port
        elif parsed.scheme in {"ws", "http"}:
            port = 80
        elif parsed.scheme in {"wss", "https"}:
            port = 443
        else:
            port = 4222

        started = time.perf_counter()
        try:
            with socket.create_connection((host, port), timeout=0.5):
                elapsed_ms = (time.perf_counter() - started) * 1000.0
        except OSError:
            continue

        if best_rtt is None or elapsed_ms < best_rtt:
            best_rtt = elapsed_ms
    return best_rtt


def _load_or_create_node_id(cache_dir: str | os.PathLike[str] | None) -> str:
    cache_root = Path(cache_dir).expanduser() if cache_dir is not None else kinopio_cache_root()
    node_id_file = cache_root / "auto-leaf-node-id"
    node_id_file.parent.mkdir(parents=True, exist_ok=True)
    if node_id_file.exists():
        existing = node_id_file.read_text(encoding="utf-8").strip()
        if existing:
            return existing

    node_id = f"kinopio-{zlib.crc32(str(time.time_ns()).encode()):08x}-{os.getpid()}"
    node_id_file.write_text(node_id, encoding="utf-8")
    return node_id


def _namespace_multicast_port(discovery_namespace: str) -> int:
    namespace_hash = zlib.crc32(discovery_namespace.encode("utf-8")) % _MULTICAST_PORT_SPAN
    return _MULTICAST_PORT_BASE + namespace_hash


def _lease_expiry_timestamp(_: float) -> str:
    expires_at = time.time() + _LEASE_DURATION_SECONDS
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(expires_at))
