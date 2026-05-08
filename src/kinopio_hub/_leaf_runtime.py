from __future__ import annotations

import json
import logging
import os
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Final, Literal, Sequence, cast
from urllib.request import urlopen

from ._leaf_tls import LeafTLSMaterials, resolve_leaf_tls_materials
from ._nats_server_binary import resolve_nats_server_binary

LeafBridgeState = Literal["disabled", "connecting", "connected", "error", "stopped"]
_READY_TIMEOUT_SECONDS: Final[float] = 15.0
_PROCESS_STOP_TIMEOUT_SECONDS: Final[float] = 10.0
_DISCOVERY_MANIFEST_TTL_SECONDS: Final[float] = 5.0


@dataclass(frozen=True)
class LeafNodeOptions:
    backbone_servers: tuple[str, ...] = ()
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
class LeafNodeListenerStatus:
    url: str
    host: str
    port: int
    ready: bool


@dataclass(frozen=True)
class LeafNodeStatus:
    client: LeafNodeListenerStatus
    websocket: LeafNodeListenerStatus
    discovery: LeafNodeListenerStatus
    monitor: LeafNodeListenerStatus
    bridge_state: LeafBridgeState
    backbone_servers: tuple[str, ...]
    server_ready: bool
    process_id: int | None


class _DiscoveryServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, server_address: tuple[str, int]) -> None:
        super().__init__(server_address, _DiscoveryRequestHandler)
        self.manifest_provider: Callable[[], dict[str, Any]] = lambda: {}


class _DiscoveryRequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        server = cast(_DiscoveryServer, self.server)
        if self.path not in ("/", "/manifest.json", "/healthz"):
            self.send_error(404, "Not found")
            return

        if self.path == "/healthz":
            payload = {"ok": True}
        else:
            payload = server.manifest_provider()

        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        return


@dataclass
class _DiscoveryServerHandle:
    host: str
    port: int
    url: str
    _server: _DiscoveryServer
    _thread: threading.Thread

    def set_manifest_provider(self, provider: Callable[[], dict[str, Any]]) -> None:
        self._server.manifest_provider = provider

    def is_running(self) -> bool:
        return self._thread.is_alive()

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2)


class LeafNodeHandle:
    def __init__(
        self,
        *,
        runtime_dir: Path,
        process: subprocess.Popen[str],
        log_path: Path,
        log_handle: Any,
        discovery_server: _DiscoveryServerHandle,
        tls_materials: LeafTLSMaterials,
        client_bind_host: str,
        client_url_host: str,
        client_port: int,
        websocket_bind_host: str,
        websocket_url_host: str,
        websocket_port: int,
        monitor_bind_host: str,
        monitor_url_host: str,
        monitor_port: int,
        backbone_servers: tuple[str, ...],
        node_id: str,
    ) -> None:
        self._runtime_dir = runtime_dir
        self._process = process
        self._log_path = log_path
        self._log_handle = log_handle
        self._discovery_server = discovery_server
        self._tls_materials = tls_materials
        self._client_bind_host = client_bind_host
        self._client_url_host = client_url_host
        self._client_port = client_port
        self._websocket_bind_host = websocket_bind_host
        self._websocket_url_host = websocket_url_host
        self._websocket_port = websocket_port
        self._monitor_bind_host = monitor_bind_host
        self._monitor_url_host = monitor_url_host
        self._monitor_port = monitor_port
        self._backbone_servers = backbone_servers
        self._node_id = node_id
        self._stopped = False

    @property
    def client_url(self) -> str:
        return f"nats://{self._client_url_host}:{self._client_port}"

    @property
    def wss_url(self) -> str:
        return f"wss://{self._websocket_url_host}:{self._websocket_port}"

    @property
    def discovery_url(self) -> str:
        return self._discovery_server.url

    @property
    def monitor_url(self) -> str:
        return f"http://{self._monitor_url_host}:{self._monitor_port}"

    @property
    def ca_cert_file(self) -> Path:
        return self._tls_materials.ca_file

    @property
    def node_id(self) -> str:
        return self._node_id

    @property
    def advertised_hostname(self) -> str:
        return self._websocket_url_host

    def set_discovery_manifest_provider(
        self,
        provider: Callable[[], dict[str, Any]],
    ) -> None:
        self._discovery_server.set_manifest_provider(provider)

    def status(self) -> LeafNodeStatus:
        if self._stopped:
            return LeafNodeStatus(
                client=LeafNodeListenerStatus(
                    url=self.client_url,
                    host=self._client_bind_host,
                    port=self._client_port,
                    ready=False,
                ),
                websocket=LeafNodeListenerStatus(
                    url=self.wss_url,
                    host=self._websocket_bind_host,
                    port=self._websocket_port,
                    ready=False,
                ),
                discovery=LeafNodeListenerStatus(
                    url=self.discovery_url,
                    host=self._discovery_server.host,
                    port=self._discovery_server.port,
                    ready=False,
                ),
                monitor=LeafNodeListenerStatus(
                    url=self.monitor_url,
                    host=self._monitor_bind_host,
                    port=self._monitor_port,
                    ready=False,
                ),
                bridge_state="stopped",
                backbone_servers=self._backbone_servers,
                server_ready=False,
                process_id=None,
            )

        process_id = self._process.pid if self._process.poll() is None else None
        client_ready = _socket_is_open(self._client_url_host, self._client_port)
        websocket_ready = _socket_is_open(self._websocket_url_host, self._websocket_port)
        discovery_ready = self._discovery_server.is_running()
        server_ready = self._monitor_ready()
        monitor_ready = server_ready or _socket_is_open(self._monitor_url_host, self._monitor_port)

        return LeafNodeStatus(
            client=LeafNodeListenerStatus(
                url=self.client_url,
                host=self._client_bind_host,
                port=self._client_port,
                ready=client_ready,
            ),
            websocket=LeafNodeListenerStatus(
                url=self.wss_url,
                host=self._websocket_bind_host,
                port=self._websocket_port,
                ready=websocket_ready,
            ),
            discovery=LeafNodeListenerStatus(
                url=self.discovery_url,
                host=self._discovery_server.host,
                port=self._discovery_server.port,
                ready=discovery_ready,
            ),
            monitor=LeafNodeListenerStatus(
                url=self.monitor_url,
                host=self._monitor_bind_host,
                port=self._monitor_port,
                ready=monitor_ready,
            ),
            bridge_state=self._bridge_state(server_ready),
            backbone_servers=self._backbone_servers,
            server_ready=server_ready,
            process_id=process_id,
        )

    def stop(self) -> None:
        if self._stopped:
            return

        self._stopped = True
        try:
            self._discovery_server.stop()
        finally:
            self._stop_process()
            self._log_handle.close()
            shutil.rmtree(self._runtime_dir, ignore_errors=True)

    def __enter__(self) -> "LeafNodeHandle":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.stop()

    def _monitor_ready(self) -> bool:
        if self._process.poll() is not None:
            return False
        try:
            with urlopen(f"{self.monitor_url}/healthz", timeout=1.0) as response:
                return bool(response.status == 200)
        except Exception:
            return False

    def _bridge_state(self, server_ready: bool) -> LeafBridgeState:
        if not self._backbone_servers:
            return "disabled"
        if self._process.poll() is not None:
            return "error"
        if not server_ready:
            return "connecting"

        try:
            leafz = _load_monitor_json(f"{self.monitor_url}/leafz")
            if _has_leaf_connections(leafz):
                return "connected"
        except Exception:
            return "connecting"
        return "connecting"

    def _stop_process(self) -> None:
        if self._process.poll() is not None:
            return
        self._process.terminate()
        try:
            self._process.wait(timeout=_PROCESS_STOP_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.wait(timeout=_PROCESS_STOP_TIMEOUT_SECONDS)

    def _manifest(self) -> dict[str, Any]:
        expires_at = _timestamp_after(_DISCOVERY_MANIFEST_TTL_SECONDS)
        return {
            "version": 1,
            "nodeId": self._node_id,
            "advertisedHostname": self._websocket_url_host,
            "wssUrl": self.wss_url,
            "clientUrl": self.client_url,
            "monitorUrl": self.monitor_url,
            "discoveryUrl": self.discovery_url,
            "fallbackServers": [self.wss_url],
            "backboneRttMs": None,
            "backboneServers": list(self._backbone_servers),
            "bridgeState": self.status().bridge_state,
            "leaderEpoch": 0,
            "expiresAt": expires_at,
            "leaseExpiresAt": expires_at,
            "isLeader": True,
            "candidateRole": "leader",
            "generatedAt": _timestamp(),
        }


def start_leaf_node(options: LeafNodeOptions | None = None) -> LeafNodeHandle:
    resolved_options = options or LeafNodeOptions()
    logger = logging.getLogger("kinopio_hub.leaf")

    advertised_host = resolved_options.advertised_host or _detect_lan_host()
    client_bind_host = resolved_options.client_host
    websocket_bind_host = resolved_options.websocket_host or advertised_host
    discovery_bind_host = resolved_options.discovery_host or advertised_host
    monitor_bind_host = resolved_options.monitor_host
    client_url_host = _url_host(client_bind_host, "127.0.0.1")
    websocket_url_host = _url_host(websocket_bind_host, advertised_host)
    discovery_url_host = _url_host(discovery_bind_host, advertised_host)
    monitor_url_host = _url_host(monitor_bind_host, "127.0.0.1")

    client_port = resolved_options.client_port or _free_port(client_bind_host)
    websocket_port = resolved_options.websocket_port or _free_port(websocket_bind_host)
    discovery_port = resolved_options.discovery_port or _free_port(discovery_bind_host)
    monitor_port = resolved_options.monitor_port or _free_port(monitor_bind_host)

    runtime_dir = _prepare_runtime_dir(resolved_options.runtime_dir)
    node_id = uuid.uuid4().hex
    process: subprocess.Popen[str] | None = None
    log_handle: Any | None = None
    discovery_server: _DiscoveryServerHandle | None = None

    try:
        binary_path = resolve_nats_server_binary(
            binary_path=resolved_options.binary_path,
            cache_dir=resolved_options.cache_dir,
        )
        tls_materials = resolve_leaf_tls_materials(
            runtime_dir=runtime_dir,
            advertised_hosts=(
                client_url_host,
                websocket_url_host,
                discovery_url_host,
                monitor_url_host,
                advertised_host,
            ),
            cert_file=resolved_options.cert_file,
            key_file=resolved_options.key_file,
            ca_file=resolved_options.ca_file,
            cache_dir=resolved_options.cache_dir,
        )

        discovery_server = _start_discovery_server(
            host=discovery_bind_host,
            port=discovery_port,
            url_host=discovery_url_host,
        )

        config_path = runtime_dir / "leaf-nats.conf"
        log_path = runtime_dir / "leaf-nats.log"
        config_path.write_text(
            _build_nats_config(
                name=resolved_options.name or f"kinopio-leaf-{node_id[:8]}",
                client_host=client_bind_host,
                client_port=client_port,
                monitor_host=monitor_bind_host,
                monitor_port=monitor_port,
                websocket_host=websocket_bind_host,
                websocket_port=websocket_port,
                tls_materials=tls_materials,
                backbone_servers=resolved_options.backbone_servers,
            ),
            encoding="utf-8",
        )
        log_handle = log_path.open("w", encoding="utf-8")
        process = subprocess.Popen(
            [str(binary_path), "-c", str(config_path)],
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
        (runtime_dir / "leaf-nats.pid").write_text(str(process.pid), encoding="utf-8")

        _wait_until_ready(
            process=process,
            log_path=log_path,
            client_host=client_url_host,
            client_port=client_port,
            websocket_host=websocket_url_host,
            websocket_port=websocket_port,
            monitor_url=f"http://{monitor_url_host}:{monitor_port}",
            discovery_server=discovery_server,
        )

        handle = LeafNodeHandle(
            runtime_dir=runtime_dir,
            process=process,
            log_path=log_path,
            log_handle=log_handle,
            discovery_server=discovery_server,
            tls_materials=tls_materials,
            client_bind_host=client_bind_host,
            client_url_host=client_url_host,
            client_port=client_port,
            websocket_bind_host=websocket_bind_host,
            websocket_url_host=websocket_url_host,
            websocket_port=websocket_port,
            monitor_bind_host=monitor_bind_host,
            monitor_url_host=monitor_url_host,
            monitor_port=monitor_port,
            backbone_servers=tuple(resolved_options.backbone_servers),
            node_id=node_id,
        )
        discovery_server.set_manifest_provider(handle._manifest)
        return handle
    except Exception:
        logger.exception("failed to start leaf node runtime")
        if discovery_server is not None:
            discovery_server.stop()
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=_PROCESS_STOP_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=_PROCESS_STOP_TIMEOUT_SECONDS)
        if log_handle is not None:
            log_handle.close()
        shutil.rmtree(runtime_dir, ignore_errors=True)
        raise


def _prepare_runtime_dir(runtime_dir: str | os.PathLike[str] | None) -> Path:
    if runtime_dir is None:
        return Path(tempfile.mkdtemp(prefix="kinopio-leaf-"))

    resolved = Path(runtime_dir).expanduser()
    if resolved.exists() and any(resolved.iterdir()):
        raise RuntimeError("runtime_dir must be empty when provided")
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def _build_nats_config(
    *,
    name: str,
    client_host: str,
    client_port: int,
    monitor_host: str,
    monitor_port: int,
    websocket_host: str,
    websocket_port: int,
    tls_materials: LeafTLSMaterials,
    backbone_servers: Sequence[str],
) -> str:
    lines = [
        f"server_name: {_quoted(name)}",
        f"listen: {_quoted(f'{client_host}:{client_port}')}",
        f"http: {_quoted(f'{monitor_host}:{monitor_port}')}",
        "websocket {",
        f"  listen: {_quoted(f'{websocket_host}:{websocket_port}')}",
        "  tls {",
        f"    cert_file: {_quoted(str(tls_materials.cert_file))}",
        f"    key_file: {_quoted(str(tls_materials.key_file))}",
        "  }",
        "}",
    ]

    if backbone_servers:
        rendered_urls = ", ".join(_quoted(url) for url in backbone_servers)
        lines.extend(
            [
                "leafnodes {",
                "  reconnect: 1",
                "  remotes = [",
                "    {",
                f"      urls: [{rendered_urls}]",
                "      no_randomize: true",
                "    }",
                "  ]",
                "}",
            ]
        )

    return "\n".join(lines) + "\n"


def _start_discovery_server(*, host: str, port: int, url_host: str) -> _DiscoveryServerHandle:
    server = _DiscoveryServer((host, port))
    thread = threading.Thread(target=server.serve_forever, name="kinopio.discovery", daemon=True)
    thread.start()
    return _DiscoveryServerHandle(
        host=host,
        port=port,
        url=f"http://{url_host}:{port}/manifest.json",
        _server=server,
        _thread=thread,
    )


def _wait_until_ready(
    *,
    process: subprocess.Popen[str],
    log_path: Path,
    client_host: str,
    client_port: int,
    websocket_host: str,
    websocket_port: int,
    monitor_url: str,
    discovery_server: _DiscoveryServerHandle,
) -> None:
    deadline = time.monotonic() + _READY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(_startup_failure_message(log_path))

        client_ready = _socket_is_open(client_host, client_port)
        websocket_ready = _socket_is_open(websocket_host, websocket_port)
        monitor_ready = False
        try:
            with urlopen(f"{monitor_url}/healthz", timeout=1.0) as response:
                monitor_ready = response.status == 200
        except Exception:
            monitor_ready = False

        if client_ready and websocket_ready and monitor_ready and discovery_server.is_running():
            return
        time.sleep(0.1)

    raise RuntimeError(_startup_failure_message(log_path, suffix="Timed out waiting for ready"))


def _startup_failure_message(log_path: Path, *, suffix: str | None = None) -> str:
    log_output = ""
    if log_path.exists():
        log_output = log_path.read_text(encoding="utf-8")
    message = "Failed to start local leaf runtime"
    if suffix:
        message = f"{message}: {suffix}"
    if log_output:
        message = f"{message}\nLog output:\n{log_output}"
    return message


def _socket_is_open(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.3)
        return sock.connect_ex((host, port)) == 0


def _free_port(host: str) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        sock.listen(1)
        return cast(int, sock.getsockname()[1])


def _detect_lan_host() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        try:
            sock.connect(("8.8.8.8", 80))
            host = cast(str, sock.getsockname()[0])
            if host and not host.startswith("127."):
                return host
        except OSError:
            pass

    try:
        for entry in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            host = cast(str, entry[4][0])
            if host and not host.startswith("127."):
                return host
    except OSError:
        pass
    return "127.0.0.1"


def _url_host(bind_host: str, fallback_host: str) -> str:
    if bind_host in {"0.0.0.0", "::"}:
        return fallback_host
    return bind_host


def _quoted(value: str) -> str:
    return json.dumps(value)


def _load_monitor_json(url: str) -> dict[str, Any]:
    with urlopen(url, timeout=1.5) as response:
        return cast(dict[str, Any], json.load(response))


def _has_leaf_connections(payload: dict[str, Any]) -> bool:
    for key in ("leafs", "leaf_nodes", "remote_leafs"):
        value = payload.get(key)
        if isinstance(value, list) and value:
            return True

    for key in ("leafnodes", "num_leafs", "num_leaf_nodes"):
        value = payload.get(key)
        if isinstance(value, int) and value > 0:
            return True
    return False


def _timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _timestamp_after(seconds: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + seconds))
