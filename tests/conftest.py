from __future__ import annotations

import shutil
import socket
import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterator

import pytest

from kinopio_hub._leaf_tls import _generate_ca, _generate_server_certificate
from kinopio_hub._nats_server_binary import download_nats_server_binary
from kinopio_hub.leaf import (
    AutoLeafHandle,
    AutoLeafOptions,
    LeafNodeHandle,
    LeafNodeOptions,
    enable_auto_leaf,
    start_leaf_node,
)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


def _wait_for_port(port: int, *, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.2)
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.1)
    raise RuntimeError(f"Timed out waiting for port {port}")


@dataclass
class NatsTestServer:
    tcp_url: str
    ws_url: str
    _tcp_port: int
    _ws_port: int
    _workdir: Path
    _config_text: str | None = None
    _extra_ports: tuple[int, ...] = ()
    _process: subprocess.Popen[str] | None = None
    _container_name: str | None = None
    _using_docker: bool = False
    _log_path: Path | None = None

    def start(self) -> None:
        if self._process is not None:
            return

        self._workdir.mkdir(parents=True, exist_ok=True)
        config = self._workdir / "nats.conf"
        self._log_path = self._workdir / "nats.log"
        config_text = self._config_text
        if config_text is None:
            config_text = (
                f"port: {self._tcp_port}\n"
                "server_name: kinopio-test\n"
                "websocket {\n"
                f"  port: {self._ws_port}\n"
                "  no_tls: true\n"
                "}\n"
            )
        config.write_text(config_text, encoding="utf-8")

        binary = shutil.which("nats-server")
        if binary is None:
            try:
                binary = str(
                    download_nats_server_binary(
                        version="2.11.4",
                        cache_dir=Path.cwd() / ".pytest-nats-tools",
                    )
                )
            except Exception:
                binary = None
        log_handle = self._log_path.open("w", encoding="utf-8")
        if binary:
            self._process = subprocess.Popen(
                [binary, "-c", str(config)],
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                text=True,
            )
        else:
            docker = shutil.which("docker")
            if docker is None:
                raise RuntimeError("Neither nats-server nor docker is available")

            self._using_docker = True
            self._container_name = f"kinopio-tests-{uuid.uuid4().hex[:8]}"
            self._process = subprocess.Popen(
                [
                    docker,
                    "run",
                    "--rm",
                    "--name",
                    self._container_name,
                    "-p",
                    f"{self._tcp_port}:4222",
                    "-p",
                    f"{self._ws_port}:9222",
                    "-v",
                    f"{config}:/etc/nats/nats-server.conf:ro",
                    "nats:2.11-alpine",
                    "-c",
                    "/etc/nats/nats-server.conf",
                ],
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                text=True,
            )

        start_timeout = 60.0 if self._using_docker else 15.0
        try:
            _wait_for_port(self._tcp_port, timeout=start_timeout)
            _wait_for_port(self._ws_port, timeout=start_timeout)
            for extra_port in self._extra_ports:
                _wait_for_port(extra_port, timeout=start_timeout)
        except Exception as exc:
            self.stop()
            log_output = ""
            if self._log_path is not None and self._log_path.exists():
                log_output = self._log_path.read_text(encoding="utf-8")
            raise RuntimeError(
                f"Failed to start NATS test server: {exc}\nLog output:\n{log_output}"
            ) from exc

    def stop(self) -> None:
        process = self._process
        self._process = None
        if process is None:
            return

        if self._using_docker and self._container_name:
            subprocess.run(
                ["docker", "stop", self._container_name],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
            )
            process.wait(timeout=10)
            self._container_name = None
            return

        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)

    def restart(self) -> None:
        self.stop()
        self.start()


@dataclass
class NatsCluster:
    server_a: NatsTestServer
    server_b: NatsTestServer

    @property
    def urls(self) -> tuple[str, str]:
        return (self.server_a.tcp_url, self.server_b.tcp_url)


@dataclass
class NatsLeafBackbone:
    client_url: str
    leaf_url: str
    server: NatsTestServer


@dataclass
class NatsTlsFirstServer:
    tcp_url: str
    ca_cert_file: Path
    server: NatsTestServer


@dataclass
class NatsServerPool:
    servers: tuple[NatsTestServer, ...]

    @property
    def urls(self) -> tuple[str, ...]:
        return tuple(server.tcp_url for server in self.servers)

    @property
    def ws_urls(self) -> tuple[str, ...]:
        return tuple(server.ws_url for server in self.servers)


def _cluster_server_config(
    *,
    client_port: int,
    websocket_port: int,
    route_port: int,
    cluster_name: str,
    server_name: str,
    routes: tuple[str, ...] = (),
) -> str:
    routes_block = ""
    if routes:
        formatted_routes = "\n".join(f"    {route}" for route in routes)
        routes_block = f"  routes = [\n{formatted_routes}\n  ]\n"

    return (
        f"port: {client_port}\n"
        f"server_name: {server_name}\n"
        "websocket {\n"
        f"  port: {websocket_port}\n"
        "  no_tls: true\n"
        "}\n"
        "cluster {\n"
        f"  name: {cluster_name}\n"
        f"  listen: 127.0.0.1:{route_port}\n"
        "  pool_size: 1\n"
        f"{routes_block}"
        "}\n"
    )


def _leaf_backbone_config(
    *,
    client_port: int,
    websocket_port: int,
    leaf_port: int,
    server_name: str,
) -> str:
    return (
        f"port: {client_port}\n"
        f"server_name: {server_name}\n"
        "websocket {\n"
        f"  port: {websocket_port}\n"
        "  no_tls: true\n"
        "}\n"
        "leafnodes {\n"
        f"  port: {leaf_port}\n"
        "}\n"
    )


def _tls_first_server_config(
    *,
    client_port: int,
    websocket_port: int,
    cert_file: Path,
    key_file: Path,
    server_name: str,
) -> str:
    return (
        f"port: {client_port}\n"
        f"server_name: {server_name}\n"
        "tls {\n"
        f"  cert_file: {cert_file}\n"
        f"  key_file: {key_file}\n"
        "  handshake_first: true\n"
        "}\n"
        "websocket {\n"
        f"  port: {websocket_port}\n"
        "  no_tls: true\n"
        "}\n"
    )


@pytest.fixture(scope="session")
def nats_server() -> Iterator[NatsTestServer]:
    tcp_port = _free_port()
    ws_port = _free_port()
    base_dir = Path.cwd() / ".pytest-nats"
    workdir = base_dir / uuid.uuid4().hex
    server = NatsTestServer(
        tcp_url=f"nats://127.0.0.1:{tcp_port}",
        ws_url=f"ws://127.0.0.1:{ws_port}",
        _tcp_port=tcp_port,
        _ws_port=ws_port,
        _workdir=workdir,
    )
    server.start()
    try:
        yield server
    finally:
        server.stop()
        shutil.rmtree(workdir, ignore_errors=True)


@pytest.fixture(scope="session")
def nats_server_binary() -> str:
    binary = shutil.which("nats-server")
    if binary is not None:
        return binary
    return str(
        download_nats_server_binary(
            version="2.11.4",
            cache_dir=Path.cwd() / ".pytest-nats-tools",
        )
    )


@pytest.fixture
def nats_server_pool() -> Iterator[NatsServerPool]:
    base_dir = Path.cwd() / ".pytest-nats"
    pool_dir = base_dir / uuid.uuid4().hex
    servers: list[NatsTestServer] = []

    for index in range(2):
        tcp_port = _free_port()
        ws_port = _free_port()
        server = NatsTestServer(
            tcp_url=f"nats://127.0.0.1:{tcp_port}",
            ws_url=f"ws://127.0.0.1:{ws_port}",
            _tcp_port=tcp_port,
            _ws_port=ws_port,
            _workdir=pool_dir / f"server-{index}",
        )
        server.start()
        servers.append(server)

    try:
        yield NatsServerPool(servers=tuple(servers))
    finally:
        for server in reversed(servers):
            server.stop()
        shutil.rmtree(pool_dir, ignore_errors=True)


@pytest.fixture
def nats_cluster() -> Iterator[NatsCluster]:
    cluster_name = f"kinopio-cluster-{uuid.uuid4().hex[:8]}"
    base_dir = Path.cwd() / ".pytest-nats"
    cluster_dir = base_dir / uuid.uuid4().hex

    server_a_tcp = _free_port()
    server_a_ws = _free_port()
    server_a_route = _free_port()

    server_b_tcp = _free_port()
    server_b_ws = _free_port()
    server_b_route = _free_port()

    route_to_a = f"nats://127.0.0.1:{server_a_route}"
    server_a = NatsTestServer(
        tcp_url=f"nats://127.0.0.1:{server_a_tcp}",
        ws_url=f"ws://127.0.0.1:{server_a_ws}",
        _tcp_port=server_a_tcp,
        _ws_port=server_a_ws,
        _workdir=cluster_dir / "server-a",
        _config_text=_cluster_server_config(
            client_port=server_a_tcp,
            websocket_port=server_a_ws,
            route_port=server_a_route,
            cluster_name=cluster_name,
            server_name="kinopio-cluster-a",
        ),
    )
    server_b = NatsTestServer(
        tcp_url=f"nats://127.0.0.1:{server_b_tcp}",
        ws_url=f"ws://127.0.0.1:{server_b_ws}",
        _tcp_port=server_b_tcp,
        _ws_port=server_b_ws,
        _workdir=cluster_dir / "server-b",
        _config_text=_cluster_server_config(
            client_port=server_b_tcp,
            websocket_port=server_b_ws,
            route_port=server_b_route,
            cluster_name=cluster_name,
            server_name="kinopio-cluster-b",
            routes=(route_to_a,),
        ),
    )

    server_a.start()
    server_b.start()
    time.sleep(0.5)

    try:
        yield NatsCluster(server_a=server_a, server_b=server_b)
    finally:
        server_b.stop()
        server_a.stop()
        shutil.rmtree(cluster_dir, ignore_errors=True)


@pytest.fixture
def nats_leaf_backbone() -> Iterator[NatsLeafBackbone]:
    client_port = _free_port()
    ws_port = _free_port()
    leaf_port = _free_port()
    base_dir = Path.cwd() / ".pytest-nats"
    workdir = base_dir / uuid.uuid4().hex
    server = NatsTestServer(
        tcp_url=f"nats://127.0.0.1:{client_port}",
        ws_url=f"ws://127.0.0.1:{ws_port}",
        _tcp_port=client_port,
        _ws_port=ws_port,
        _workdir=workdir,
        _config_text=_leaf_backbone_config(
            client_port=client_port,
            websocket_port=ws_port,
            leaf_port=leaf_port,
            server_name="kinopio-leaf-backbone",
        ),
        _extra_ports=(leaf_port,),
    )
    server.start()
    try:
        yield NatsLeafBackbone(
            client_url=server.tcp_url,
            leaf_url=f"nats://127.0.0.1:{leaf_port}",
            server=server,
        )
    finally:
        server.stop()
        shutil.rmtree(workdir, ignore_errors=True)


@pytest.fixture
def nats_tls_first_server() -> Iterator[NatsTlsFirstServer]:
    tcp_port = _free_port()
    ws_port = _free_port()
    base_dir = Path.cwd() / ".pytest-nats"
    workdir = base_dir / uuid.uuid4().hex
    workdir.mkdir(parents=True, exist_ok=True)

    ca_cert_file = workdir / "ca-cert.pem"
    ca_key_file = workdir / "ca-key.pem"
    cert_file = workdir / "server-cert.pem"
    key_file = workdir / "server-key.pem"
    _generate_ca(ca_cert_file=ca_cert_file, ca_key_file=ca_key_file)
    _generate_server_certificate(
        cert_path=cert_file,
        key_path=key_file,
        ca_cert_file=ca_cert_file,
        ca_key_file=ca_key_file,
        advertised_hosts=("127.0.0.1", "localhost"),
    )

    server = NatsTestServer(
        tcp_url=f"nats://127.0.0.1:{tcp_port}",
        ws_url=f"ws://127.0.0.1:{ws_port}",
        _tcp_port=tcp_port,
        _ws_port=ws_port,
        _workdir=workdir,
        _config_text=_tls_first_server_config(
            client_port=tcp_port,
            websocket_port=ws_port,
            cert_file=cert_file,
            key_file=key_file,
            server_name="kinopio-tls-first",
        ),
    )
    server.start()
    try:
        yield NatsTlsFirstServer(
            tcp_url=server.tcp_url,
            ca_cert_file=ca_cert_file,
            server=server,
        )
    finally:
        server.stop()
        shutil.rmtree(workdir, ignore_errors=True)


@pytest.fixture
def leaf_node_factory() -> Iterator[Callable[[LeafNodeOptions], LeafNodeHandle]]:
    handles: list[LeafNodeHandle] = []

    def factory(options: LeafNodeOptions) -> LeafNodeHandle:
        handle = start_leaf_node(options)
        handles.append(handle)
        return handle

    try:
        yield factory
    finally:
        for handle in reversed(handles):
            try:
                handle.stop()
            except Exception:
                pass


@pytest.fixture
def auto_leaf_factory() -> Iterator[Callable[[AutoLeafOptions], AutoLeafHandle]]:
    handles: list[AutoLeafHandle] = []

    def factory(options: AutoLeafOptions) -> AutoLeafHandle:
        handle = enable_auto_leaf(options)
        handles.append(handle)
        return handle

    try:
        yield factory
    finally:
        for handle in reversed(handles):
            try:
                handle.stop()
            except Exception:
                pass


@pytest.fixture
def auto_leaf_pair_factory(
    auto_leaf_factory: Callable[[AutoLeafOptions], AutoLeafHandle],
) -> Callable[[AutoLeafOptions, AutoLeafOptions], tuple[AutoLeafHandle, AutoLeafHandle]]:
    def factory(
        first: AutoLeafOptions,
        second: AutoLeafOptions,
    ) -> tuple[AutoLeafHandle, AutoLeafHandle]:
        return (auto_leaf_factory(first), auto_leaf_factory(second))

    return factory
