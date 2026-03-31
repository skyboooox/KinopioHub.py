from __future__ import annotations

import platform
import shutil
import socket
import stat
import subprocess
import tarfile
import time
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import pytest

NATS_SERVER_VERSION = "2.11.4"


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


def _download_nats_server_binary(cache_dir: Path) -> str | None:
    system = platform.system().lower()
    machine = platform.machine().lower()
    assets = {
        ("darwin", "arm64"): "darwin-arm64",
        ("darwin", "x86_64"): "darwin-amd64",
        ("linux", "x86_64"): "linux-amd64",
        ("linux", "aarch64"): "linux-arm64",
    }
    platform_tag = assets.get((system, machine))
    if platform_tag is None:
        return None

    binary_path = cache_dir / f"nats-server-v{NATS_SERVER_VERSION}-{platform_tag}" / "nats-server"
    if binary_path.exists():
        return str(binary_path)

    cache_dir.mkdir(parents=True, exist_ok=True)
    archive_name = f"nats-server-v{NATS_SERVER_VERSION}-{platform_tag}.tar.gz"
    archive_path = cache_dir / archive_name
    url = (
        "https://github.com/nats-io/nats-server/releases/download/"
        f"v{NATS_SERVER_VERSION}/{archive_name}"
    )

    urllib.request.urlretrieve(url, archive_path)

    with tarfile.open(archive_path, "r:gz") as tar:
        tar.extractall(path=cache_dir)

    if not binary_path.exists():
        return None

    binary_path.chmod(binary_path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return str(binary_path)


@dataclass
class NatsTestServer:
    tcp_url: str
    ws_url: str
    _tcp_port: int
    _ws_port: int
    _workdir: Path
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
        config.write_text(
            f"port: {self._tcp_port}\n"
            "server_name: kinopio-test\n"
            "websocket {\n"
            f"  port: {self._ws_port}\n"
            "  no_tls: true\n"
            "}\n",
            encoding="utf-8",
        )

        binary = shutil.which("nats-server")
        if binary is None:
            binary = _download_nats_server_binary(Path.cwd() / ".pytest-nats-tools")
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
