from __future__ import annotations

import inspect
import json
import os
import platform
import shutil
import stat
import tarfile
import urllib.request
import zipfile
from pathlib import Path
from typing import Final
from urllib.parse import urlparse

_LATEST_RELEASE_API: Final[str] = "https://api.github.com/repos/nats-io/nats-server/releases/latest"
_RELEASES_DOWNLOAD_PREFIX: Final[str] = (
    "https://github.com/nats-io/nats-server/releases/download"
)
_CACHE_ENV_VAR: Final[str] = "KINOPIO_HUB_CACHE_DIR"
_DEFAULT_BINARY_NAME: Final[str] = "nats-server"
_ARCHIVE_LAYOUT: Final[dict[tuple[str, str], tuple[str, str, str]]] = {
    ("darwin", "arm64"): ("darwin-arm64", "tar.gz", "nats-server"),
    ("darwin", "x86_64"): ("darwin-amd64", "tar.gz", "nats-server"),
    ("linux", "aarch64"): ("linux-arm64", "tar.gz", "nats-server"),
    ("linux", "x86_64"): ("linux-amd64", "tar.gz", "nats-server"),
    ("windows", "amd64"): ("windows-amd64", "zip", "nats-server.exe"),
    ("windows", "x86_64"): ("windows-amd64", "zip", "nats-server.exe"),
}


def kinopio_cache_root() -> Path:
    overridden = os.environ.get(_CACHE_ENV_VAR)
    if overridden:
        return Path(overridden).expanduser()

    system = platform.system().lower()
    home = Path.home()
    if system == "darwin":
        return home / "Library" / "Caches" / "kinopio-hub"
    if system == "windows":
        local_app_data = os.environ.get("LOCALAPPDATA")
        if local_app_data:
            return Path(local_app_data) / "kinopio-hub"
    xdg_cache_home = os.environ.get("XDG_CACHE_HOME")
    if xdg_cache_home:
        return Path(xdg_cache_home) / "kinopio-hub"
    return home / ".cache" / "kinopio-hub"


def nats_server_cache_dir() -> Path:
    return kinopio_cache_root() / "nats-server"


def resolve_nats_server_binary(
    *,
    binary_path: str | os.PathLike[str] | None = None,
    cache_dir: str | os.PathLike[str] | None = None,
) -> Path:
    if binary_path is not None:
        return _validate_binary_path(Path(binary_path).expanduser())

    discovered = shutil.which(_DEFAULT_BINARY_NAME)
    if discovered is not None:
        return Path(discovered)

    resolved_cache_dir = (
        Path(cache_dir).expanduser() if cache_dir is not None else nats_server_cache_dir()
    )
    cached_binary = _find_cached_binary(resolved_cache_dir)
    if cached_binary is not None:
        return cached_binary

    version = latest_stable_nats_server_version()
    return download_nats_server_binary(version=version, cache_dir=resolved_cache_dir)


def latest_stable_nats_server_version() -> str:
    try:
        request = urllib.request.Request(
            _LATEST_RELEASE_API,
            headers={"Accept": "application/json", "User-Agent": "kinopio-hub"},
        )
        with urllib.request.urlopen(request, timeout=15) as response:
            payload = json.load(response)

        tag_name = str(payload.get("tag_name", "")).strip()
        if tag_name:
            return tag_name[1:] if tag_name.startswith("v") else tag_name
    except Exception:
        pass

    request = urllib.request.Request(
        "https://github.com/nats-io/nats-server/releases/latest",
        headers={"User-Agent": "kinopio-hub"},
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        final_url = response.geturl()

    tag_name = Path(urlparse(final_url).path).name.strip()
    if not tag_name:
        raise RuntimeError("Unable to determine the latest stable nats-server release")
    return tag_name[1:] if tag_name.startswith("v") else tag_name


def download_nats_server_binary(
    *,
    version: str,
    cache_dir: str | os.PathLike[str] | None = None,
) -> Path:
    resolved_cache_dir = (
        Path(cache_dir).expanduser() if cache_dir is not None else nats_server_cache_dir()
    )
    resolved_cache_dir.mkdir(parents=True, exist_ok=True)

    platform_tag, archive_ext, binary_name = _resolve_archive_layout()
    release_dir = resolved_cache_dir / f"nats-server-v{version}-{platform_tag}"
    binary_path = release_dir / binary_name
    if binary_path.exists():
        return _validate_binary_path(binary_path)

    archive_name = f"nats-server-v{version}-{platform_tag}.{archive_ext}"
    archive_path = resolved_cache_dir / archive_name
    if not archive_path.exists():
        request = urllib.request.Request(
            f"{_RELEASES_DOWNLOAD_PREFIX}/v{version}/{archive_name}",
            headers={"User-Agent": "kinopio-hub"},
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            archive_path.write_bytes(response.read())

    if archive_ext == "tar.gz":
        with tarfile.open(archive_path, "r:gz") as archive:
            _safe_extract_tar(archive, resolved_cache_dir)
    else:
        with zipfile.ZipFile(archive_path) as archive:
            _safe_extract_zip(archive, resolved_cache_dir)

    if not binary_path.exists():
        raise RuntimeError(f"Downloaded archive did not contain {binary_name}")

    if platform.system().lower() != "windows":
        binary_path.chmod(
            binary_path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
        )
    return binary_path


def _validate_binary_path(path: Path) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"nats-server binary was not found: {path}")
    if not path.is_file():
        raise RuntimeError(f"nats-server binary path is not a file: {path}")
    return path


def _resolve_archive_layout() -> tuple[str, str, str]:
    system = platform.system().lower()
    machine = platform.machine().lower()
    layout = _ARCHIVE_LAYOUT.get((system, machine))
    if layout is None:
        raise RuntimeError(
            f"Automatic nats-server download is not supported on {system}/{machine}"
        )
    return layout


def _find_cached_binary(cache_dir: Path) -> Path | None:
    if not cache_dir.exists():
        return None

    cached_binaries: list[tuple[tuple[int, ...], Path]] = []
    for candidate in cache_dir.iterdir():
        if not candidate.is_dir():
            continue
        version = _parse_cached_version(candidate.name)
        if version is None:
            continue
        binary = candidate / _cached_binary_name()
        if binary.exists():
            cached_binaries.append((version, binary))

    if not cached_binaries:
        return None

    cached_binaries.sort(key=lambda item: item[0], reverse=True)
    return cached_binaries[0][1]


def _cached_binary_name() -> str:
    try:
        _, _, binary_name = _resolve_archive_layout()
    except RuntimeError:
        return _DEFAULT_BINARY_NAME
    return binary_name


def _parse_cached_version(name: str) -> tuple[int, ...] | None:
    if not name.startswith("nats-server-v"):
        return None

    version_and_platform = name[len("nats-server-v") :]
    if "-" not in version_and_platform:
        return None

    version, _, _ = version_and_platform.partition("-")
    parts = version.split(".")
    parsed: list[int] = []
    for part in parts:
        if not part.isdigit():
            return None
        parsed.append(int(part))
    return tuple(parsed)


def _safe_extract_tar(archive: tarfile.TarFile, destination: Path) -> None:
    destination_root = destination.resolve()
    for member in archive.getmembers():
        target = (destination / member.name).resolve()
        if not str(target).startswith(str(destination_root)):
            raise RuntimeError("Refusing to extract archive outside cache directory")
    if "filter" in inspect.signature(archive.extractall).parameters:
        archive.extractall(destination, filter="data")
    else:
        archive.extractall(destination)


def _safe_extract_zip(archive: zipfile.ZipFile, destination: Path) -> None:
    destination_root = destination.resolve()
    for member in archive.namelist():
        target = (destination / member).resolve()
        if not str(target).startswith(str(destination_root)):
            raise RuntimeError("Refusing to extract archive outside cache directory")
    archive.extractall(destination)
