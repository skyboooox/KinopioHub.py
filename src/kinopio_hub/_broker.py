"""Pinned NATS Core installation and independently supervised broker lifetime."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import re
import shutil
import sys
import tarfile
import tempfile
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit
import zipfile

import aiohttp

from ._protocol import KinopioError

BROKER_VERSION = '2.14.6'
HASHES = {
    'darwin-arm64': '291b9aa8c342b3cdcc53d872585bd467de4b5c9acf375066d882eb5412a09d55',
    'darwin-amd64': '239d4f3314334cc86cb12d7c252cf9b3461364451cc8ec5a39fb4915acd717c1',
    'linux-amd64': '61c3d55f69f61ec616b75782250936445f2819e9e5f2ae6159b10a31abd2200c',
    'linux-arm64': '3ff6e463762db64186a36cf0276dae8320509e995151ad0153ba9c9f67eee3f9',
    'windows-amd64': 'b47e9c69480e41e668e495e8b980b12dbf226d1ce7eceb9c44acdd33640bafcd',
    'windows-arm64': 'd7d7a4d22039f265f049591194f592bab742f33ed36236f4475f670a275946fb',
}


def _cache_root() -> Path:
    if sys.platform == 'darwin':
        return Path.home() / 'Library/Caches/kinopio-hub'
    if sys.platform == 'win32':
        return Path(os.environ.get('LOCALAPPDATA', str(Path.home() / 'AppData/Local'))) / 'kinopio-hub'
    return Path(os.environ.get('XDG_CACHE_HOME', str(Path.home() / '.cache'))) / 'kinopio-hub'


def _target() -> str:
    system = 'windows' if sys.platform == 'win32' else sys.platform
    arch = {'x86_64': 'amd64', 'AMD64': 'amd64', 'aarch64': 'arm64'}.get(platform.machine(), platform.machine())
    return f'{system}-{arch}'


async def _validate_binary(binary: Path) -> Path:
    child = None
    spawn = asyncio.create_task(asyncio.create_subprocess_exec(str(binary), '-v', stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT))
    try:
        child = await asyncio.shield(spawn)
        output, _ = await asyncio.wait_for(child.communicate(), 5)
        if child.returncode or not re.search(rb'\bv' + re.escape(BROKER_VERSION.encode()) + rb'\b', output):
            raise ValueError('Unexpected version')
        return binary
    except (OSError, ValueError, asyncio.TimeoutError) as exc:
        raise KinopioError('BROKER_VERSION', f'Managed broker must be executable NATS v{BROKER_VERSION}') from exc
    finally:
        if child is None and not spawn.cancelled():
            try:
                child = await spawn
            except OSError:
                pass
        if child is not None and child.returncode is None:
            child.kill()
            await child.wait()


async def _download_archive(url: str, max_bytes: int = 64 * 1024 * 1024) -> bytes:
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise KinopioError('DOWNLOAD_FAILED', 'NATS archive download failed')
                if response.content_length is not None and response.content_length > max_bytes:
                    raise KinopioError('DOWNLOAD_TOO_LARGE', 'NATS archive exceeds download limit')
                data = bytearray()
                async for chunk in response.content.iter_chunked(65536):
                    data.extend(chunk)
                    if len(data) > max_bytes:
                        raise KinopioError('DOWNLOAD_TOO_LARGE', 'NATS archive exceeds download limit')
                return bytes(data)
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        raise KinopioError('DOWNLOAD_FAILED', 'NATS archive download failed or timed out') from exc


async def ensure_managed_broker(*, binary: str | Path | None = None, cache_dir: str | Path | None = None) -> Path:
    if binary is not None:
        if not isinstance(binary, (str, Path)) or not str(binary) or '\0' in str(binary):
            raise KinopioError('INVALID_OPTIONS', 'binary must be an executable path')
        return await _validate_binary(Path(binary).resolve())
    target = _target()
    if target not in HASHES:
        raise KinopioError('UNSUPPORTED_PLATFORM', 'Provide a compatible binary on this platform')
    directory = Path(cache_dir or _cache_root()).resolve() / 'nats-server' / BROKER_VERSION / target
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    executable = directory / ('nats-server.exe' if sys.platform == 'win32' else 'nats-server')
    metadata = directory / 'binary.sha256'
    try:
        if hashlib.sha256(executable.read_bytes()).hexdigest() == metadata.read_text().strip():
            return await _validate_binary(executable)
    except (OSError, KinopioError):
        pass
    staging = Path(tempfile.mkdtemp(prefix='download-', dir=directory))
    try:
        stem = f'nats-server-v{BROKER_VERSION}-{target}'
        filename = stem + ('.zip' if sys.platform == 'win32' else '.tar.gz')
        data = await _download_archive(f'https://github.com/nats-io/nats-server/releases/download/v{BROKER_VERSION}/{filename}')
        if hashlib.sha256(data).hexdigest() != HASHES[target]:
            raise KinopioError('CHECKSUM_FAILED', 'NATS archive checksum does not match the pinned release')
        member = f'{stem}/{executable.name}'
        if sys.platform == 'win32':
            with zipfile.ZipFile(io.BytesIO(data)) as archive:
                payload = archive.read(member)
        else:
            with tarfile.open(fileobj=io.BytesIO(data), mode='r:gz') as tar:
                entry = tar.getmember(member)
                if not entry.isfile():
                    raise KinopioError('CHECKSUM_FAILED', 'Archive binary must be a regular file')
                stream = tar.extractfile(entry)
                if stream is None:
                    raise KinopioError('CHECKSUM_FAILED', 'Archive binary is missing')
                payload = stream.read()
        extracted = staging / executable.name
        extracted.write_bytes(payload)
        extracted.chmod(0o700)
        await _validate_binary(extracted)
        digest = hashlib.sha256(payload).hexdigest()
        try:
            os.replace(extracted, executable)
        except OSError:
            if hashlib.sha256(executable.read_bytes()).hexdigest() != digest:
                raise
        staged_metadata = staging / metadata.name
        staged_metadata.write_text(digest)
        staged_metadata.chmod(0o600)
        try:
            os.replace(staged_metadata, metadata)
        except OSError:
            if metadata.read_text().strip() != digest:
                raise
        return executable
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def managed_broker_config(options: dict[str, Any], directory: str) -> dict[str, Any]:
    def invalid(message: str) -> None:
        raise KinopioError('INVALID_OPTIONS', message)
    for key in ('authenticator', 'authorization', 'credentials', 'nkey', 'jwt', 'tls', 'tls_cert', 'tls_key'):
        if options.get(key) is not None:
            invalid(f'Managed broker does not support {key}')
    token, user, password = (options.get(key) for key in ('token', 'user', 'password'))
    if token is not None and (user is not None or password is not None):
        invalid('Use either token or user/password authentication')
    for value in (token, user, password):
        if value is not None and (not isinstance(value, str) or not value):
            invalid('Authentication values must be nonempty strings')
    if (user is None) != (password is None):
        invalid('Managed broker requires both user and password')
    host = options.get('host', '0.0.0.0')
    if not isinstance(host, str) or not host or '\0' in host:
        invalid('Invalid broker bind host')
    for key in ('port', 'ws_port', 'leaf_port'):
        value = options.get(key)
        if value is not None and (type(value) is not int or not 0 <= value <= 65535):
            invalid(f'Invalid {key}')
    config: dict[str, Any] = {'host': host, 'port': options.get('port') or -1, 'http': '127.0.0.1:-1', 'ports_file_dir': directory, 'max_payload': 1024 * 1024, 'websocket': {'host': host, 'port': options.get('ws_port') or -1, 'no_tls': True}}
    if token is not None:
        config['authorization'] = {'token': token}
    if user is not None:
        config['authorization'] = {'user': user, 'password': password}
    if options.get('leaf_port') is not None:
        config['leafnodes'] = {'host': '127.0.0.1', 'port': options['leaf_port'] or -1}
    upstreams = options.get('upstreams', [])
    tls_options = options.get('upstream_tls')
    if not isinstance(upstreams, list) or len(upstreams) > 32:
        invalid('upstreams must be a list of at most 32 leaf endpoints')
    urls, modes = [], set()
    for value in upstreams:
        try:
            if not isinstance(value, str):
                raise ValueError()
            url = urlsplit(value)
            if url.scheme not in ('nats', 'nats-leaf', 'tls', 'ws', 'wss') or not url.hostname or url.fragment or url.query:
                raise ValueError()
            _ = url.port
        except ValueError:
            invalid('Invalid leaf endpoint URL')
        if url.username is not None or url.password is not None:
            invalid('Provide leaf authentication using token or user/password options, not URL credentials')
        secure, ws = url.scheme in ('tls', 'wss'), url.scheme in ('ws', 'wss')
        modes.add((ws, secure))
        auth = quote(token, safe='') + '@' if token is not None else (quote(user, safe='') + ':' + quote(str(password), safe='') + '@' if user is not None else '')
        urls.append(urlunsplit((url.scheme if ws else 'nats-leaf', auth + url.netloc, '' if ws and url.path == '/' else url.path, '', '')))
    if len(modes) > 1:
        invalid('Leaf alternatives must use the same transport and TLS mode')
    if urls:
        remote: dict[str, Any] = {'urls': urls, 'no_randomize': True}
        if next(iter(modes))[1] or tls_options is not None:
            if tls_options is not None and not isinstance(tls_options, dict):
                invalid('upstream_tls must contain TLS file options')
            tls = dict(tls_options or {})
            for key, value in tls.items():
                if key not in ('handshake_first', 'ca_file', 'cert_file', 'key_file') or (type(value) is not bool if key == 'handshake_first' else not isinstance(value, str) or not value):
                    invalid('Supported upstream_tls options are handshake_first, ca_file, cert_file, key_file')
            if bool(tls.get('cert_file')) != bool(tls.get('key_file')):
                invalid('Provide both TLS cert_file and key_file')
            remote['tls'] = tls
        config.setdefault('leafnodes', {})['remotes'] = [remote]
    elif tls_options is not None:
        invalid('upstream_tls requires leaf upstreams')
    return config


@dataclass
class ManagedBroker:
    url: str
    websocket_url: str
    port: int
    ws_port: int
    pid: int
    _child: asyncio.subprocess.Process = field(repr=False)
    _directory: Path = field(repr=False)
    _monitor: str = field(repr=False)
    _has_upstreams: bool = field(repr=False)
    _close_task: asyncio.Task[None] | None = field(default=None, repr=False)

    async def _close(self) -> None:
        if self._child.stdin is not None:
            self._child.stdin.close()
        await self._child.wait()
        shutil.rmtree(self._directory, ignore_errors=True)

    async def close(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await asyncio.shield(self._close_task)

    async def upstream_connected(self) -> bool | None:
        if self._close_task is not None or self._child.returncode is not None:
            return False
        if not self._has_upstreams:
            return None
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=1)) as session:
                async with session.get(self._monitor + '/leafz') as response:
                    return response.status == 200 and (await response.json()).get('leafnodes', 0) > 0
        except (aiohttp.ClientError, ValueError, asyncio.TimeoutError):
            return False


async def start_managed_broker(*, binary: str | Path | None = None, host: str = '0.0.0.0', token: str | None = None, user: str | None = None, password: str | None = None, upstreams: list[str] | None = None, upstream_tls: dict[str, Any] | None = None, cache_dir: str | Path | None = None, port: int = 0, ws_port: int = 0, leaf_port: int | None = None, **unsupported: Any) -> ManagedBroker:
    options = dict(host=host, token=token, user=user, password=password, upstreams=[] if upstreams is None else upstreams, upstream_tls=upstream_tls, port=port, ws_port=ws_port, leaf_port=leaf_port, **unsupported)
    if unsupported:
        raise KinopioError('INVALID_OPTIONS', f'Unsupported managed broker options: {", ".join(unsupported)}')
    managed_broker_config(options, '')
    executable = await ensure_managed_broker(binary=binary, cache_dir=cache_dir)
    directory = Path(tempfile.mkdtemp(prefix='kinopio-managed-'))
    child = None
    spawn_task = None
    try:
        config = directory / 'nats.json'
        with os.fdopen(os.open(config, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as output:
            json.dump(managed_broker_config(options, str(directory)), output)
        spawn_task = asyncio.create_task(asyncio.create_subprocess_exec(sys.executable, str(Path(__file__).with_name('_watchdog.py')), str(directory), stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL))
        child = await asyncio.shield(spawn_task)
        assert child.stdin is not None and child.stdout is not None
        child.stdin.write((json.dumps({'binary': str(executable), 'config': str(config)}) + '\n').encode())
        await child.stdin.drain()
        message = await asyncio.wait_for(child.stdout.readline(), 5)
        if not message:
            raise KinopioError('BROKER_FAILED', 'Managed NATS broker failed to start')
        pid = json.loads(message)['pid']
        for _ in range(200):
            if child.returncode is not None:
                raise KinopioError('BROKER_FAILED', 'Managed NATS broker exited')
            for file in directory.glob('*.ports'):
                try:
                    ports = json.loads(file.read_text())
                    tcp_port = int(urlsplit(ports['nats'][0]).port or 0)
                    websocket_port = int(urlsplit(ports['websocket'][0]).port or 0)
                    monitor_port = urlsplit(ports['monitoring'][0]).port
                except (OSError, ValueError, KeyError, IndexError):
                    continue
                address = '::1' if host == '::' else '127.0.0.1' if host == '0.0.0.0' else host
                if ':' in address:
                    address = f'[{address}]'
                return ManagedBroker(f'nats://{address}:{tcp_port}', f'ws://{address}:{websocket_port}', tcp_port, websocket_port, pid, child, directory, f'http://127.0.0.1:{monitor_port}', bool(upstreams))
            await asyncio.sleep(0.025)
        raise KinopioError('BROKER_TIMEOUT', 'Managed NATS broker startup timed out')
    except BaseException:
        async def cleanup() -> None:
            nonlocal child
            try:
                if child is None and spawn_task is not None:
                    child = await spawn_task
                if child is not None:
                    if child.stdin is not None:
                        child.stdin.close()
                    await child.wait()
            finally:
                shutil.rmtree(directory, ignore_errors=True)
        await asyncio.shield(asyncio.create_task(cleanup()))
        raise
