
from kinopio_hub._compat import timeout as async_timeout
import asyncio
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import sys

from aiohttp import web
import nats
import pytest

from kinopio_hub import _broker
from kinopio_hub._broker import ensure_managed_broker, managed_broker_config, start_managed_broker
from kinopio_hub._protocol import KinopioError


async def until(check):
    async with async_timeout(8):
        while not await check():
            await asyncio.sleep(0.04)


@pytest.fixture
async def binary():
    return await ensure_managed_broker()


@pytest.mark.parametrize('options', [dict(token='t', user='u', password='p'), dict(user='u'), dict(token=''), dict(authenticator=True), dict(upstreams=['tls://host:7422', 'nats://host:7422']), dict(upstreams=['nats://u:p@host:7422']), dict(upstreams=['tls://host:7422'], upstream_tls={'reject_unauthorized': False})])
async def test_invalid_options(options):
    with pytest.raises(KinopioError) as error:
        await start_managed_broker(**options)
    assert error.value.code == 'INVALID_OPTIONS'


async def test_config_and_version():
    config = managed_broker_config(dict(token='secret', upstreams=['tls://host:7422'], upstream_tls={'handshake_first': True, 'ca_file': '/ca.pem'}), '/tmp/runtime')
    assert config['leafnodes']['remotes'] == [{'urls': ['nats-leaf://secret@host:7422'], 'no_randomize': True, 'tls': {'handshake_first': True, 'ca_file': '/ca.pem'}}]
    assert config['http'] == '127.0.0.1:-1'
    assert 'jetstream' not in config
    with pytest.raises(KinopioError):
        await ensure_managed_broker(binary=sys.executable)


@pytest.mark.parametrize('transport', ['tcp', 'ws'])
async def test_auth_leaf_and_cleanup(binary, transport):
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        leaf_port = listener.getsockname()[1]
    upstream = await start_managed_broker(binary=binary, host='127.0.0.1', leaf_port=leaf_port)
    local = None
    a = b = None
    try:
        assert await upstream.upstream_connected() is None
        endpoint = upstream.websocket_url if transport == 'ws' else f'nats://127.0.0.1:{leaf_port}'
        local = await start_managed_broker(binary=binary, host='127.0.0.1', token='secret', upstreams=[endpoint])
        await until(local.upstream_connected)
        async def ignore_error(error):
            pass
        with pytest.raises(nats.errors.Error):
            await nats.connect(local.url, allow_reconnect=False, connect_timeout=1, error_cb=ignore_error)
        a = await nats.connect(local.url, token='secret', allow_reconnect=False)
        b = await nats.connect(upstream.url, allow_reconnect=False)
        sub = await b.subscribe('test.forward')
        await b.flush()
        await asyncio.sleep(.1)
        await a.publish('test.forward', b'forwarded')
        await a.flush()
        assert (await sub.next_msg(timeout=3)).data == b'forwarded'
        await b.close()
        await upstream.close()
        async def disconnected():
            return not await local.upstream_connected()
        await until(disconnected)
        await a.close()
        await asyncio.gather(local.close(), local.close())
        with pytest.raises(ProcessLookupError):
            os.kill(local.pid, 0)
    finally:
        if a is not None:
            await a.close()
        if b is not None:
            await b.close()
        if local is not None:
            await local.close()
        await upstream.close()


async def test_cache_integrity_cancel_and_concurrency(binary, tmp_path, monkeypatch):
    directory = tmp_path / 'nats-server' / _broker.BROKER_VERSION / _broker._target()
    directory.mkdir(parents=True)
    executable = directory / binary.name
    shutil.copy2(binary, executable)
    (directory / 'binary.sha256').write_text(hashlib.sha256(executable.read_bytes()).hexdigest())
    calls = 0
    async def corrupt(url):
        nonlocal calls
        calls += 1
        return b'corrupted'
    monkeypatch.setattr(_broker, '_download_archive', corrupt)
    assert await asyncio.gather(*(ensure_managed_broker(cache_dir=tmp_path) for _ in range(4))) == [executable] * 4
    assert calls == 0
    executable.write_bytes(b'corrupted')
    with pytest.raises(KinopioError, match='checksum'):
        await ensure_managed_broker(cache_dir=tmp_path)
    entered = asyncio.Event()
    async def stalled(url):
        entered.set()
        await asyncio.Event().wait()
    monkeypatch.setattr(_broker, '_download_archive', stalled)
    task = asyncio.create_task(ensure_managed_broker(cache_dir=tmp_path))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not list(directory.glob('download-*'))


async def test_bounded_stream_download():
    release = asyncio.Event()
    async def handler(request):
        response = web.StreamResponse()
        await response.prepare(request)
        await response.write(b'x' * 20)
        await release.wait()
        return response
    app = web.Application()
    app.router.add_get('/', handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 0)
    await site.start()
    url = f'http://127.0.0.1:{runner.addresses[0][1]}/'
    try:
        with pytest.raises(KinopioError, match='limit'):
            await _broker._download_archive(url, max_bytes=10)
        task = asyncio.create_task(_broker._download_archive(url))
        await asyncio.sleep(.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        release.set()
        await runner.cleanup()


@pytest.mark.skipif(sys.platform == 'win32', reason='SIGKILL is POSIX-only')
async def test_watchdog_parent_sigkill(binary):
    code = "import asyncio,json; from kinopio_hub._broker import start_managed_broker\nasync def main():\n b=await start_managed_broker(binary=" + repr(str(binary)) + ",host='127.0.0.1'); print(json.dumps({'pid':b.pid,'directory':str(b._directory)}),flush=True); await asyncio.Event().wait()\nasyncio.run(main())"
    parent = await asyncio.create_subprocess_exec(sys.executable, '-c', code, stdout=asyncio.subprocess.PIPE)
    try:
        assert parent.stdout is not None
        info = json.loads(await asyncio.wait_for(parent.stdout.readline(), 10))
        os.kill(info['pid'], 0)
        parent.send_signal(signal.SIGKILL)
        await parent.wait()
        async def gone():
            try:
                os.kill(info['pid'], 0)
                return False
            except ProcessLookupError:
                return not Path(info['directory']).exists()
        await until(gone)
    finally:
        if parent.returncode is None:
            parent.kill()
            await parent.wait()


async def test_tls_first_leaf_verifies_ca(binary, tmp_path):
    if shutil.which('openssl') is None:
        pytest.skip('OpenSSL is required for the TLS fixture')
    cert, key = tmp_path / 'cert.pem', tmp_path / 'key.pem'
    generator = await asyncio.create_subprocess_exec('openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-keyout', str(key), '-out', str(cert), '-days', '1', '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost,IP:127.0.0.1', stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
    assert await asyncio.wait_for(generator.wait(), 10) == 0
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        leaf_port = listener.getsockname()[1]
    config = tmp_path / 'upstream.json'
    config.write_text(json.dumps({'host': '127.0.0.1', 'port': -1, 'leafnodes': {'host': '127.0.0.1', 'port': leaf_port, 'tls': {'cert_file': str(cert), 'key_file': str(key), 'handshake_first': True}}}))
    upstream = await asyncio.create_subprocess_exec(str(binary), '-c', str(config), stderr=asyncio.subprocess.PIPE)
    untrusted = trusted = None
    try:
        assert upstream.stderr is not None
        async with async_timeout(5):
            while b'Server is ready' not in await upstream.stderr.readline():
                assert upstream.returncode is None
        options = dict(binary=binary, host='127.0.0.1', upstreams=[f'tls://127.0.0.1:{leaf_port}'])
        untrusted = await start_managed_broker(**options, upstream_tls={'handshake_first': True})
        await asyncio.sleep(.25)
        assert await untrusted.upstream_connected() is False
        trusted = await start_managed_broker(**options, upstream_tls={'handshake_first': True, 'ca_file': str(cert)})
        await until(trusted.upstream_connected)
    finally:
        if trusted:
            await trusted.close()
        if untrusted:
            await untrusted.close()
        if upstream.returncode is None:
            upstream.terminate()
            await asyncio.wait_for(upstream.wait(), 3)


async def test_start_cancellation_after_watchdog_spawn(binary, monkeypatch):
    original = asyncio.create_subprocess_exec
    spawned = asyncio.Event()
    owned = []
    async def delayed(*args, **kwargs):
        child = await original(*args, **kwargs)
        if '_watchdog.py' in str(args):
            owned.append(child)
            spawned.set()
            await asyncio.sleep(.1)
        return child
    monkeypatch.setattr(asyncio, 'create_subprocess_exec', delayed)
    task = asyncio.create_task(start_managed_broker(binary=binary, host='127.0.0.1'))
    await spawned.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert owned[0].returncode == 0


async def test_concurrent_install_atomic_publication(binary, tmp_path, monkeypatch):
    import io
    import tarfile
    buffer = io.BytesIO()
    payload = binary.read_bytes()
    target = _broker._target()
    with tarfile.open(fileobj=buffer, mode='w:gz') as archive:
        entry = tarfile.TarInfo(f'nats-server-v{_broker.BROKER_VERSION}-{target}/{binary.name}')
        entry.size = len(payload)
        archive.addfile(entry, io.BytesIO(payload))
        unrelated = tarfile.TarInfo('../../must-not-extract')
        unrelated.size = 1
        archive.addfile(unrelated, io.BytesIO(b'x'))
    data = buffer.getvalue()
    monkeypatch.setattr(_broker, 'HASHES', {target: hashlib.sha256(data).hexdigest()})
    async def download(url):
        await asyncio.sleep(.02)
        return data
    monkeypatch.setattr(_broker, '_download_archive', download)
    paths = await asyncio.gather(*(ensure_managed_broker(cache_dir=tmp_path) for _ in range(4)))
    assert len(set(paths)) == 1
    assert paths[0].read_bytes() == payload
    assert not list(tmp_path.rglob('download-*'))
    assert not list(tmp_path.rglob('must-not-extract'))
    assert await ensure_managed_broker(cache_dir=tmp_path) == paths[0]
