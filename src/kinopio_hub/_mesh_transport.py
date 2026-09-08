"""IPv4 discovery/control I/O and local network/load measurements."""
from __future__ import annotations

from kinopio_hub._compat import timeout as async_timeout

import asyncio
import json
import socket
import time
from typing import Any, Callable
from urllib.parse import urlsplit

import aiohttp
from aiohttp import web
import psutil

from ._protocol import js_stringify

GROUP, MAX_BODY = '239.255.42.100', 16384


def ipv4() -> list[str]:
    return list(dict.fromkeys(a.address for items in psutil.net_if_addrs().values() for a in items if a.family == socket.AF_INET))


class NativeTransport:
    def __init__(self, config: dict[str, Any], on_hint: Callable[..., Any], on_request: Callable[..., Any], on_error: Callable[..., Any], on_healthy: Callable[..., Any]):
        self.config, self.on_hint, self.on_request, self.on_error, self.on_healthy = config, on_hint, on_request, on_error, on_healthy
        self.closed = False
        self.interfaces: set[str] = set()
        self.udp: socket.socket | None = None
        self.runner: web.AppRunner | None = None
        self.session: aiohttp.ClientSession | None = None
        self.reader: asyncio.Task[Any] | None = None
        self.datagrams: asyncio.DatagramTransport | None = None
        self.incoming: asyncio.Queue[tuple[bytes, Any]] = asyncio.Queue(maxsize=128)

    async def start(self) -> int:
        app = web.Application(client_max_size=MAX_BODY)
        app.router.add_post('/kinopio-mesh/v1', self._request)
        self.runner = web.AppRunner(app, keepalive_timeout=self.config['probe_timeout_ms'] / 500, shutdown_timeout=0)
        await self.runner.setup()
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('0.0.0.0', self.config['control_port']))
        server.listen(128)
        server.setblocking(False)
        port = server.getsockname()[1]
        try:
            await web.SockSite(self.runner, server).start()
        except BaseException:
            server.close()
            raise
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.config['probe_timeout_ms'] / 1000), connector=aiohttp.TCPConnector(force_close=True, limit=128))
        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, 'SO_REUSEPORT'):
            self.udp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self.udp.bind(('0.0.0.0', self.config['discovery_port']))
        self.udp.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
        self.udp.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
        self.udp.setblocking(False)
        self.refresh_interfaces()
        owner = self

        class Receiver(asyncio.DatagramProtocol):
            def datagram_received(self, data: bytes, remote: Any) -> None:
                if len(data) <= 2048 and not owner.incoming.full():
                    owner.incoming.put_nowait((data, remote))

            def error_received(self, error: Exception) -> None:
                if not owner.closed:
                    owner.on_error(error, 'discovery')

        self.datagrams, _ = await asyncio.get_running_loop().create_datagram_endpoint(Receiver, sock=self.udp)
        self.reader = asyncio.create_task(self._read())
        return int(port)

    async def _request(self, request: web.Request) -> web.Response:
        try:
            async with async_timeout(self.config['probe_timeout_ms'] / 500):
                message = json.loads(await request.read())
            reply = self.on_request(message, request.remote)
            return web.Response(text=js_stringify(reply), content_type='application/json') if reply else web.Response(status=403)
        except (ValueError, asyncio.TimeoutError, web.HTTPRequestEntityTooLarge):
            return web.Response(status=400)

    def refresh_interfaces(self) -> None:
        assert self.udp is not None
        addresses = set(ipv4())
        for address in self.interfaces - addresses:
            try:
                self.udp.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP, socket.inet_aton(GROUP) + socket.inet_aton(address))
            except OSError:
                pass
        self.interfaces &= addresses
        for address in addresses - self.interfaces:
            try:
                self.udp.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton(GROUP) + socket.inet_aton(address))
                self.interfaces.add(address)
            except OSError:
                pass
        if not self.interfaces:
            raise OSError('No IPv4 multicast interface is available')

    async def _read(self) -> None:
        assert self.udp is not None
        while not self.closed:
            try:
                data, remote = await self.incoming.get()
                if len(data) <= 2048:
                    try:
                        self.on_hint(json.loads(data), remote[0])
                    except (ValueError, TypeError):
                        pass
            except OSError as error:
                if not self.closed:
                    self.on_error(error, 'discovery')
                    await asyncio.sleep(self.config['heartbeat_ms'] / 1000)

    async def announce(self, message: dict[str, Any]) -> None:
        if self.closed:
            return
        assert self.udp is not None
        try:
            self.refresh_interfaces()
            data = js_stringify(message).encode()
            for address in sorted(self.interfaces):
                self.udp.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(address))
                assert self.datagrams is not None
                self.datagrams.sendto(data, (GROUP, self.config['discovery_port']))
            self.on_healthy('discovery')
        except OSError as error:
            if not self.closed:
                self.on_error(error, 'discovery')

    async def probe(self, peer: dict[str, Any], message: dict[str, Any]) -> Any:
        assert self.session is not None
        async with self.session.post(f"http://{peer['address']}:{peer['port']}/kinopio-mesh/v1", data=js_stringify(message), headers={'content-type': 'application/json'}) as response:
            if response.status != 200:
                raise ValueError('Mesh authentication failed')
            body = bytearray()
            async for chunk in response.content.iter_chunked(4096):
                body.extend(chunk)
                if len(body) > MAX_BODY:
                    raise ValueError('Oversized mesh response')
            return json.loads(body)

    async def close(self) -> None:
        self.closed = True
        if self.reader:
            self.reader.cancel()
            await asyncio.gather(self.reader, return_exceptions=True)
        if self.datagrams:
            self.datagrams.close()
        elif self.udp:
            self.udp.close()
        if self.session:
            await self.session.close()
        if self.runner:
            await self.runner.cleanup()


async def broker_reachable(address: str, port: int, timeout_ms: int) -> bool:
    try:
        async with async_timeout(timeout_ms / 1000):
            _, writer = await asyncio.open_connection(address, port)
            writer.close()
            await writer.wait_closed()
        return True
    except (OSError, asyncio.TimeoutError):
        return False


async def probe_uplink(upstreams: list[str], timeout_ms: int) -> dict[str, Any]:
    async def probe(endpoint: str) -> float | None:
        parsed = urlsplit(endpoint)
        start = time.monotonic()
        port = parsed.port or (80 if parsed.scheme == 'ws' else 443 if parsed.scheme == 'wss' else 4222)
        if await broker_reachable(parsed.hostname or '', port, timeout_ms):
            return min(60000, (time.monotonic() - start) * 1000)
        return None
    samples = [sample for sample in await asyncio.gather(*(probe(u) for u in upstreams)) if sample is not None]
    return {'reachable': bool(samples), 'rtt': min(samples) if samples else None}


class LoadSampler:
    def __init__(self) -> None:
        self.previous = psutil.cpu_times()
        self.smoothed = {'cpu': 0.0, 'memory': 0.0}

    def __call__(self) -> dict[str, float]:
        current = psutil.cpu_times()
        elapsed = sum(current) - sum(self.previous)
        cpu = max(0, min(1, 1 - (current.idle - self.previous.idle) / elapsed)) if elapsed > 0 else self.smoothed['cpu']
        self.previous = current
        memory = psutil.virtual_memory()
        self.smoothed = {'cpu': self.smoothed['cpu'] * 0.7 + cpu * 0.3, 'memory': self.smoothed['memory'] * 0.7 + max(0, min(1, 1 - memory.free / memory.total)) * 0.3}
        return self.smoothed.copy()
