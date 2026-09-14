"""Bounded NATS transports; reconnection belongs to the hub."""
from __future__ import annotations

from kinopio_hub._compat import timeout as async_timeout

import asyncio
import ssl
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from nats.aio.client import Client
from ._message_transport import MessageClient

from ._protocol import PROTOCOL, KinopioError


def endpoint(url: str) -> str:
    try:
        parsed = urlsplit(url)
        if parsed.scheme not in ("nats", "tls", "ws", "wss") or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError
        parsed.port
        return urlunsplit(parsed)
    except (ValueError, TypeError) as error:
        raise KinopioError("INVALID_SERVER", "Use a nats/tls/ws/wss URL; credentials belong in auth options") from error


def display_endpoint(url: str | None) -> str | None:
    if url is None:
        return None
    parsed = urlsplit(url)
    return f"{parsed.scheme}://{parsed.netloc}"


def tls_context(settings: Any) -> ssl.SSLContext | None:
    if isinstance(settings, ssl.SSLContext):
        return settings
    if settings is None or settings is False:
        return None
    if settings is True:
        return ssl.create_default_context()
    if not isinstance(settings, dict):
        raise KinopioError("INVALID_OPTIONS", "tls must be an SSLContext or a mapping")
    context = ssl.create_default_context(cafile=settings.get("ca_file"))
    if settings.get("cert_file"):
        context.load_cert_chain(settings["cert_file"], settings.get("key_file"))
    return context


async def connect(url: str, options: dict[str, Any], **callbacks: Any) -> Client:
    url = endpoint(url)
    tls = options.get("tls")
    context = tls_context(tls)
    if url.startswith(("tls:", "wss:")) and context is None:
        context = ssl.create_default_context()
    kwargs: dict[str, Any] = {
        "servers": [url], "allow_reconnect": False,
        "connect_timeout": options.get("timeout", 3), "name": "KinopioHub Python",
        "token": options.get("token"), "user": options.get("user"),
        "password": options.get("password"), "tls": context,
        "tls_handshake_first": isinstance(tls, dict) and tls.get("handshake_first", False),
    }
    for field in ("user_credentials", "nkeys_seed"):
        if options.get(field) is not None:
            kwargs[field] = options[field]
    kwargs.update(callbacks)
    client = MessageClient()
    try:
        async with async_timeout(options.get("timeout", 3)):
            await client.connect(**kwargs)
        return client
    except BaseException:
        await close(client)
        raise


async def discover(callback: Any, on_error: Any) -> asyncio.DatagramTransport:
    """Receive the same bounded IPv4 discovery hints as the Node SDK."""
    import socket
    import psutil
    from ._protocol import decode

    class Receiver(asyncio.DatagramProtocol):
        def datagram_received(self, data: bytes, addr: Any) -> None:
            if len(data) > 4096:
                return
            try:
                value = decode(data)
                if not isinstance(value, dict) or value.get("kind") != "kinopio-edge" or value.get("protocol") != PROTOCOL:
                    return
                host = urlsplit(value["url"]).hostname or ""
                if (host in ("localhost", "::1") or host.startswith("127.")) and not addr[0].startswith("127."):
                    return
                callback(value)
            except Exception:
                pass

        def error_received(self, exc: Exception) -> None:
            on_error(exc)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("0.0.0.0", 44479))
        joined = False
        addresses = {entry.address for rows in psutil.net_if_addrs().values() for entry in rows if entry.family == socket.AF_INET}
        for address in addresses:
            try:
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton("239.255.42.99") + socket.inet_aton(address))
                joined = True
            except OSError:
                pass
        if not joined:
            raise OSError("No IPv4 multicast interface is available")
        sock.setblocking(False)
        transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(Receiver, sock=sock)
        return transport
    except BaseException:
        sock.close()
        raise


async def close(client: Client) -> None:
    """Bound closing, including nats-py 2.15's incomplete WebSocket handshake."""
    try:
        async with async_timeout(1):
            await client.close()
    except Exception:
        pass
    finally:
        # nats-py leaves its session open when cancellation precedes the WS response.
        transport = getattr(client, "_transport", None)
        session = getattr(transport, "_client", None)
        if session is not None and not session.closed:
            await session.close()
