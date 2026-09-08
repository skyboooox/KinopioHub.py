import ssl

import pytest

from kinopio_hub import KinopioError
from kinopio_hub._transport import endpoint, tls_context


@pytest.mark.parametrize("url", ["http://localhost", "nats://user:pass@localhost", "nats://localhost?token=x", "nats://", "nats://localhost:bad"])
def test_reject_invalid_endpoints(url):
    with pytest.raises(KinopioError):
        endpoint(url)


def test_tls_defaults_verify_certificates():
    context = tls_context(True)
    assert context is not None
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname
    assert endpoint("wss://example.com/nats") == "wss://example.com/nats"


@pytest.mark.parametrize("scheme", ["nats", "ws"])
async def test_blackhole_connect_has_total_deadline(scheme):
    import asyncio
    from kinopio_hub._transport import connect
    closed = asyncio.Event()
    async def blackhole(reader, writer):
        try:
            await reader.read()
        finally:
            writer.close()
            await writer.wait_closed()
            closed.set()
    server = await asyncio.start_server(blackhole, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        with pytest.raises(asyncio.TimeoutError):
            await connect(f"{scheme}://127.0.0.1:{port}", {"timeout": .05})
        await asyncio.wait_for(closed.wait(), 2)
    finally:
        server.close()
        await server.wait_closed()
