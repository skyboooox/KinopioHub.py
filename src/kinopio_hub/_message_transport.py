"""nats-py header adapter; framing and transport remain owned by nats-py."""
from __future__ import annotations

from email.parser import BytesHeaderParser
from typing import Any

from nats.aio.client import Client

from ._messaging import Headers, MessageError
from ._compat import timeout as async_timeout


class HeaderFailure(dict[str, str]):
    def __init__(self, error: Exception):
        super().__init__({"invalid": ""})
        self.kinopio_error = error


class MessageClient(Client):
    def __init__(self) -> None:
        super().__init__()
        self.message_callbacks: dict[int, Any] = {}

    async def flush(self, timeout: float = 10) -> None:  # type: ignore[override]
        # nats-py 2.15 writes PING directly while SUB/PUB may still be queued.
        # Flush its native command queue first so PONG is a real write barrier.
        async with async_timeout(timeout):
            await self._flush_pending(force_flush=True)
            await super().flush(timeout=timeout)  # type: ignore[arg-type]

    async def _process_msg(self, sid: int, subject: bytes, reply: bytes, data: bytes, headers: bytes) -> None:
        callback = self.message_callbacks.get(sid)
        if callback is None:
            await super()._process_msg(sid, subject, reply, data, headers)
            return
        sub = self._subs.get(sid)
        if sub is None:
            return
        # This admission callback never awaits application code. Native framing,
        # header processing and Msg ownership remain nats-py's responsibility;
        # one bounded SDK queue replaces a second native business-message queue.
        self.stats['in_msgs'] += 1
        self.stats['in_bytes'] += len(data)
        sub._received += 1
        msg = self._build_message(sid, subject, reply, data, await self._process_headers(headers))
        if msg is not None:
            await callback(msg)

    async def _process_headers(self, headers: Any) -> Any:
        if headers and len(headers) > 4096:
            return HeaderFailure(MessageError('MESSAGE_TOO_LARGE', 'Headers exceed 4 KiB'))
        parsed = await super()._process_headers(headers)
        if not headers:
            return parsed
        # nats-py's dict representation loses exact duplicate keys. Reuse the
        # stdlib header parser solely for metadata; nats-py has already framed it.
        _, _, block = headers.partition(b'\r\n')
        metadata = BytesHeaderParser().parsebytes(block)
        if metadata.defects:
            return HeaderFailure(MessageError('INVALID_HEADERS', 'Malformed NATS Headers'))
        entries = list(metadata.raw_items())
        if headers.split(b'\r\n', 1)[0].startswith(b'NATS/1.0 ') and parsed and parsed.get('Status'):
            entries.insert(0, ('Status', parsed['Status']))
        try:
            return Headers(entries)
        except Exception as error:
            # Preserve invalid metadata for message-layer validation. Returning a
            # sentinel avoids exceptions interrupting the native parser loop.
            return HeaderFailure(error)
