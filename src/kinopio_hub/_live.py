"""Ephemeral controls protected by receiver-issued monotonic leases."""
from __future__ import annotations

import asyncio
import inspect
import math
import time
from dataclasses import dataclass, field
from typing import Any, Callable, TYPE_CHECKING

from nats.errors import ConnectionClosedError

from . import _protocol as p
from ._compat import timeout as async_timeout

if TYPE_CHECKING:
    from ._hub import KinopioHub

MAX_PENDING = 32
MAX_BYTES = 256 * 1024


@dataclass(frozen=True)
class LiveContext:
    """Recheck immediately before applying a command queued by the callback."""

    expires_at: float
    _receiver: Receiver = field(repr=False)
    _session: str = field(repr=False)

    def is_valid(self) -> bool:
        return (time.monotonic() < self.expires_at and self._receiver.current()
                and self._receiver.session == self._session)


class LiveChannel:
    def __init__(self, hub: KinopioHub, name: str):
        self.hub = hub
        self.subject = f'{hub.base}.live.{p.token(name)}'
        self.receivers: list[Receiver] = []
        self.grant: dict[str, Any] | None = None
        self.sending = False
        self.aborted = False
        self.pending: asyncio.Task[Any] | None = None

    def invalidate(self) -> None:
        self.grant = None
        self.aborted = True
        if self.pending and self.pending is not asyncio.current_task():
            self.pending.cancel()
        for receiver in self.receivers:
            receiver.leases.clear()

    async def subscribe(self, callback: Callable[..., None], *, max_age_ms: int = 300, with_context: bool = False) -> Callable[[], Any]:
        self.hub._assert_open()
        if not callable(callback) or inspect.iscoroutinefunction(callback):
            raise TypeError('Expected a synchronous callback')
        if type(with_context) is not bool:
            raise p.KinopioError('INVALID_OPTIONS', 'with_context must be boolean')
        if type(max_age_ms) is not int or not 1 <= max_age_ms <= 60000:
            raise p.KinopioError('INVALID_OPTIONS', 'max_age_ms must be 1–60000')
        if self.receivers:
            raise p.KinopioError('ALREADY_SUBSCRIBED', 'A live control channel has one receiver')
        if sum(len(channel.receivers) for channel in self.hub._live_channels.values()) >= 32:
            raise p.KinopioError('LIMIT_EXCEEDED', 'At most 32 live subscriptions are supported')
        receiver = Receiver(self, callback, max_age_ms, with_context)
        self.receivers.append(receiver)
        try:
            if self.hub.connection.active:
                await receiver.bind(self.hub.connection.active)
        except BaseException:
            await receiver.close()
            raise
        return receiver.close

    async def send(self, value: Any, *, timeout: float = 3) -> None:
        self.hub._assert_open()
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
            raise p.KinopioError('INVALID_OPTIONS', 'timeout must be positive')
        value = p.value_of(value)
        candidate = self.hub.connection.active
        if candidate is None or not candidate['connection'].is_connected:
            raise p.KinopioError('DISCONNECTED', 'Live messages require an active connection')
        connection = candidate['connection']
        if self.sending:
            raise p.KinopioError('BUSY', 'A live send is already in progress')
        self.sending = True
        self.aborted = False
        self.pending = asyncio.current_task()
        sub = None
        try:
            async with async_timeout(timeout):
                grant = self.grant
                # Renew before sending when a cached lease cannot cover known transit time.
                reserve = max(grant['rtt'] if grant else 0, candidate['rtt'] / 1000) + .01
                if grant is None or grant['candidate'] is not candidate or time.monotonic() + reserve >= grant['expiry']:
                    self.grant = None
                    started = time.monotonic()
                    response: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
                    inbox = f'{self.subject}.inbox.{p.id()}'

                    async def receive(message: Any) -> None:
                        if response.done() or self.hub.connection.active is not candidate:
                            return
                        if message.headers and message.headers.get('Status') == '503':
                            response.set_exception(p.KinopioError('NO_RECEIVERS', 'No live receiver is subscribed'))
                            return
                        try:
                            reply = p.decode(message.data)
                            if (isinstance(reply, dict) and reply.get('protocol') == 1
                                and isinstance(reply.get('session'), str) and 1 <= len(reply['session']) <= 128
                                and isinstance(reply.get('lease'), str) and 1 <= len(reply['lease']) <= 128
                                and type(reply.get('ttl_ms')) is int and 1 <= reply['ttl_ms'] <= 60000):
                                response.set_result(reply)
                        except Exception:
                            return

                    sub = await connection.subscribe(inbox, cb=receive, pending_msgs_limit=MAX_PENDING, pending_bytes_limit=MAX_BYTES)
                    await connection.publish(f'{self.subject}.lease', p.encode({'protocol': 1}), reply=inbox)
                    reply = await response
                    grant = {**reply, 'candidate': candidate, 'expiry': started + reply['ttl_ms'] / 1000,
                             'rtt': time.monotonic() - started, 'seq': 0}
                    self.grant = grant
                if self.hub.connection.active is not candidate or not connection.is_connected:
                    raise p.KinopioError('DISCONNECTED', 'Connection changed during live send')
                if time.monotonic() >= grant['expiry']:
                    self.grant = None
                    raise p.KinopioError('EXPIRED', 'Receiver lease expired in transit')
                grant['seq'] += 1
                data = p.encode({'protocol': 1, 'session': grant['session'], 'lease': grant['lease'], 'seq': grant['seq'], 'value': value})
                if len(data) > connection.max_payload:
                    raise p.KinopioError('MESSAGE_TOO_LARGE', 'Live message exceeds broker payload limit')
                await connection.publish(f'{self.subject}.data', data)
                await connection.flush(timeout=timeout)
                if self.hub.connection.active is not candidate or not connection.is_connected:
                    raise p.KinopioError('DISCONNECTED', 'Connection changed during live confirmation')
        except asyncio.CancelledError as error:
            if self.aborted:
                raise p.KinopioError('CLOSED' if self.hub.closed else 'DISCONNECTED', 'Live send interrupted by connection lifecycle') from error
            raise
        except asyncio.TimeoutError as error:
            raise p.KinopioError('TIMEOUT', 'Live transport confirmation timed out') from error
        finally:
            self.sending = False
            self.pending = None
            if sub is not None:
                try:
                    await sub.unsubscribe()
                except ConnectionClosedError:
                    pass

    async def bind(self, candidate: Any) -> None:
        self.invalidate()
        for receiver in list(self.receivers):
            await receiver.bind(candidate)

    async def close(self) -> None:
        pending = self.pending
        self.invalidate()
        if pending and pending is not asyncio.current_task():
            await asyncio.gather(pending, return_exceptions=True)
        for receiver in list(self.receivers):
            await receiver.close()


class Receiver:
    def __init__(self, channel: LiveChannel, callback: Callable[..., None], max_age_ms: int, with_context: bool):
        self.channel, self.callback, self.max_age_ms = channel, callback, max_age_ms
        self.with_context = with_context
        self.candidate: Any = None
        self.session = ''
        self.leases: dict[str, tuple[float, int]] = {}
        self.subscriptions: list[Any] = []
        self.closed = False

    def current(self) -> bool:
        return (not self.closed and not self.channel.hub.closed and self.candidate is not None
                and self.channel.hub.connection.active is self.candidate
                and self.candidate['connection'].is_connected)

    async def bind(self, candidate: Any) -> None:
        if self.closed or self.candidate is candidate:
            return
        await self.unbind()
        self.candidate = candidate
        self.session = p.id()
        session = self.session
        connection = candidate['connection']

        async def lease(message: Any) -> None:
            if not self.current() or session != self.session or not message.reply.startswith(f'{self.channel.subject}.inbox.'):
                return
            try:
                request = p.decode(message.data)
                if not isinstance(request, dict) or request.get('protocol') != 1:
                    return
                now = time.monotonic()
                self.leases = {key: item for key, item in self.leases.items() if item[0] > now}
                if len(self.leases) >= 32:
                    self.leases.pop(next(iter(self.leases)))
                nonce = p.id()
                self.leases[nonce] = (now + self.max_age_ms / 1000, 0)
                await connection.publish(message.reply, p.encode({'protocol': 1, 'session': session, 'lease': nonce, 'ttl_ms': self.max_age_ms}))
            except Exception as error:
                self.channel.hub._error(error, 'live')

        async def data(message: Any) -> None:
            if not self.current() or session != self.session or len(message.data) > p.MAX_VALUE_BYTES + 1024:
                return
            try:
                value = p.decode(message.data)
                if not isinstance(value, dict) or value.get('protocol') != 1 or value.get('session') != session:
                    return
                nonce, seq = value.get('lease'), value.get('seq')
                if not isinstance(nonce, str) or type(seq) is not int or not 1 <= seq <= 9007199254740991:
                    return
                grant = self.leases.get(nonce)
                if grant is None or time.monotonic() >= grant[0] or seq <= grant[1]:
                    return
                payload = p.value_of(value['value'])
                self.leases[nonce] = (grant[0], seq)
                context = LiveContext(grant[0], self, session)
                if self.with_context:
                    self.channel.hub._invoke(self.callback, payload, context)
                else:
                    self.channel.hub._invoke(self.callback, payload)
            except Exception as error:
                self.channel.hub._error(error, 'live')

        try:
            for suffix, callback in [('lease', lease), ('data', data)]:
                self.subscriptions.append(await connection.subscribe(f'{self.channel.subject}.{suffix}', cb=callback, pending_msgs_limit=MAX_PENDING, pending_bytes_limit=MAX_BYTES))
            await connection.flush(timeout=self.channel.hub.options['timeout'])
        except BaseException:
            await self.unbind()
            raise

    async def unbind(self) -> None:
        self.candidate = None
        self.session = ''
        self.leases.clear()
        subscriptions, self.subscriptions = self.subscriptions, []
        for sub in subscriptions:
            try:
                await sub.unsubscribe()
            except ConnectionClosedError:
                pass

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        if self in self.channel.receivers:
            self.channel.receivers.remove(self)
        await self.unbind()
