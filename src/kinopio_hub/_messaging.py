"""Bounded, generation-bound NATS Core messaging on stable variable references."""
from __future__ import annotations

import asyncio
import contextvars
import inspect
import math
import re
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

from nats.errors import BadSubscriptionError, ConnectionClosedError, ConnectionDrainingError

from . import _protocol as p
from ._compat import timeout as async_timeout

if TYPE_CHECKING:
    from ._hub import KinopioHub

_CURRENT: contextvars.ContextVar[Any] = contextvars.ContextVar('kinopio_message_handler', default=None)
DEFAULTS = dict(subscriptions=128, requests=64, pending_messages=4096,
                pending_bytes=8 * 1024 * 1024, outbound_bytes=8 * 1024 * 1024)


def positive(value: Any, name: str, integer: bool = False) -> Any:
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0 or (integer and (type(value) is not int or value > 9007199254740991)):
        raise p.KinopioError('INVALID_OPTIONS', f'{name} must be positive')
    return value


def subject(namespace: str, name: str, pattern: bool = False) -> str:
    try:
        if not isinstance(name, str) or not 1 <= len(name.encode('utf-8')) <= 128:
            raise ValueError
        parts = name.split('.')
        for i, part in enumerate(parts):
            if not part or any(ord(c) < 32 or ord(c) == 127 for c in part):
                raise ValueError
            if '*' in part or '>' in part:
                if not pattern or not (part == '*' or (part == '>' and i == len(parts) - 1)):
                    raise ValueError
        return '_msg.v1.' + p.token(namespace) + '.' + '.'.join(
            s if s in ('*', '>') else s.encode('utf-8').hex() for s in parts)
    except (ValueError, UnicodeError) as error:
        raise p.KinopioError('INVALID_TOPIC', 'Invalid message topic or pattern') from error


def queue_name(namespace: str, name: str | None) -> str:
    if name is None:
        return ''
    try:
        p.name(name, 'queue')
        if '*' in name or '>' in name:
            raise ValueError
    except (ValueError, p.KinopioError) as error:
        raise p.KinopioError('INVALID_TOPIC', 'Invalid queue name') from error
    return '_q.v1.' + p.token(namespace) + '.' + name.encode('utf-8').hex()


async def unsubscribe_native(native: Any) -> None:
    native._conn.message_callbacks.pop(native._id, None)
    try:
        await native.unsubscribe()
    except (ConnectionClosedError, ConnectionDrainingError, BadSubscriptionError):
        pass


class Headers(dict[str, str]):
    """Native NATS header entries, preserving spelling, order and repeated values."""
    def __init__(self, values: Any = None):
        super().__init__()
        if hasattr(values, 'kinopio_error'):
            raise values.kinopio_error
        self._entries: list[tuple[str, str]] = []
        if values is not None:
            entries = values.items() if isinstance(values, Mapping) else values
            for key, value in entries:
                for item in value if isinstance(value, list) else [value]:
                    self.add(key, item)
        self.validate()

    def add(self, key: str, value: str) -> None:
        if not isinstance(key, str) or not re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+", key) or not isinstance(value, str) or any(not 32 <= ord(c) <= 126 for c in value):
            raise p.KinopioError('INVALID_HEADERS', 'Headers require ASCII token names and visible ASCII values')
        self._entries.append((key, value))
        super().__setitem__(key, value)
        try:
            self.validate()
        except BaseException:
            self._entries.pop()
            super().clear()
            for k, v in self._entries:
                super().__setitem__(k, v)
            raise

    def __setitem__(self, key: str, value: str) -> None:
        self.add(key, value)

    def update(self, values: Any = (), **kwargs: str) -> None:  # type: ignore[override]
        incoming = Headers(values)
        for key, value in kwargs.items():
            incoming.add(key, value)
        replacement = Headers(self.items() + incoming.items())
        super().clear()
        self._entries = []
        for key, value in replacement.items():
            self.add(key, value)

    def clear(self) -> None:
        self._entries.clear()
        super().clear()

    def __delitem__(self, key: str) -> None:
        if not self.get_all(key):
            raise KeyError(key)
        remaining = [(k, v) for k, v in self._entries if k.lower() != key.lower()]
        self.clear()
        for k, v in remaining:
            self.add(k, v)

    def pop(self, key: str, *default: Any) -> Any:
        if not self.get_all(key):
            if default:
                return default[0]
            raise KeyError(key)
        value = self[key]
        del self[key]
        return value

    def popitem(self) -> tuple[str, str]:
        if not self._entries:
            raise KeyError('Headers are empty')
        key, value = self._entries[-1]
        del self[key]
        return key, value

    def setdefault(self, key: str, default: str = '') -> str:
        if not self.get_all(key):
            self.add(key, default)
        return self[key]

    def __ior__(self, other: Any) -> Headers:  # type: ignore[override,misc]
        self.update(other)
        return self

    def copy(self) -> Headers:
        return Headers(self)

    def get_all(self, key: str) -> list[str]:
        return [v for k, v in self._entries if k.lower() == key.lower()]

    def get(self, key: str, default: Any = None) -> Any:
        values = self.get_all(key)
        return values[0] if values else default

    def __getitem__(self, key: str) -> str:
        values = self.get_all(key)
        if not values:
            raise KeyError(key)
        return values[0]

    def items(self) -> Any:
        return list(self._entries)

    @property
    def byte_size(self) -> int:
        return 12 + sum(len(k) + len(v) + 4 for k, v in self._entries) if self._entries else 0

    def validate(self) -> None:
        if len(self._entries) > 32 or self.byte_size > 4096:
            raise p.KinopioError('MESSAGE_TOO_LARGE', 'Headers exceed 32 entries or 4 KiB')


@dataclass(frozen=True)
class Reply:
    data: Any
    headers: Headers


@dataclass(frozen=True)
class ManyResult:
    replies: list[Reply]
    reason: str


class MessageError(p.KinopioError):
    def __init__(self, code: str, message: str, partial_replies: list[Reply] | None = None):
        super().__init__(code, message)
        self.partial_replies = list(partial_replies or [])


class HandleContext:
    def __init__(self, manager: Messaging, sub: Subscription, message: Any, candidate: Any, generation: int):
        self.topic = '.'.join(bytes.fromhex(s).decode('utf-8') for s in message.subject.split('.')[3:])
        if subject(manager.hub.namespace, self.topic) != message.subject:
            raise MessageError('INVALID_MESSAGE', 'Malformed message subject')
        self.headers = Headers(message.headers)
        self.reply_headers = Headers()
        self._manager, self._sub = manager, sub
        self._reply = message.reply
        self._candidate, self._generation = candidate, generation
        self._valid = True

    async def _respond(self, data: Any, headers: Any = None) -> None:
        if not self._reply:
            raise MessageError('NO_REPLY_SUBJECT', 'Message has no reply subject')
        if not self._valid or self._generation != self._manager.generation:
            raise MessageError('DISCONNECTED', 'Reply context is no longer valid')
        await self._manager.send(self._reply, data, headers, candidate=self._candidate, reply_context=True)


class MessageContext(HandleContext):
    async def reply(self, data: Any, *, headers: Any = None) -> None:
        await self._respond(data, headers)


class Subscription:
    def __init__(self, manager: Messaging, topic: str, callback: Any, *, queue: str | None,
                 with_context: bool, pending_messages: int, pending_bytes: int, handle: bool):
        self.manager, self.topic, self.callback = manager, topic, callback
        self.subject = subject(manager.hub.namespace, topic, True)
        self.queue = queue_name(manager.hub.namespace, queue)
        self.with_context, self.handle = with_context, handle
        self.limit_messages = positive(pending_messages, 'pending_messages', True)
        self.limit_bytes = positive(pending_bytes, 'pending_bytes', True)
        self.pending: deque[Any] = deque()
        self.pending_bytes = 0
        self.in_flight = 0
        self.dropped = 0
        self.high_messages = self.high_bytes = 0
        self.registered = asyncio.Event()
        self.native: Any = None
        self.worker: asyncio.Task[Any] | None = None
        self.closed = False
        self.draining = False
        self.ready = False
        self.context: HandleContext | None = None
        self.slow = False

    async def bind(self, candidate: Any, generation: int) -> None:
        async def receive(message: Any) -> None:
            if self.closed or generation != self.manager.generation:
                return
            self.manager.hub.counters['receivedMessages'] += 1
            self.manager.hub.counters['receivedBytes'] += len(message.data)
            if self.handle and not message.reply:
                return
            try:
                headers = Headers(message.headers)
                size = len(message.data) + headers.byte_size + len(message.subject.encode()) + len(message.reply.encode())
                if len(message.data) > 65536:
                    raise MessageError('MESSAGE_TOO_LARGE', 'Payload exceeds 64 KiB')
            except p.KinopioError as error:
                self.manager.hub._error(error, 'messaging')
                self.dropped += 1
                self.manager.dropped += 1
                return
            if len(self.pending) + self.in_flight >= self.limit_messages or self.pending_bytes + size > self.limit_bytes or not self.manager.reserve(size):
                self.dropped += 1
                self.manager.dropped += 1
                if not self.slow:
                    self.manager.hub._error(MessageError('SLOW_CONSUMER', 'Message queue is full; new message dropped'), 'messaging')
                self.slow = True
                return
            self.pending.append((message, size, candidate, generation))
            self.pending_bytes += size
            self.high_messages = max(self.high_messages, len(self.pending) + self.in_flight)
            self.high_bytes = max(self.high_bytes, self.pending_bytes)
            if self.worker is None or self.worker.done():
                self.worker = asyncio.create_task(self.run())
        before_error = candidate.get('error')
        self.native = await candidate['connection'].subscribe(self.subject, queue=self.queue, cb=receive,
                    pending_msgs_limit=self.limit_messages, pending_bytes_limit=self.limit_bytes)
        candidate['connection'].message_callbacks[self.native._id] = receive
        await candidate['connection'].flush(timeout=self.manager.hub.options['timeout'])
        if candidate.get('error') is not before_error:
            raise MessageError('PERMISSION_DENIED', 'Subscription was rejected')
        self.ready = True

    async def run(self) -> None:
        while self.pending and not self.closed:
            message, size, candidate, generation = self.pending.popleft()
            self.in_flight = 1
            invocation = [self, True]
            token = _CURRENT.set(invocation)
            context: HandleContext | None = None
            try:
                if generation != self.manager.generation:
                    continue
                data = p.value_of(p.decode(message.data), 65536)
                context_type = HandleContext if self.handle else MessageContext
                context = context_type(self.manager, self, message, candidate, generation)
                self.context = context
                result = self.callback(data, context) if self.with_context else self.callback(data)
                if inspect.isawaitable(result):
                    result = await result
                if self.handle:
                    await context._respond(result, context.reply_headers)
                self.manager.hub._clear_error('messaging-handler')
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.manager.hub._error(MessageError('HANDLER_ERROR', 'Message handler failed'), 'messaging-handler')
                self.manager.last_handler_error = p.safe_error(error)
            finally:
                if context:
                    context._valid = False
                self.context = None
                invocation[1] = False
                _CURRENT.reset(token)
                self.in_flight = 0
                self.pending_bytes -= size
                self.manager.release(size)
        if not self.pending:
            self.slow = False
            self.manager.recovered()

    async def stop_interest(self) -> None:
        self.ready = False
        native, self.native = self.native, None
        if native:
            # Native drain receives all frames preceding the UNSUB/PONG barrier.
            try:
                await native.drain()
            finally:
                native._conn.message_callbacks.pop(native._id, None)

    def discard(self) -> None:
        while self.pending:
            _, size, _, _ = self.pending.popleft()
            self.pending_bytes -= size
            self.manager.release(size)
            self.dropped += 1
            self.manager.dropped += 1

    async def unsubscribe(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.ready = False
        if self.native:
            await unsubscribe_native(self.native)
            self.native = None
        self.discard()
        if self.worker and not self.worker.done():
            self.manager.retired.add(self)
            self.worker.add_done_callback(lambda _: self.manager.retired.discard(self))
        self.manager.subscriptions.discard(self)

    async def finish(self) -> None:
        worker = self.worker
        if worker and not worker.done():
            await asyncio.shield(worker)

    async def drain(self, *, timeout: float = 5) -> None:
        positive(timeout, 'timeout')
        current = _CURRENT.get()
        if current is not None and current[0] is self and current[1]:
            raise MessageError('DRAIN_IN_HANDLER', 'Drain must run outside its managed handler')
        if self.closed:
            return
        self.draining = True
        try:
            async with async_timeout(timeout):
                await self.stop_interest()
                await self.finish()
                active = self.manager.hub.connection.active
                if active is None:
                    raise MessageError('DISCONNECTED', 'Connection ended during subscription drain')
                await active['connection'].flush(timeout=timeout)
        except asyncio.TimeoutError as error:
            raise MessageError('DRAIN_TIMEOUT', 'Subscription drain exceeded its deadline') from error
        finally:
            await self.unsubscribe()

    def status(self) -> dict[str, Any]:
        return dict(ready=self.ready, phase='closed' if self.closed else 'draining' if self.draining else 'active',
                    pendingMessages=len(self.pending), pendingBytes=self.pending_bytes,
                    inFlightHandlers=self.in_flight, droppedMessages=self.dropped,
                    highWaterMessages=self.high_messages, highWaterBytes=self.high_bytes,
                    limits=dict(pendingMessages=self.limit_messages, pendingBytes=self.limit_bytes, inFlightHandlers=1))


class Messaging:
    def __init__(self, hub: KinopioHub):
        self.hub = hub
        settings = hub.options.get('messaging', {})
        if not isinstance(settings, dict) or settings.keys() - DEFAULTS.keys():
            raise p.KinopioError('INVALID_OPTIONS', 'Unknown messaging limits')
        self.limits = {k: positive(settings.get(k, v), k, True) for k, v in DEFAULTS.items()}
        self.subscriptions: set[Subscription] = set()
        self.retired: set[Subscription] = set()
        self.requests: dict[asyncio.Future[Any], list[Reply]] = {}
        self.generation = 0
        self.phase = 'offline'
        self.pending_messages = self.pending_bytes = self.dropped = self.native_drops = 0
        self.high_messages = self.high_bytes = 0
        self.outbound_bytes = 0
        self.last_handler_error: Any = None
        self.drain_task: asyncio.Task[Any] | None = None

    def check(self) -> Any:
        self.hub._assert_open()
        if self.phase == 'draining':
            raise MessageError('DRAINING', 'Hub is draining')
        active = self.hub.connection.active
        if self.phase != 'active' or not active or not active['connection'].is_connected:
            raise MessageError('DISCONNECTED', 'Messaging requires an active connection')
        return active

    def reserve(self, size: int) -> bool:
        if self.pending_messages >= self.limits['pending_messages'] or self.pending_bytes + size > self.limits['pending_bytes']:
            return False
        self.pending_messages += 1
        self.pending_bytes += size
        self.high_messages = max(self.high_messages, self.pending_messages)
        self.high_bytes = max(self.high_bytes, self.pending_bytes)
        return True

    def release(self, size: int) -> None:
        self.pending_messages -= 1
        self.pending_bytes -= size

    def recovered(self) -> None:
        if not any(s.slow for s in self.subscriptions):
            current = self.hub.errors.get('messaging')
            if current and current['code'] == 'SLOW_CONSUMER':
                self.hub._clear_error('messaging')

    async def send(self, topic: str, data: Any, headers: Any = None, *, reply: str = '', candidate: Any = None, reply_context: bool = False) -> None:
        if not reply_context:
            candidate = self.check()
        elif self.hub.closed or not candidate or not candidate['connection'].is_connected:
            raise MessageError('DISCONNECTED', 'Reply connection is unavailable')
        try:
            encoded = p.encode(p.value_of(data, 65536))
        except p.KinopioError as error:
            code = 'MESSAGE_TOO_LARGE' if error.code == 'VALUE_TOO_LARGE' else 'INVALID_MESSAGE'
            raise MessageError(code, 'Invalid message JSON payload') from error
        header = Headers((key.lower(), value.strip(' ')) for key, value in Headers(headers).items())
        size = len(encoded) + header.byte_size
        client = candidate['connection']
        if size > client.max_payload:
            raise MessageError('MESSAGE_TOO_LARGE', 'Message and Headers exceed broker payload limit')
        wire_size = size + len(topic.encode()) + len(reply.encode()) + 64
        if self.outbound_bytes + getattr(client, '_pending_data_size', 0) + wire_size > self.limits['outbound_bytes']:
            raise MessageError('BUFFER_OVERFLOW', 'Outbound message budget reached')
        self.outbound_bytes += wire_size
        try:
            await client.publish(topic, encoded, reply=reply, headers=header if header else None)
            self.hub.counters['sentMessages'] += 1
            self.hub.counters['sentBytes'] += size
        except p.KinopioError:
            raise
        except Exception as error:
            raise MessageError('DISCONNECTED', 'Message transport failed') from error
        finally:
            self.outbound_bytes -= wire_size

    async def subscribe(self, topic: str, callback: Any, *, queue: str | None = None,
                        with_context: bool = False, pending_messages: int = 256,
                        pending_bytes: int = 1048576, handle: bool = False) -> Subscription:
        candidate = self.check()
        if not callable(callback) or type(with_context) is not bool:
            raise MessageError('INVALID_OPTIONS', 'Callback must be callable and with_context boolean')
        if len(self.subscriptions | self.retired) >= self.limits['subscriptions']:
            raise MessageError('BUFFER_OVERFLOW', 'Subscription capacity reached')
        sub = Subscription(self, topic, callback, queue=queue, with_context=with_context,
                           pending_messages=pending_messages, pending_bytes=pending_bytes, handle=handle)
        self.subscriptions.add(sub)
        generation = self.generation
        try:
            await sub.bind(candidate, generation)
            if candidate is not self.hub.connection.active or generation != self.generation:
                raise MessageError('DISCONNECTED', 'Connection changed during subscription registration')
            self.check()
        except BaseException:
            await sub.unsubscribe()
            raise
        finally:
            sub.registered.set()
        return sub

    async def request(self, topic: str, data: Any = None, *, timeout: float = 3,
                      headers: Any = None, details: bool = False, many: bool = False,
                      max_replies: int = 16, max_bytes: int = 1048576) -> Any:
        positive(timeout, 'timeout')
        if type(details) is not bool:
            raise MessageError('INVALID_OPTIONS', 'details must be boolean')
        positive(max_replies, 'max_replies', True)
        positive(max_bytes, 'max_bytes', True)
        if max_replies > 16 or max_bytes > 1048576:
            raise MessageError('INVALID_OPTIONS', 'Collection limits cannot exceed 16 replies / 1 MiB')
        wire = subject(self.hub.namespace, topic)
        candidate = self.check()
        if len(self.requests) >= self.limits['requests']:
            raise MessageError('BUFFER_OVERFLOW', 'Request capacity reached')
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        replies: list[Reply] = []
        sizes: list[int] = []
        self.requests[future] = replies
        client = candidate['connection']
        generation = self.generation
        inbox = client.new_inbox()
        sub: Any = None
        reason = 'deadline'

        async def receive(message: Any) -> None:
            if future.done() or generation != self.generation:
                return
            self.hub.counters['receivedMessages'] += 1
            self.hub.counters['receivedBytes'] += len(message.data)
            try:
                header = Headers(message.headers)
                if not message.data and header.get('Status') == '503':
                    raise MessageError('NO_RESPONDERS', 'No matching subscriber interest')
                if len(message.data) > 65536:
                    raise MessageError('MESSAGE_TOO_LARGE', 'Reply payload exceeds 64 KiB')
                size = len(message.data) + header.byte_size + len(message.subject) + len(message.reply)
                if sum(sizes) + size > max_bytes or not self.reserve(size):
                    raise MessageError('BUFFER_OVERFLOW', 'Reply collection budget reached')
                try:
                    value = p.value_of(p.decode(message.data), 65536)
                except BaseException:
                    self.release(size)
                    raise
                sizes.append(size)
                replies.append(Reply(value, header))
                if not many or len(replies) >= max_replies:
                    future.set_result('maxReplies')
            except Exception as error:
                code = getattr(error, 'code', 'INVALID_MESSAGE')
                code = {'VALUE_TOO_LARGE': 'MESSAGE_TOO_LARGE', 'INVALID_VALUE': 'INVALID_MESSAGE'}.get(code, code)
                future.set_exception(MessageError(code, 'Invalid or failed reply', replies))
        try:
            async with async_timeout(timeout):
                sub = await client.subscribe(inbox, cb=receive, pending_msgs_limit=16, pending_bytes_limit=max_bytes)
                client.message_callbacks[sub._id] = receive
                if candidate is not self.hub.connection.active or generation != self.generation:
                    raise MessageError('DISCONNECTED', 'Connection changed before request send')
                await self.send(wire, data, headers, reply=inbox)
                reason = await asyncio.shield(future)
        except asyncio.TimeoutError as error:
            if not many:
                raise MessageError('TIMEOUT', 'Request timed out; remote execution may have started', replies) from error
        except asyncio.CancelledError as error:
            error.partial_replies = list(replies)  # type: ignore[attr-defined]
            task = asyncio.current_task()
            if task is not None:
                task.partial_replies = list(replies)  # type: ignore[attr-defined]
            raise
        finally:
            self.requests.pop(future, None)
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                future.exception()
            for size in sizes:
                self.release(size)
            if sub:
                await unsubscribe_native(sub)
        if many:
            return ManyResult(replies, reason) if details else [r.data for r in replies]
        return replies[0] if details else replies[0].data

    def fail_requests(self, code: str = 'DISCONNECTED') -> None:
        for future, replies in list(self.requests.items()):
            if not future.done():
                future.set_exception(MessageError(code, 'Request interrupted; remote execution may have started', replies))

    async def unbind(self, graceful: bool = False) -> None:
        self.phase = 'closed' if self.hub.closed else 'draining' if self.phase == 'draining' else 'handoff' if graceful else 'offline'
        self.fail_requests('CLOSED' if self.hub.closed else 'DISCONNECTED')
        subs = list(self.subscriptions | self.retired)
        try:
            if graceful:
                async with async_timeout(5):
                    await asyncio.gather(*(s.registered.wait() for s in subs))
                    await asyncio.gather(*(s.stop_interest() for s in subs))
                    await asyncio.gather(*(s.finish() for s in subs))
        except (asyncio.TimeoutError, Exception):
            pass
        finally:
            self.generation += 1
            for sub in subs:
                sub.ready = False
                if sub.native:
                    try:
                        await unsubscribe_native(sub.native)
                    except Exception:
                        pass
                    sub.native = None
                sub.discard()
                if sub.context:
                    sub.context._valid = False
                if sub.worker and not sub.worker.done():
                    sub.worker.cancel()

    async def bind(self, candidate: Any) -> None:
        self.generation += 1
        for sub in list(self.subscriptions):
            if not sub.closed:
                await sub.bind(candidate, self.generation)
        self.phase = 'active'

    async def close(self) -> None:
        await self.unbind()
        for sub in list(self.subscriptions):
            await sub.unsubscribe()
        self.phase = 'closed'

    async def drain(self, timeout: float = 5) -> None:
        positive(timeout, 'timeout')
        current = _CURRENT.get()
        if current is not None and current[0].manager is self and current[1]:
            raise MessageError('DRAIN_IN_HANDLER', 'Drain must run outside a managed handler')
        if self.drain_task is None:
            self.drain_task = asyncio.create_task(self._drain(timeout))
        await asyncio.shield(self.drain_task)

    async def _drain(self, timeout: float) -> None:
        self.hub._assert_open()
        self.phase = 'draining'
        try:
            async with async_timeout(timeout):
                await asyncio.gather(*(s.registered.wait() for s in list(self.subscriptions)))
                await asyncio.gather(*(s.stop_interest() for s in list(self.subscriptions)))
                await asyncio.gather(*(s.finish() for s in list(self.subscriptions | self.retired)))
                if self.requests:
                    await asyncio.gather(*(asyncio.shield(f) for f in list(self.requests)), return_exceptions=True)
                await self.hub.flush(timeout=timeout)
                await self.hub.close()
        except asyncio.TimeoutError as error:
            self.hub._begin_close()
            raise MessageError('DRAIN_TIMEOUT', 'Hub drain exceeded its total deadline') from error
        except BaseException:
            self.hub._begin_close()
            raise

    def status(self) -> dict[str, Any]:
        return dict(phase=self.phase, subscriptions=len(self.subscriptions), pendingRequests=len(self.requests),
                    pendingMessages=self.pending_messages, pendingBytes=self.pending_bytes,
                    inFlightHandlers=sum(s.in_flight for s in self.subscriptions | self.retired), droppedMessages=self.dropped,
                    nativeDroppedMessages=self.native_drops, highWaterMessages=self.high_messages,
                    highWaterBytes=self.high_bytes, limits=dict(self.limits))
