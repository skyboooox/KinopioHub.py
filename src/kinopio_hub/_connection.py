"""Broker selection, connection handoff and RAM synchronization."""

from __future__ import annotations

from kinopio_hub._compat import timeout as async_timeout

import asyncio
import math
import re
import time
from typing import Any, Callable, TYPE_CHECKING

from . import _protocol as p
from . import _transport

if TYPE_CHECKING:
    from ._hub import KinopioHub

Callback = Callable[..., Any]


class Connection:
    def __init__(self, hub: KinopioHub):
        self.hub = hub
        self.active: Any = None
        self.last_switch = -math.inf
        self.last_switch_reason: Any = None
        self.better: Any = None
        self.ever_connected = False
        self._flush_lock = asyncio.Lock()
        self._flushing_task: asyncio.Task[Any] | None = None

    def schedule_flush(self) -> None:
        if self.active and (self._flushing_task is None or self._flushing_task.done()):
            self._flushing_task = self.hub._spawn(self._flush_pending(), "transport")

    async def close(self) -> None:
        if self.active:
            await _transport.close(self.active["connection"])
            self.active = None

    async def _probe(self, url: str) -> Any:
        if self.active and self.active["url"] == url:
            candidate = self.active
            existing = True
        else:
            candidate = {"url": url, "subscriptions": [], "error": None, "syncing": False}
            existing = False

            async def error_cb(error: Exception) -> None:
                from nats.errors import SlowConsumerError
                if isinstance(error, SlowConsumerError):
                    self.hub.messaging.native_drops += 1
                    self.hub._error(p.KinopioError("SLOW_CONSUMER", "Native subscription dropped a message"), "messaging")
                    if self.active is candidate:
                        self.hub.messaging.fail_requests("BUFFER_OVERFLOW")
                    return
                candidate["error"] = error
                denied = "permissions" in str(error).lower() or "authorization" in str(error).lower()
                if denied and self.active is candidate:
                    self.hub.messaging.fail_requests("PERMISSION_DENIED")
                self.hub._error(p.KinopioError("PERMISSION_DENIED", "NATS permission denied") if denied else error, "permissions")

            async def closed_cb() -> None:
                if self.active is candidate and not self.hub.closed:
                    self.active = None
                    await self.hub.messaging.unbind()
                    for channel in self.hub._live_channels.values():
                        channel.invalidate()
                    self.hub._set_state("offline")
                    self.hub.instances._expire_instances()
                    self.hub._wake.set()

            candidate["connection"] = await _transport.connect(
                url, self.hub.options, error_cb=error_cb, closed_cb=closed_cb
            )
        try:
            before = time.monotonic()
            await candidate["connection"].flush(timeout=self.hub.options["timeout"])
            rtt = (time.monotonic() - before) * 1000
            candidate["rtt"] = (candidate["rtt"] * 2 + rtt) / 3 if existing else rtt
            return candidate
        except BaseException:
            if not existing:
                await _transport.close(candidate["connection"])
            raise

    async def _cycle_loop(self) -> None:
        while not self.hub.closed:
            if self.hub.messaging.phase == "draining":
                return
            self.hub._wake.clear()
            await self._cycle()
            delay = (
                self.hub.options["probe_interval"]
                if self.active
                else min(1.5, self.hub.options["probe_interval"])
            )
            try:
                await asyncio.wait_for(self.hub._wake.wait(), delay)
            except asyncio.TimeoutError:
                pass

    async def _cycle(self) -> None:
        probes: list[Any] = []
        preferred = None
        leader = self.hub.mesh_leader
        if leader and (
            not self.hub.explicit_servers
            or not self.hub.servers
            or leader.get("upstreamConnected")
            or self.hub.mesh_trusted_url == leader["url"]
        ):
            preferred = _transport.endpoint(leader["url"])
        self.hub.discovered = {
            url: expiry for url, expiry in self.hub.discovered.items() if expiry >= time.monotonic()
        }
        urls = list(
            dict.fromkeys(
                ([preferred] if preferred else [])
                + ([self.active["url"]] if self.active else [])
                + self.hub.servers
                + list(self.hub.discovered)
            )
        )[:32]
        initial = self.active is None
        if initial:
            self.hub._set_state("connecting")
        error: Any = None
        try:
            for offset in range(0, len(urls), 4):
                tasks = [asyncio.create_task(self._probe(url)) for url in urls[offset : offset + 4]]
                try:
                    for completed in asyncio.as_completed(tasks):
                        try:
                            candidate = await completed
                            probes.append(candidate)
                            if (
                                initial
                                and self.active is None
                                and (preferred is None or candidate["url"] == preferred)
                            ):
                                try:
                                    await self._activate(candidate, "available-server")
                                except Exception as exc:
                                    candidate["failed"] = True
                                    error = exc
                        except Exception as exc:
                            error = exc
                finally:
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for result in results:
                        if isinstance(result, dict) and result not in probes:
                            probes.append(result)
            viable = sorted((c for c in probes if not c.get("failed")), key=lambda c: c["rtt"])
            current = next((c for c in viable if c is self.active), None)
            if self.active and current is None:
                old = self.active
                self.active = None
                await self.hub.messaging.unbind()
                for channel in self.hub._live_channels.values():
                    channel.invalidate()
                await _transport.close(old["connection"])
                self.hub._set_state("offline")
            best = next((c for c in viable if c["url"] == preferred), viable[0] if viable else None)
            if best is None:
                if error:
                    self.hub._error(error, "connection")
                self.hub._set_state("offline")
                return
            selection = self.hub.options.get("selection", {})
            worthwhile = (
                current
                and current["url"] != preferred
                and best is not current
                and time.monotonic() - self.last_switch >= selection.get("cooldown", 30)
                and current["rtt"] - best["rtt"]
                >= max(
                    selection.get("improvement_ms", 10),
                    current["rtt"] * selection.get("improvement_ratio", 0.2),
                )
            )
            elected = best["url"] == preferred and best is not current
            if not current or elected or (worthwhile and self.better == best["url"]):
                failure: Exception | None = None
                choices = [best] if current else [best] + [item for item in viable if item is not best]
                for choice in choices:
                    try:
                        await self._activate(
                            choice,
                            "elected-lan-node"
                            if elected and choice is best
                            else "lower-latency"
                            if current
                            else "available-server",
                        )
                        failure = None
                        break
                    except Exception as exc:
                        failure = exc
                if failure:
                    raise failure
                self.better = None
            else:
                self.better = best["url"] if worthwhile else None
            self.hub._clear_error("connection")
            if self.active:
                await self._peer_sync(self.active)
            await self._flush_pending()
        except Exception as exc:
            self.hub._error(exc, "connection")
            if not self.active:
                self.hub._set_state("offline")
        finally:
            for candidate in probes:
                if candidate is not self.active:
                    await _transport.close(candidate["connection"])

    async def _publish(self, candidate: Any, subject: str, value: Any, reply: str = "") -> None:
        data = p.encode(value)
        if len(data) > candidate["connection"].max_payload:
            raise p.KinopioError("MESSAGE_TOO_LARGE", "Record exceeds broker payload limit")
        await candidate["connection"].publish(subject, data, reply=reply)
        self.hub.counters["sentMessages"] += 1
        self.hub.counters["sentBytes"] += len(data)

    async def _subscribe(self, candidate: Any, subject: str, consume: Callback) -> Any:
        async def callback(message: Any) -> None:
            if self.hub.closed:
                return
            try:
                self.hub.counters["receivedMessages"] += 1
                self.hub.counters["receivedBytes"] += len(message.data)
                if message.headers and message.headers.get("Status") == "503":
                    return
                await consume(p.decode(message.data), message)
            except Exception as error:
                self.hub._error(error, "updates")

        sub = await candidate["connection"].subscribe(subject, cb=callback)
        candidate["subscriptions"].append(sub)
        return sub

    async def _activate(self, candidate: Any, reason: str) -> None:
        old = self.active
        requests: dict[str, float] = {}

        async def update(record: Any, message: Any) -> None:
            record = p.record_of(record)
            if message.subject != p.key(self.hub.namespace, record["name"]):
                raise p.KinopioError("INVALID_RECORD", "Record name does not match subject")
            self.hub.store._commit(record)

        async def sync(request: Any, message: Any) -> None:
            instance = request.get("instanceId") if isinstance(request, dict) else None
            if (
                not isinstance(instance, str)
                or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", instance)
                or instance == self.hub.instance_id
                or not re.fullmatch(re.escape(f"{self.hub.base}.inbox.") + r"[A-Za-z0-9_-]{1,128}", message.reply)
            ):
                return
            now = time.monotonic()
            if now - requests.get(instance, -math.inf) < self.hub.options["peer_timeout"]:
                return
            if len(requests) >= self.hub.options["max_instances"]:
                requests.pop(next(iter(requests)))
            requests[instance] = now
            for record in list(self.hub.store.records.values()):
                await self._publish(candidate, message.reply, record)
            if candidate is self.active:
                await self.hub.instances._report()

        async def health(report: Any, message: Any) -> None:
            self.hub.instances.receive(report, message.subject.rsplit(".", 1)[1])

        try:
            await self._subscribe(candidate, f"{p.token(self.hub.namespace)}.*", update)
            await self._subscribe(candidate, f"{self.hub.base}.sync", sync)
            await self._subscribe(candidate, f"{self.hub.base}.health.*", health)
            await candidate["connection"].flush(timeout=self.hub.options["timeout"])
            self.hub._assert_open()
            if candidate["error"]:
                raise candidate["error"]
            records = list(self.hub.store.records.values())
            for record in records:
                await self._publish(
                    candidate, p.key(self.hub.namespace, record["name"]), record
                )
            await candidate["connection"].flush(timeout=self.hub.options["timeout"])
            self.hub._assert_open()
            if candidate["error"]:
                raise candidate["error"]
            if self.hub.messaging.phase == "draining":
                raise p.KinopioError("DRAINING", "Hub is draining")
            if old and old is not candidate:
                await self.hub.messaging.unbind(graceful=True)
                self.active = None
                await _transport.close(old["connection"])
            for channel in self.hub._live_channels.values():
                await channel.bind(candidate)
            self.active = candidate
            await self.hub.messaging.bind(candidate)
            self.last_switch = time.monotonic()
            self.last_switch_reason = reason
            if self.ever_connected:
                self.hub.counters["reconnects"] += 1
            self.ever_connected = True
            self.hub._clear_error("permissions")
            self.hub._clear_error("connection")
            self.hub._set_state("connected")
            if old and old is not candidate:
                await _transport.close(old["connection"])
            self.hub.store._acknowledge(records)
            await self._peer_sync(candidate)
            await self.hub.instances._report()
        except BaseException:
            if self.active is candidate:
                self.active = None
                await self.hub.messaging.unbind()
                self.hub._set_state("offline")
            await _transport.close(candidate["connection"])
            raise

    async def _peer_sync(self, candidate: Any) -> None:
        if candidate["syncing"] or self.hub.closed:
            return
        candidate["syncing"] = True
        inbox = f"{self.hub.base}.inbox.{p.id()}"

        async def receive(record: Any, message: Any) -> None:
            self.hub.store._commit(p.record_of(record))
            self.hub._clear_error("updates")

        sub: Any = None
        try:
            sub = await self._subscribe(candidate, inbox, receive)
            await self._publish(
                candidate, f"{self.hub.base}.sync", {"instanceId": self.hub.instance_id}, reply=inbox
            )
        except BaseException:
            candidate["syncing"] = False
            if sub is not None:
                try:
                    await sub.unsubscribe()
                finally:
                    if sub in candidate["subscriptions"]:
                        candidate["subscriptions"].remove(sub)
            raise

        async def finish() -> None:
            try:
                await asyncio.sleep(self.hub.options["peer_timeout"])
            finally:
                try:
                    await sub.unsubscribe()
                finally:
                    if sub in candidate["subscriptions"]:
                        candidate["subscriptions"].remove(sub)
                    candidate["syncing"] = False
                    if not self.hub.closed:
                        self.hub.discovery_done = True
                        self.hub.store.emit()
                        self.hub._notify()

        self.hub._spawn(finish(), "updates")

    async def _send_records(self, candidate: Any, records: list[Any], timeout: float) -> None:
        for record in records:
            await self._publish(
                candidate, p.key(self.hub.namespace, record["name"]), record
            )
        await candidate["connection"].flush(timeout=timeout)
        self.hub._assert_open()
        if candidate["error"]:
            raise candidate["error"]
        if candidate is not self.active:
            raise p.KinopioError("DISCONNECTED", "Connection changed during transport confirmation")
        self.hub.store._acknowledge(records)

    async def _flush_pending(self) -> None:
        async with self._flush_lock:
            while self.active and not self.hub.closed and self.hub.store.pending:
                await self._send_records(
                    self.active,
                    [self.hub.store.records[k] for k in self.hub.store.pending],
                    self.hub.options["timeout"],
                )

    async def flush(self, timeout: float | None = None) -> None:
        duration = self.hub.connection_timeout if timeout is None else timeout
        start = time.monotonic()
        await self.hub.connected(duration)
        try:
            async with async_timeout(max(0.001, duration - (time.monotonic() - start))):
                async with self._flush_lock:
                    await self._send_records(
                        self.active,
                        list(self.hub.store.records.values()),
                        max(0.001, duration - (time.monotonic() - start)),
                    )
        except asyncio.TimeoutError as error:
            raise p.KinopioError("TIMEOUT", "Transport confirmation timed out") from error
