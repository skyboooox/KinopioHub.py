"""Authenticated LAN membership, election, and owned broker coordination."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import inspect
import ipaddress
import re
import time
import uuid
from typing import Any, Callable

from ._election import MeshElection
from ._mesh_config import MAX_PEERS, _port, config_for, host_identity, valid_id, valid_member
from ._mesh_transport import LoadSampler, NativeTransport, broker_reachable, probe_uplink
from ._protocol import PROTOCOL, js_stringify


class MeshManager:
    def __init__(self, options: dict[str, Any], **dependencies: Any):
        from ._broker import start_managed_broker
        self.config = config_for(options)
        self.id, self.host_id = dependencies.get('id', str(uuid.uuid4())), dependencies.get('host_id', host_identity())
        self.now = dependencies.get('now', lambda: time.monotonic() * 1000)
        self.sample_load = dependencies.get('sample_load', LoadSampler())
        self.broker_factory = dependencies.get('broker_factory', start_managed_broker)
        self.probe_broker = dependencies.get('probe_broker', broker_reachable)
        self.probe_uplink = dependencies.get('probe_uplink', probe_uplink)
        self.uplink: dict[str, Any] = {'reachable': None, 'rtt': None}
        self.next_uplink_probe = 0.0
        self.peers: dict[str, Any] = {}
        self.hints: dict[str, Any] = {}
        self.observations: dict[str, Any] = {}
        self.subscribers: list[dict[str, Any]] = []
        self.errors: dict[str, Exception] = {}
        self.reported_error: Exception | None = None
        self.seq, self.vote, self.closed = 0, None, False
        self.election = MeshElection(self.id, minimum_term_ms=self.config['minimum_term_ms'])
        self.started, self.retry_at = self.now(), 0.0
        self.load = self.sample_load()
        self.current_status = {'role': 'discovering', 'leaderId': None, 'members': 1, 'reason': 'Discovering LAN candidates', 'upstreamConnected': None}
        self.last_leader: dict[str, Any] | None = None
        self.broker: Any = None
        self.upstream_connected: bool | None = None
        self.drain_started: float | None = None
        self.starting_broker: asyncio.Task[Any] | None = None
        self.timer: asyncio.Task[Any] | None = None
        self.ticking: asyncio.Task[Any] | None = None
        self.closing: asyncio.Task[Any] | None = None
        self.transport = dependencies.get('transport_factory', NativeTransport)(self.config, self.receive_hint, self.receive_request, self.emit_error, lambda source: self.emit_error(None, source))
        self.port = 0

    def sign(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {'payload': payload, 'signature': hmac.new(self.config['key'].encode(), js_stringify(payload).encode(), hashlib.sha256).hexdigest()}

    def verify(self, message: Any) -> dict[str, Any] | None:
        if not isinstance(message, dict) or not isinstance(message.get('signature'), str) or re.fullmatch('[a-f0-9]{64}', message['signature']) is None:
            return None
        payload = message.get('payload')
        if not isinstance(payload, dict) or payload.get('domain') != self.config['domain'] or type(payload.get('protocol')) is not int or payload['protocol'] != PROTOCOL:
            return None
        try:
            return payload if hmac.compare_digest(self.sign(payload)['signature'], message['signature']) else None
        except (ValueError, TypeError, OverflowError):
            return None

    def advertisement(self) -> dict[str, Any]:
        self.seq += 1
        broker = {'port': self.broker.port, 'wsPort': self.broker.ws_port, 'upstreamConnected': self.upstream_connected} if self.broker else None
        return {'id': self.id, 'hostId': self.host_id, 'seq': self.seq, 'port': self.port, 'vote': self.vote, 'load': self.load.copy(), 'uplink': self.uplink.copy(), 'observations': {k: v.copy() for k, v in self.observations.items()}, 'broker': broker, 'retryAfterMs': max(0, self.retry_at - self.now())}

    def envelope(self, extra: dict[str, Any]) -> dict[str, Any]:
        return self.sign({'protocol': PROTOCOL, 'domain': self.config['domain'], **extra})

    @staticmethod
    def _ipv4(address: Any) -> bool:
        try:
            return isinstance(address, str) and ipaddress.ip_address(address).version == 4
        except ValueError:
            return False

    def _hint(self, member_id: str, address: str, port: int, seq: int) -> None:
        old = self.hints.get(member_id)
        if old and seq < old['seq'] or not old and len(self.hints) >= MAX_PEERS:
            return
        self.hints[member_id] = {'id': member_id, 'port': port, 'address': old['address'] if old else address, 'addresses': list(dict.fromkeys((old['addresses'] if old else []) + [address]))[-4:], 'seq': seq, 'seen': self.now()}

    def receive_hint(self, message: Any, address: str) -> None:
        payload = self.verify(message)
        if not payload or payload.get('kind') != 'hint' or not valid_id(payload.get('id')) or payload['id'] == self.id or not _port(payload.get('port')) or type(payload.get('seq')) is not int or not 0 <= payload['seq'] <= 9007199254740991 or not self._ipv4(address):
            return
        old = self.hints.get(payload['id'])
        if old and payload['seq'] == old['seq'] and address in old['addresses']:
            return
        self._hint(payload['id'], address, payload['port'], payload['seq'])

    def receive_request(self, message: Any, address: str) -> dict[str, Any] | None:
        payload = self.verify(message)
        if payload and payload.get('kind') == 'discover' and valid_id(payload.get('challenge')) and self._ipv4(address):
            return self.envelope({'kind': 'discovery', 'challenge': payload['challenge'], 'member': self.advertisement()})
        if not payload or payload.get('kind') != 'probe' or not valid_id(payload.get('challenge')) or not valid_member(payload.get('member')) or payload['member']['id'] == self.id or not self._ipv4(address):
            return None
        peer = payload['member']
        self._hint(peer['id'], address, peer['port'], peer['seq'])
        return self.envelope({'kind': 'reply', 'challenge': payload['challenge'], 'member': self.advertisement()})

    async def start(self) -> MeshManager:
        try:
            self.port = await self.transport.start()
        except BaseException:
            await self.transport.close()
            raise
        self.timer = asyncio.create_task(self._heartbeat())
        return self

    async def _heartbeat(self) -> None:
        while not self.closed:
            await self.tick()
            await asyncio.sleep(self.config['heartbeat_ms'] / 1000)

    @staticmethod
    def call(callback: Any, value: Any) -> None:
        if callback:
            try:
                result = callback(value)
                if inspect.isawaitable(result):
                    task = asyncio.ensure_future(result)
                    task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
            except Exception:
                pass

    def subscribe(self, callbacks: dict[str, Any]) -> Callable[[], None]:
        self.subscribers.append(callbacks)
        self.call(callbacks.get('on_status'), self.status())
        self.call(callbacks.get('on_leader'), self.last_leader.copy() if self.last_leader else None)
        if self.reported_error:
            self.call(callbacks.get('on_error'), self.reported_error)
        def unsubscribe() -> None:
            for index, item in enumerate(self.subscribers):
                if item is callbacks:
                    del self.subscribers[index]
                    break
        return unsubscribe

    def emit_error(self, error: Exception | None, source: str = 'coordinator') -> None:
        if self.closed:
            return
        if error:
            self.errors[source] = error
        else:
            self.errors.pop(source, None)
        current = next(reversed(self.errors.values())) if self.errors else None
        if current is self.reported_error:
            return
        self.reported_error = current
        for subscriber in self.subscribers[:]:
            self.call(subscriber.get('on_error'), current)

    def update_status(self, status: dict[str, Any]) -> None:
        if status != self.current_status:
            self.current_status = status
            for subscriber in self.subscribers[:]:
                self.call(subscriber.get('on_status'), status.copy())

    def update_leader(self, member: dict[str, Any] | None) -> None:
        next_leader = {'url': f"nats://{member['address']}:{member['broker']['port']}", 'websocketUrl': f"ws://{member['address']}:{member['broker']['wsPort']}", 'upstreamConnected': member['broker']['upstreamConnected']} if member and member.get('broker') else None
        if next_leader != self.last_leader:
            self.last_leader = next_leader
            for subscriber in self.subscribers[:]:
                self.call(subscriber.get('on_leader'), next_leader.copy() if next_leader else None)

    async def probe_peer(self, hint: dict[str, Any]) -> None:
        start = self.now()
        async def request_at(address: str) -> tuple[dict[str, Any], str]:
            challenge = str(uuid.uuid4())
            response = self.verify(await self.transport.probe({**hint, 'address': address}, self.envelope({'kind': 'probe', 'challenge': challenge, 'member': self.advertisement()})))
            if self.closed or not response or response.get('kind') != 'reply' or response.get('challenge') != challenge or not valid_member(response.get('member')) or response['member']['id'] != hint['id']:
                raise ValueError('Invalid mesh response')
            previous = self.peers.get(hint['id'])
            if previous and response['member']['seq'] <= previous['seq']:
                raise ValueError('Replayed mesh response')
            return response['member'], address
        try:
            try:
                member, address = await request_at(hint['address'])
            except Exception:
                alternatives = [address for address in hint.get('addresses', []) if address != hint['address']]
                if not alternatives:
                    raise
                tasks = [asyncio.create_task(request_at(address)) for address in alternatives]
                try:
                    for completed in asyncio.as_completed(tasks):
                        try:
                            member, address = await completed
                            break
                        except Exception:
                            continue
                    else:
                        raise OSError('Mesh peer is unavailable')
                finally:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
            sample = self.observations.get(member['id'])
            rtt = min(60000, max(0, self.now() - start))
            self.observations[member['id']] = {'rtt': sample['rtt'] * 0.7 + rtt * 0.3 if sample else rtt, 'loss': sample['loss'] * 0.7 if sample else 0}
            failed = bool(member['broker']) and not await self.probe_broker(address, member['broker']['port'], self.config['probe_timeout_ms'])
            if self.closed:
                return
            current = self.hints.get(member['id'])
            if current:
                current.update(address=address, seen=self.now())
            self.peers[member['id']] = {**member, 'address': address, 'brokerFailed': failed, 'lastSeen': self.now(), 'unavailableUntil': self.now() + member.get('retryAfterMs', 0)}
        except Exception:
            sample = self.observations.get(hint['id'], {})
            self.observations[hint['id']] = {'rtt': sample.get('rtt', self.config['probe_timeout_ms']), 'loss': sample.get('loss', 0) * 0.7 + 0.3}

    def self_member(self) -> dict[str, Any]:
        return {**self.advertisement(), 'address': '127.0.0.1', 'unavailableUntil': self.retry_at, 'lastSeen': self.now()}

    async def tick(self) -> None:
        if self.closed:
            return
        if self.ticking is None:
            self.ticking = asyncio.create_task(self._safe_tick())
        await asyncio.shield(self.ticking)

    async def _safe_tick(self) -> None:
        try:
            await self.run_tick()
        except Exception as error:
            self.emit_error(error)
        finally:
            self.ticking = None

    async def run_tick(self) -> None:
        self.load, now = self.sample_load(), self.now()
        for member_id, hint in list(self.hints.items()):
            if now - hint['seen'] > self.config['expiry_ms']:
                del self.hints[member_id]
                self.observations.pop(member_id, None)
        self.peers = {member_id: peer for member_id, peer in self.peers.items() if now - peer['lastSeen'] <= self.config['expiry_ms']}
        self.seq += 1
        announcement = self.transport.announce(self.envelope({'kind': 'hint', 'id': self.id, 'port': self.port, 'seq': self.seq}))
        if inspect.isawaitable(announcement):
            await announcement
        await asyncio.gather(*(self.probe_peer(hint) for hint in list(self.hints.values())), return_exceptions=True)
        if self.closed:
            return
        if self.config['upstreams'] and self.now() >= self.next_uplink_probe:
            self.next_uplink_probe = self.now() + max(5000, self.config['heartbeat_ms'] * 3)
            self.uplink = await self.probe_uplink(self.config['upstreams'], self.config['probe_timeout_ms'])
        if self.broker:
            await self._refresh_upstream_status()
            if not await self.probe_broker('127.0.0.1', self.broker.port, self.config['probe_timeout_ms']):
                stale, self.broker = self.broker, None
                self.retry_at = self.now() + self.config['retry_ms']
                await stale.close()
                self.emit_error(OSError('Managed LAN broker stopped responding'), 'broker')
        if self.closed:
            return
        members = [self.self_member(), *self.peers.values()]
        decision = self.election.evaluate(members, self.now())
        self.vote = decision['vote']
        settled = self.now() - self.started >= self.config['settle_ms']
        elected = next((m for m in members if m['id'] == decision['winner']), None)
        reachable = sorted([m for m in members if m['broker'] and not m.get('brokerFailed')], key=lambda m: m['id'])
        selected = elected if elected and elected['broker'] and not elected.get('brokerFailed') else reachable[0] if reachable else None
        self.update_leader(selected)
        if selected and selected['id'] != self.id and not self.starting_broker:
            self.emit_error(None, 'broker')
        if settled and decision['winner'] == self.id and not self.broker and not self.starting_broker and self.now() >= self.retry_at:
            self.starting_broker = asyncio.create_task(self._launch_broker())
        if self.broker and selected and selected['id'] != self.id and decision['winner'] != self.id:
            if self.drain_started is None:
                self.drain_started = self.now()
            if self.now() - self.drain_started >= self.config['drain_ms']:
                old, self.broker, self.drain_started = self.broker, None, None
                await old.close()
        else:
            self.drain_started = None
        role = 'leader' if self.broker else 'follower' if selected else 'error' if 'broker' in self.errors else 'candidate' if settled else 'discovering'
        reason = 'Managed LAN broker unavailable; retrying' if role == 'error' else 'Handing off to elected LAN node' if self.drain_started is not None else 'Using elected LAN node' if selected else 'Starting elected LAN node' if self.starting_broker else 'Collecting LAN member votes'
        self.update_status({'role': role, 'leaderId': selected['id'] if selected else None, 'members': len(members), 'reason': reason, 'upstreamConnected': selected['broker']['upstreamConnected'] if selected else None})
        self.emit_error(None, 'coordinator')

    async def _refresh_upstream_status(self) -> None:
        try:
            self.upstream_connected = await self.broker.upstream_connected() if self.config['upstreams'] else None
        except Exception:
            self.upstream_connected = False

    async def _launch_broker(self) -> None:
        try:
            config = self.config
            broker = await self.broker_factory(binary=config['binary'], host='0.0.0.0', token=config['token'], user=config['user'], password=config['pass'], upstreams=config['upstreams'], upstream_tls=config['upstream_tls'])
            if self.closed:
                await broker.close()
                return
            self.broker, self.retry_at = broker, 0
            await self._refresh_upstream_status()
            self.emit_error(None, 'broker')
        except Exception as error:
            if not self.closed:
                self.retry_at = self.now() + self.config['retry_ms']
                self.emit_error(error, 'broker')
                self.update_status({**self.current_status, 'role': 'error', 'reason': 'Managed LAN broker startup failed'})
        finally:
            self.starting_broker = None

    def status(self) -> dict[str, Any]:
        return self.current_status.copy()

    async def close(self) -> None:
        if self.closing is None:
            self.closing = asyncio.create_task(self._close())
        await asyncio.shield(self.closing)

    async def _close(self) -> None:
        self.closed = True
        tasks = [t for t in (self.timer, self.ticking, self.starting_broker) if t]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self.transport.close()
        if self.broker:
            await self.broker.close()
            self.broker = None
        self.subscribers.clear()
