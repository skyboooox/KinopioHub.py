import asyncio
import json
import shutil
import socket
import subprocess

import pytest

from kinopio_hub._mesh import acquire_mesh
from kinopio_hub._mesh_config import config_for, host_identity, valid_member
from kinopio_hub._mesh_manager import MeshManager
from kinopio_hub._mesh_transport import NativeTransport


class Network:
    def __init__(self):
        self.transports = []
        self.links = True
        self.multicast = True
        self.clock = 0
        self.brokers = []

    def factory(self, config, hint, request, error, healthy):
        network = self
        class Transport:
            def __init__(self):
                self.index = len(network.transports)
                network.transports.append(self)
                self.hint, self.request = hint, request
            async def start(self):
                return 12000 + self.index
            async def announce(self, message):
                if network.links and network.multicast:
                    for target in network.transports:
                        if target is not self:
                            target.hint(message, '127.0.0.1')
            async def probe(self, peer, message):
                if not network.links:
                    raise OSError('partition')
                return network.transports[peer['port'] - 12000].request(message, '127.0.0.1')
            async def close(self):
                pass
        return Transport()

    async def broker(self, **kwargs):
        network = self
        class Broker:
            port = 20000 + len(network.brokers)
            ws_port = port + 1000
            closed = False
            async def close(self):
                self.closed = True
            async def upstream_connected(self):
                return None
        broker = Broker()
        self.brokers.append(broker)
        return broker

    async def reachable(self, address, port, timeout):
        return any(b.port == port and not b.closed for b in self.brokers)

    def manager(self, name, host=None):
        return MeshManager({'mesh': {'settle_ms': 1, 'heartbeat_ms': 10, 'expiry_ms': 100, 'probe_timeout_ms': 5, 'drain_ms': 20}}, id=name, host_id=host or name, now=lambda: self.clock, transport_factory=self.factory, broker_factory=self.broker, probe_broker=self.reachable, sample_load=lambda: {'cpu': 0, 'memory': 0})

    async def rounds(self, managers, count=1):
        for _ in range(count):
            self.clock += 15
            await asyncio.gather(*(m.tick() for m in managers))
            await asyncio.sleep(0)


async def initialize(managers):
    for manager in managers:
        manager.port = await manager.transport.start()


@pytest.mark.asyncio
async def test_client_discovery_does_not_join_election():
    network = Network()
    manager = network.manager('leader')
    await initialize([manager])
    try:
        await network.rounds([manager], 4)
        before = (dict(manager.hints), dict(manager.peers), manager.vote)
        request = manager.envelope({'kind': 'discover', 'challenge': 'client-123'})
        reply = manager.verify(manager.receive_request(request, '192.168.1.10'))
        assert reply['kind'] == 'discovery'
        assert reply['challenge'] == 'client-123'
        assert reply['member']['broker']['port'] > 0
        assert (manager.hints, manager.peers, manager.vote) == before
        request['signature'] = '0' * 64
        assert manager.receive_request(request, '192.168.1.10') is None
        assert manager.receive_request(manager.envelope({'kind': 'discover', 'challenge': ''}), '192.168.1.10') is None
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_partition_merge_and_direct_retention():
    network = Network()
    managers = [network.manager('a'), network.manager('b')]
    await initialize(managers)
    try:
        network.links = False
        await network.rounds(managers, 3)
        assert sum(bool(m.broker) for m in managers) == 2
        network.links = True
        await network.rounds(managers, 8)
        assert sum(bool(m.broker) for m in managers) == 1
        assert all(m.status()['leaderId'] == 'a' for m in managers)
        network.multicast = False
        await network.rounds(managers, 20)
        assert all(m.status()['members'] == 2 for m in managers)
    finally:
        await asyncio.gather(*(m.close() for m in managers))
    assert all(b.closed for b in network.brokers)


@pytest.mark.asyncio
async def test_same_host_and_broker_failure_backoff():
    network = Network()
    managers = [network.manager('a', 'shared'), network.manager('b', 'shared')]
    await initialize(managers)
    try:
        await network.rounds(managers, 8)
        assert managers[0].broker and not managers[1].broker
        managers[0].broker.closed = True
        await network.rounds(managers, 8)
        assert managers[1].broker
        assert managers[0].retry_at > network.clock
    finally:
        await asyncio.gather(*(m.close() for m in managers))


def test_auth_domains_validation_and_bounded_hints():
    network = Network()
    manager = network.manager('a')
    assert config_for({'namespace': 'x'})['domain'] == config_for({'namespace': 'y'})['domain']
    assert config_for({'password': 'p', 'user': 'u'})['domain'] == config_for({'pass': 'p', 'user': 'u'})['domain']
    assert config_for({'mesh': {'binary': '/a'}})['cache_key'] != config_for({'mesh': {'binary': '/b'}})['cache_key']
    for i in range(40):
        manager.receive_hint(manager.envelope({'kind': 'hint', 'id': f'p{i}', 'port': 1234, 'seq': 1}), '127.0.0.1')
    assert len(manager.hints) == 32
    manager.receive_hint(manager.envelope({'kind': 'hint', 'id': 'p0', 'port': 9999, 'seq': 0}), '127.0.0.2')
    assert manager.hints['p0']['port'] == 1234
    manager.port = 1234
    advertisement = manager.advertisement()
    assert valid_member(advertisement)
    advertisement['observations']['__proto__'] = {'rtt': 0, 'loss': 0}
    assert not valid_member(advertisement)
    for options in ({'token': 'a', 'user': 'b', 'password': 'c'}, {'mesh': {'upstreams': ['nats://user:pass@localhost:7422']}}, {'mesh': {'expiry_ms': 1}}):
        with pytest.raises(ValueError):
            config_for(options)


def test_js_hmac_and_host_identity():
    node = shutil.which('node')
    if not node:
        pytest.skip('Node is used only for cross-language test verification')
    script = r'''import os from 'node:os';import{createHash,createHmac}from'node:crypto';const identity=JSON.stringify({group:'default',upstreams:[]});const key=createHash('sha256').update('kinopio-mesh-control-v1:'+identity).digest('hex');const payload={protocol:4,domain:createHash('sha256').update(identity).digest('hex'),a:1e-7,b:1e20,c:0.30000000000000004};console.log(JSON.stringify({payload,signature:createHmac('sha256',key).update(JSON.stringify(payload)).digest('hex')}));console.log(createHash('sha256').update(`${os.hostname()}|${[...new Set(Object.values(os.networkInterfaces()).flat().filter(x=>x&&!x.internal&&x.mac!=='00:00:00:00:00:00').map(x=>x.mac))].sort().join(',')}`).digest('hex').slice(0,32));'''
    lines = subprocess.check_output([node, '--input-type=module', '-e', script], text=True).splitlines()
    manager = Network().manager('a')
    assert manager.verify(json.loads(lines[0]))
    assert host_identity() == lines[1]


@pytest.mark.asyncio
async def test_disabled_and_shared_lifecycle():
    disabled = await acquire_mesh({'mesh': False})
    assert disabled.status()['role'] == 'disabled'
    await disabled.close()
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        probe.bind(('127.0.0.1', 0))
        port = probe.getsockname()[1]
    options = {'mesh': {'discovery_port': port, 'group': 'pytest-' + str(__import__('uuid').uuid4()), 'settle_ms': 100000}}
    handles = await asyncio.gather(acquire_mesh(options), acquire_mesh(options))
    from kinopio_hub._mesh import _managers
    assert len(_managers) == 1
    await handles[0].close()
    assert len(_managers) == 1
    await handles[1].close()
    assert not _managers


@pytest.mark.asyncio
async def test_native_authenticated_probe_and_body_deadline():
    network = Network()
    manager = network.manager('a')
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        probe.bind(('127.0.0.1', 0))
        port = probe.getsockname()[1]
    config = config_for({'mesh': {'discovery_port': port, 'probe_timeout_ms': 100}})
    transport = NativeTransport(config, lambda *args: None, manager.receive_request, lambda *args: None, lambda *args: None)
    try:
        manager.port = await transport.start()
        peer = network.manager('b')
        peer.port = 12345
        request = peer.envelope({'kind': 'probe', 'challenge': 'test', 'member': peer.advertisement()})
        response = await transport.probe({'address': '127.0.0.1', 'port': manager.port}, request)
        assert peer.verify(response)['challenge'] == 'test'
        assert manager.hints['b']['address'] == '127.0.0.1'
        reader, writer = await asyncio.open_connection('127.0.0.1', manager.port)
        writer.write(b'POST /kinopio-mesh/v1 HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\n\r\n{')
        await writer.drain()
        data = await asyncio.wait_for(reader.read(4096), 1)
        assert b'400' in data
        writer.close()
        await writer.wait_closed()
    finally:
        await transport.close()


@pytest.mark.asyncio
async def test_close_cancels_broker_start_and_source_errors_recover():
    network = Network()
    manager = network.manager('a')
    entered, cancelled = asyncio.Event(), asyncio.Event()
    async def blocked_broker(**kwargs):
        entered.set()
        try:
            await asyncio.Future()
        finally:
            cancelled.set()
    manager.broker_factory = blocked_broker
    errors: list[Exception | None] = []
    manager.subscribe({'on_error': errors.append})
    first, second = OSError('discovery'), OSError('broker')
    manager.emit_error(first, 'discovery')
    manager.emit_error(second, 'broker')
    manager.emit_error(None, 'broker')
    assert errors == [first, second, first]
    manager.emit_error(None, 'discovery')
    assert errors[-1] is None
    await initialize([manager])
    await network.rounds([manager])
    await entered.wait()
    await manager.close()
    assert cancelled.is_set()


@pytest.mark.asyncio
async def test_one_way_discovery_bootstraps_reverse_direct_peer_and_replay_rejected():
    network = Network()
    a, b = network.manager('a'), network.manager('b')
    await initialize([a, b])
    network.multicast = False
    try:
        a.receive_hint(b.envelope({'kind': 'hint', 'id': 'b', 'port': b.port, 'seq': 1}), '127.0.0.1')
        await network.rounds([a, b], 8)
        assert a.status()['members'] == b.status()['members'] == 2
        assert sum(bool(m.broker) for m in (a, b)) == 1
        previous = a.peers['b'].copy()
        original = a.transport.probe
        async def replay(peer, request):
            return b.envelope({'kind': 'reply', 'challenge': request['payload']['challenge'], 'member': previous})
        a.transport.probe = replay
        await a.probe_peer(a.hints['b'])
        assert a.peers['b']['seq'] == previous['seq']
        assert a.observations['b']['loss'] > 0
        a.transport.probe = original
    finally:
        await asyncio.gather(a.close(), b.close())


@pytest.mark.parametrize(('field', 'value'), [
    ('seq', True),
    ('seq', 9007199254740992),
    ('vote', '__proto__'),
    ('load', {'cpu': float('nan'), 'memory': 0}),
    ('uplink', {'reachable': 1}),
    ('observations', {'peer': {'rtt': 0, 'loss': 1.1}}),
    ('broker', {'port': 1234, 'wsPort': 1235, 'upstreamConnected': 1}),
])
def test_member_validation_rejects_invalid_wire_fields(field, value):
    manager = Network().manager('a')
    manager.port = 1234
    member = manager.advertisement()
    assert valid_member(member)
    member[field] = value
    assert not valid_member(member)


@pytest.mark.asyncio
async def test_peer_probe_recovers_through_alternate_interface():
    network = Network()
    a, b = network.manager('a'), network.manager('b')
    await initialize([a, b])
    original = a.transport.probe
    visited = []

    async def probe(peer, message):
        visited.append(peer['address'])
        if peer['address'] == '192.0.2.1':
            raise OSError('Interface unavailable')
        return await original(peer, message)

    a.transport.probe = probe
    try:
        hint = b.envelope({'kind': 'hint', 'id': 'b', 'port': b.port, 'seq': 1})
        a.receive_hint(hint, '192.0.2.1')
        a.receive_hint(hint, '127.0.0.1')
        await a.probe_peer(a.hints['b'])
        assert visited == ['192.0.2.1', '127.0.0.1']
        assert a.peers['b']['address'] == '127.0.0.1'
        assert a.hints['b']['address'] == '127.0.0.1'
        visited.clear()
        await a.probe_peer(a.hints['b'])
        assert visited == ['127.0.0.1']
    finally:
        await asyncio.gather(a.close(), b.close())


async def test_native_datagrams_receive_and_announce(monkeypatch):
    monkeypatch.setattr('kinopio_hub._mesh_transport.ipv4', lambda: ['127.0.0.1'])
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        probe.bind(('127.0.0.1', 0))
        port = probe.getsockname()[1]
    hints = asyncio.Queue()
    errors = []
    transport = NativeTransport(
        config_for({'mesh': {'discovery_port': port}}),
        lambda value, address: hints.put_nowait((value, address)),
        lambda *args: None, lambda *args: errors.append(args), lambda *args: None,
    )
    try:
        await transport.start()
        await transport.announce({'kind': 'test'})
        value, address = await asyncio.wait_for(hints.get(), 2)
        assert value == {'kind': 'test'}
        assert address == '127.0.0.1'
        assert not errors
    finally:
        await transport.close()
