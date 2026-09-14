import asyncio
import time

import pytest

from kinopio_hub import KinopioError, KinopioHub
from kinopio_hub import _protocol as p
from kinopio_hub._broker import start_managed_broker
from kinopio_hub._compat import timeout as async_timeout


@pytest.fixture
async def pair():
    broker = await start_managed_broker(host='127.0.0.1')
    sender = KinopioHub("live-tests", mesh=False, discovery=False, servers=broker.url, peer_timeout=.01)
    receiver = KinopioHub("live-tests", mesh=False, discovery=False, servers=broker.url, peer_timeout=.01)
    try:
        await asyncio.gather(sender.connected(), receiver.connected())
        yield sender, receiver
    finally:
        await asyncio.gather(sender.close(), receiver.close())
        await broker.close()


async def test_live_equal_values_no_state_or_replay(pair):
    sender, receiver = pair
    values = asyncio.Queue()
    stop = await receiver.live('robot/cmd').subscribe(values.put_nowait)
    channel = sender.live('robot/cmd')
    await channel.send({'speed': 1})
    await channel.send({'speed': 1})
    assert await asyncio.wait_for(values.get(), 1) == {'speed': 1}
    assert await asyncio.wait_for(values.get(), 1) == {'speed': 1}
    assert channel.grant['seq'] == 2
    assert not sender.store.records and not receiver.store.records
    await stop()
    await receiver.live('robot/cmd').subscribe(values.put_nowait)
    await asyncio.sleep(.02)
    assert values.empty()
    with pytest.raises(KinopioError, match='one receiver'):
        await receiver.live('robot/cmd').subscribe(values.put_nowait)


async def test_live_no_receiver_and_offline_fail_without_pending(pair):
    sender, receiver = pair
    with pytest.raises(KinopioError) as error:
        await sender.live('absent').send(1, timeout=.2)
    assert error.value.code == 'NO_RECEIVERS'
    assert sender.live('absent').pending is None
    candidate = sender.connection.active
    sender.connection.active = None
    try:
        with pytest.raises(KinopioError) as error:
            await sender.live('absent').send(2)
        assert error.value.code == 'DISCONNECTED'
        assert not sender.store.pending
    finally:
        sender.connection.active = candidate


async def test_live_receiver_rejects_expiry_sequence_session_and_clock_skew(pair, monkeypatch):
    sender, receiver = pair
    values = asyncio.Queue()
    await receiver.live('robot').subscribe(values.put_nowait, max_age_ms=100)
    channel = sender.live('robot')
    await channel.send(1)
    assert await asyncio.wait_for(values.get(), 1) == 1
    grant = channel.grant.copy()
    connection = sender.connection.active['connection']

    async def raw(seq, value, **fields):
        data = {'protocol': 1, 'session': grant['session'], 'lease': grant['lease'], 'seq': seq, 'value': value, **fields}
        await connection.publish(f'{channel.subject}.data', p.encode(data))
        await connection.flush()

    await raw(1, 'duplicate')
    await raw(3, 3)
    await raw(2, 'reordered')
    await raw(4, 'wrong-session', session='other')
    assert await asyncio.wait_for(values.get(), 1) == 3
    await asyncio.sleep(.12)
    monkeypatch.setattr(time, 'time', lambda: -1e12)
    await raw(4, 'expired')
    await asyncio.sleep(.02)
    assert values.empty()
    await channel.send(4)
    assert await asyncio.wait_for(values.get(), 1) == 4
    assert channel.grant['lease'] != grant['lease']


async def test_live_reconnect_rebinds_and_never_replays():
    broker = await start_managed_broker(host='127.0.0.1')
    port = broker.port
    hubs = [KinopioHub("live-tests", mesh=False, discovery=False, servers=broker.url, probe_interval=.02, timeout=.1, peer_timeout=.01) for _ in range(2)]
    replacement = None
    try:
        await asyncio.gather(*(hub.connected(3) for hub in hubs))
        values = asyncio.Queue()
        await hubs[1].live('robot').subscribe(values.put_nowait)
        await hubs[0].live('robot').send(1)
        assert await asyncio.wait_for(values.get(), 1) == 1
        old_session = hubs[1].live('robot').receivers[0].session
        await broker.close()
        async with async_timeout(3):
            while any(hub.connection.active for hub in hubs):
                await asyncio.sleep(.01)
        with pytest.raises(KinopioError):
            await hubs[0].live('robot').send(2)
        replacement = await start_managed_broker(host='127.0.0.1', port=port)
        await asyncio.gather(*(hub.connected(3) for hub in hubs))
        assert hubs[1].live('robot').receivers[0].session != old_session
        await asyncio.sleep(.03)
        assert values.empty()
        await hubs[0].live('robot').send(3)
        assert await asyncio.wait_for(values.get(), 1) == 3
    finally:
        await asyncio.gather(*(hub.close() for hub in hubs))
        await broker.close()
        if replacement:
            await replacement.close()


async def test_live_expired_grant_rtt_timeout_and_close_cleanup(pair):
    sender, _ = pair
    channel = sender.live('slow')
    connection = sender.connection.active['connection']

    async def slow(message):
        await asyncio.sleep(.03)
        await connection.publish(message.reply, p.encode({'protocol': 1, 'session': 's', 'lease': 'l', 'ttl_ms': 1}))

    sub = await connection.subscribe(f'{channel.subject}.lease', cb=slow)
    try:
        with pytest.raises(KinopioError) as error:
            await channel.send(1)
        assert error.value.code == 'EXPIRED'
        with pytest.raises(KinopioError) as error:
            await channel.send(1, timeout=.005)
        assert error.value.code == 'TIMEOUT'
        assert not any(item.subject.startswith(f'{channel.subject}.inbox.') for item in connection._subs.values())
        task = asyncio.create_task(channel.send(1))
        await asyncio.sleep(.001)
        await sender.close()
        with pytest.raises(KinopioError) as error:
            await task
        assert error.value.code == 'CLOSED'
    finally:
        if not connection.is_closed:
            await sub.unsubscribe()


async def test_live_queued_context_expires_and_invalidates_on_connection_change(pair):
    sender, receiver = pair
    values = asyncio.Queue()
    await receiver.live('context').subscribe(lambda value, context: values.put_nowait((value, context)), max_age_ms=100, with_context=True)
    await sender.live('context').send(1)
    value, context = await asyncio.wait_for(values.get(), 1)
    assert value == 1 and context.is_valid()
    assert context.expires_at > time.monotonic()
    await asyncio.sleep(.11)
    assert not context.is_valid()
    await sender.live('context').send(2)
    _, context = await asyncio.wait_for(values.get(), 1)
    assert context.is_valid()
    candidate = receiver.connection.active
    receiver.connection.active = None
    assert not context.is_valid()
    receiver.connection.active = candidate


async def test_live_refreshes_cached_lease_before_network_budget_runs_out(pair, monkeypatch):
    from types import SimpleNamespace

    sender, receiver = pair
    clock = [100.0]
    monkeypatch.setattr('kinopio_hub._live.time', SimpleNamespace(monotonic=lambda: clock[0]))
    values = asyncio.Queue()
    await receiver.live('delayed').subscribe(values.put_nowait, max_age_ms=300)
    channel = sender.live('delayed')
    sending = sender.connection.active['connection']
    receiving = receiver.connection.active['connection']
    sender_publish, receiver_publish = sending.publish, receiving.publish

    async def publish(subject, payload=b'', **kwargs):
        # Deterministic local monotonic time: 60 ms grant RTT and 70 ms data transit.
        if subject == f'{channel.subject}.lease':
            clock[0] += .03
        elif subject == f'{channel.subject}.data':
            clock[0] += .07
        await sender_publish(subject, payload, **kwargs)

    async def reply(subject, payload=b'', **kwargs):
        if subject.startswith(f'{channel.subject}.inbox.'):
            clock[0] += .03
        await receiver_publish(subject, payload, **kwargs)

    monkeypatch.setattr(sending, 'publish', publish)
    monkeypatch.setattr(receiving, 'publish', reply)
    await channel.send(1)
    assert await asyncio.wait_for(values.get(), 1) == 1
    first = channel.grant.copy()
    clock[0] = first['expiry'] - .02
    await channel.send(2)
    assert await asyncio.wait_for(values.get(), .1) == 2
    assert channel.grant['lease'] != first['lease']
    assert channel.grant['ttl_ms'] == first['ttl_ms'] == 300
    assert channel.grant['seq'] == 1
