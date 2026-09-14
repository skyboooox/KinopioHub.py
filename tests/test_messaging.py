"""Real-broker tests of messaging ownership, queues and lifetime boundaries."""
import asyncio
import json
from pathlib import Path

import pytest

from kinopio_hub import Headers, KinopioError, KinopioHub, UNSET
from kinopio_hub._broker import start_managed_broker
from kinopio_hub._messaging import subject


@pytest.fixture
async def peers():
    broker = await start_managed_broker(host='127.0.0.1')
    hubs = [KinopioHub('messages.测试', mesh=False, discovery=False, servers=broker.url,
                       peer_timeout=.01) for _ in range(2)]
    try:
        await asyncio.gather(*(hub.connected() for hub in hubs))
        yield hubs
    finally:
        await asyncio.gather(*(hub.close() for hub in hubs))
        await broker.close()


async def test_events_state_sugars_and_headers(peers):
    a, b = peers
    ref = a.var('sensor.温度')
    assert ref.get(7) == 7 and ref.value is UNSET
    assert a.var(ref.name) is ref
    values = []
    stop = ref.watch_value(values.append)
    for value in (None, False, 0, '', [], {}):
        await ref.set(value)
        assert ref.get(7) == value
    await ref.delete()
    assert ref.get(7) == 7
    stop()
    seen = []
    sub = await b.var('sensor.*').sub(lambda data, ctx: seen.append((data, ctx)), with_context=True)
    await ref.pub({'v': 2}, headers={'Foo': ['a', 'b'], 'foo': 'c'})
    await a.flush()
    for _ in range(100):
        if seen:
            break
        await asyncio.sleep(.005)
    assert seen[0][0] == {'v': 2}
    assert seen[0][1].topic == 'sensor.温度'
    assert seen[0][1].headers.get_all('FOO') == ['a', 'b', 'c']
    assert seen[0][1].headers.get('foo') == 'a'
    assert ref.value is UNSET
    assert '73656e736f722e2a' not in b.store.records
    await sub.drain()


async def test_requests_auto_explicit_partial_and_errors(peers):
    a, b = peers
    async def handler(data, ctx):
        assert not hasattr(ctx, 'reply')
        ctx.reply_headers = {'X-Test': ['1', '2']}
        return data
    sub = await b.var('rpc').handle(handler, with_context=True)
    reply = await a.var('rpc').req(details=True)
    assert reply.data is None and reply.headers.get_all('x-test') == ['1', '2']
    await sub.unsubscribe()
    with pytest.raises(KinopioError) as absent:
        await a.var('rpc').request()
    assert absent.value.code == 'NO_RESPONDERS'
    async def multi(data, ctx):
        await ctx.reply(1)
        await ctx.reply(2)
    sub = await b.var('rpc').sub(multi, with_context=True)
    result = await a.var('rpc').request_many(timeout=.05, details=True)
    assert [r.data for r in result.replies] == [1, 2] and result.reason == 'deadline'
    assert await a.var('rpc').request_many(max_replies=2) == [1, 2]
    with pytest.raises(KinopioError) as overflow:
        await a.var('rpc').request_many(max_bytes=40)
    assert overflow.value.code == 'BUFFER_OVERFLOW'
    assert len(overflow.value.partial_replies) <= 1
    await sub.unsubscribe()
    calls = []
    def broken(data):
        calls.append(data)
        raise TypeError('one invocation only')
    await b.var('rpc').handle(broken)
    with pytest.raises(KinopioError) as timeout:
        await a.var('rpc').req(timeout=.05)
    assert timeout.value.code == 'TIMEOUT' and calls == [None]
    assert b.status()['currentError']['code'] == 'HANDLER_ERROR'


async def test_queue_and_wildcard_boundaries(peers):
    a, b = peers
    counts = [0, 0]
    def receive(i):
        def callback(data):
            counts[i] += 1
        return callback
    await a.var('jobs.>').sub(receive(0), queue='workers.组')
    await b.var('jobs.>').sub(receive(1), queue='workers.组')
    for n in range(20):
        await a.var('jobs.one').pub(n)
    await a.var('jobs').pub(30)
    await a.flush()
    for _ in range(100):
        if sum(counts) == 20:
            break
        await asyncio.sleep(.005)
    assert sum(counts) == 20 and all(counts)
    await a.var('jobs.*').set(4)
    assert a.var('jobs.*').get() == 4
    for topic in ('jobs.*', 'jobs.>', 'a..b', 'a.>.b', 'a*b'):
        with pytest.raises(KinopioError) as invalid:
            await a.var(topic).pub(1)
        assert invalid.value.code == 'INVALID_TOPIC'


async def test_bounded_slow_recovery_and_drain_reentry(peers):
    a, b = peers
    entered, release = asyncio.Event(), asyncio.Event()
    errors = []
    async def slow(data):
        entered.set()
        for drain in (b.drain, sub.drain):
            try:
                await drain()
            except KinopioError as error:
                errors.append(error.code)
        await release.wait()
    sub = await b.var('slow').sub(slow, pending_messages=2, pending_bytes=1000)
    await a.var('slow').pub(0)
    await entered.wait()
    for n in range(30):
        await a.var('slow').pub(n)
    await a.flush()
    await asyncio.sleep(.02)
    status = sub.status()
    assert status['droppedMessages'] > 0 and status['highWaterMessages'] <= 2
    assert status['pendingBytes'] <= 1000
    assert errors[:2] == ['DRAIN_IN_HANDLER', 'DRAIN_IN_HANDLER']
    release.set()
    await sub.drain()
    assert b.status()['messaging']['pendingBytes'] == 0
    assert b.status()['currentError'] is None


async def test_drain_completes_accepted_reply_and_rejects_new_work(peers):
    a, b = peers
    entered = asyncio.Event()
    async def handler(data):
        entered.set()
        await asyncio.sleep(.05)
        return 42
    await b.var('rpc').handle(handler)
    request = asyncio.create_task(a.var('rpc').req())
    await entered.wait()
    drain = asyncio.create_task(b.drain(timeout=1))
    await asyncio.sleep(.01)
    for work in (b.var('rpc').pub(1), b.var('rpc').set(1), b.var('rpc').req()):
        with pytest.raises(KinopioError) as draining:
            await work
        assert draining.value.code == 'DRAINING'
    assert await request == 42
    await drain
    assert b.closed


async def test_timeout_cancel_and_disconnect_release(peers):
    a, b = peers
    contexts = []
    entered = asyncio.Event()
    async def wait(data, ctx):
        contexts.append(ctx)
        await ctx.reply(1)
        entered.set()
        await asyncio.sleep(10)
    await b.var('rpc').sub(wait, with_context=True)
    task = asyncio.create_task(a.var('rpc').request_many())
    await entered.wait()
    await asyncio.sleep(.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert [r.data for r in task.partial_replies] == [1]
    assert a.status()['messaging']['pendingBytes'] == 0
    with pytest.raises(KinopioError) as deadline:
        await b.drain(timeout=.03)
    assert deadline.value.code == 'DRAIN_TIMEOUT'
    with pytest.raises(KinopioError) as stale:
        await contexts[0].reply(2)
    assert stale.value.code == 'DISCONNECTED'


def test_header_validation_and_vectors():
    header = Headers({'Foo': ['a', 'b'], 'foo': 'c'})
    assert header.items() == [('Foo', 'a'), ('Foo', 'b'), ('foo', 'c')]
    for values in ({'x\ny': 'z'}, {'a': '\r'}, {'a': '非ASCII'}):
        with pytest.raises(KinopioError):
            Headers(values)
    with pytest.raises(KinopioError):
        Headers({'x': ['a'] * 33})
    fixture = Path(__file__).parents[2] / 'KinopioHub/integration/fixtures/message-vectors.json'
    if fixture.exists():
        vectors = json.loads(fixture.read_text())
        for case in vectors['cases']:
            for method, pattern in [('publish', False), ('subscribe', True)]:
                if case[method]:
                    assert subject(vectors['namespace'], case['name'], pattern) == case['subject']
                else:
                    with pytest.raises(KinopioError):
                        subject(vectors['namespace'], case['name'], pattern)
    assert subject('workshop', 'sensor.*', True) == '_msg.v1.776f726b73686f70.73656e736f72.*'


async def test_disconnected_and_pure_reference_limit():
    hub = KinopioHub(mesh=False, servers=[], max_variables=1)
    try:
        ref = hub.var('only')
        with pytest.raises(KinopioError) as disconnected:
            await ref.req()
        assert disconnected.value.code == 'DISCONNECTED'
        assert not hub.store.records
        with pytest.raises(KinopioError):
            hub.var('second')
    finally:
        await hub.close()


async def test_invalid_replies_do_not_turn_into_timeouts(peers):
    a, b = peers
    client = b.connection.active['connection']
    for raw, expected in [(b'not json', 'INVALID_MESSAGE'), (b'"' + b'x' * 65536 + b'"', 'MESSAGE_TOO_LARGE'), (b'9007199254740992', 'INVALID_MESSAGE')]:
        async def corrupt(data, ctx):
            await client.publish(ctx._reply, raw)
        sub = await b.var('bad').sub(corrupt, with_context=True)
        with pytest.raises(KinopioError) as error:
            await a.var('bad').req(timeout=.3)
        assert error.value.code == expected
        await sub.unsubscribe()
    for data, expected in [(float('nan'), 'INVALID_MESSAGE'), ('x' * 65537, 'MESSAGE_TOO_LARGE')]:
        with pytest.raises(KinopioError) as error:
            await a.var('bad').pub(data)
        assert error.value.code == expected


async def test_unsubscribed_running_handler_is_still_drained(peers):
    a, b = peers
    entered, release = asyncio.Event(), asyncio.Event()
    async def delayed(data):
        entered.set()
        await release.wait()
        return 'done'
    sub = await b.var('retired').handle(delayed)
    task = asyncio.create_task(a.var('retired').req())
    await entered.wait()
    await sub.unsubscribe()
    assert b.status()['messaging']['inFlightHandlers'] == 1
    drain = asyncio.create_task(b.drain(timeout=.5))
    await asyncio.sleep(.01)
    assert not drain.done()
    release.set()
    assert await task == 'done'
    await drain


async def test_handoff_preserves_subscription_and_invalidates_request(peers):
    a, b = peers
    broker = await start_managed_broker(host='127.0.0.1')
    other = KinopioHub(a.namespace, mesh=False, discovery=False, servers=broker.url, peer_timeout=.01)
    try:
        await other.connected()
        seen = []
        sub = await a.var('event').sub(seen.append)
        await b.var('wait').sub(lambda data: None)
        request = asyncio.create_task(a.var('wait').req())
        await asyncio.sleep(.01)
        candidate = await a.connection._probe(broker.url)
        await a.connection._activate(candidate, 'test-switch')
        with pytest.raises(KinopioError) as error:
            await request
        assert error.value.code == 'DISCONNECTED'
        await b.var('event').pub('old')
        await other.var('event').pub('new')
        await other.flush()
        for _ in range(100):
            if seen:
                break
            await asyncio.sleep(.005)
        assert seen == ['new'] and sub.ready
        assert a.status()['messaging']['pendingRequests'] == 0
    finally:
        await other.close()
        await broker.close()


async def test_aggregate_and_outbound_budgets(peers):
    a, b = peers
    b.messaging.limits['pending_messages'] = 2
    gate = asyncio.Event()
    async def blocked(data):
        await gate.wait()
    left = await b.var('left').sub(blocked)
    right = await b.var('right').sub(blocked)
    for _ in range(10):
        await a.var('left').pub(1)
        await a.var('right').pub(2)
    await a.flush()
    await asyncio.sleep(.02)
    assert b.status()['messaging']['pendingMessages'] <= 2
    assert b.status()['messaging']['droppedMessages'] >= 18
    gate.set()
    await left.drain()
    await right.drain()
    assert b.status()['messaging']['pendingBytes'] == 0
    a.messaging.limits['outbound_bytes'] = 1
    with pytest.raises(KinopioError) as error:
        await a.var('left').pub(1)
    assert error.value.code == 'BUFFER_OVERFLOW'


async def test_permission_rejection_fails_registration_and_request(tmp_path):
    from kinopio_hub._broker import ensure_managed_broker
    binary = await ensure_managed_broker()
    denied = subject('permissions', 'denied')
    config = tmp_path / 'nats.json'
    config.write_text(json.dumps({
        'host': '127.0.0.1', 'port': -1, 'ports_file_dir': str(tmp_path),
        'authorization': {'users': [{'user': 'test', 'password': 'test', 'permissions': {
            'publish': {'allow': ['>'], 'deny': [denied]},
            'subscribe': {'allow': ['>'], 'deny': [denied]},
        }}]},
    }))
    process = await asyncio.create_subprocess_exec(str(binary), '-c', str(config),
                    stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
    hub = None
    try:
        for _ in range(200):
            ports = list(tmp_path.glob('*.ports'))
            if ports:
                break
            await asyncio.sleep(.005)
        url = json.loads(ports[0].read_text())['nats'][0]
        hub = KinopioHub('permissions', mesh=False, discovery=False, servers=url,
                         user='test', password='test', peer_timeout=.01)
        await hub.connected()
        with pytest.raises(KinopioError) as error:
            await hub.var('denied').sub(lambda data: None)
        assert error.value.code == 'PERMISSION_DENIED'
        assert hub.status()['messaging']['subscriptions'] == 0
        with pytest.raises(KinopioError) as error:
            await hub.var('denied').req(timeout=.5)
        assert error.value.code == 'PERMISSION_DENIED'
        assert hub.status()['messaging']['pendingRequests'] == 0
    finally:
        if hub:
            await hub.close()
        if process.returncode is None:
            process.terminate()
        await process.wait()


async def test_oversized_native_headers_fail_request_and_mutation(peers):
    a, b = peers
    client = b.connection.active['connection']
    async def too_many(data, context):
        await client.publish(context._reply, b'null', headers={f'X-{i}': 'v' for i in range(33)})
    await b.var('header').sub(too_many, with_context=True)
    with pytest.raises(KinopioError) as error:
        await a.var('header').req(timeout=.3)
    assert error.value.code == 'MESSAGE_TOO_LARGE'
    header = Headers({'Foo': ['a', 'b']})
    header.update({'foo': 'c'})
    assert header.copy().get_all('foo') == ['a', 'b', 'c']
    assert header.pop('FOO') == 'a' and not header
    header['x'] = '1'
    header.clear()
    assert not header.items() and not header


async def test_drain_total_deadline_includes_slow_owned_resource_cleanup(peers):
    _, hub = peers
    gate = asyncio.Event()
    class SlowResource:
        async def close(self):
            await gate.wait()
    hub.mesh_manager = SlowResource()
    start = asyncio.get_running_loop().time()
    try:
        with pytest.raises(KinopioError) as error:
            await hub.drain(timeout=.04)
        assert error.value.code == 'DRAIN_TIMEOUT'
        assert asyncio.get_running_loop().time() - start < .2
        assert hub.connection.active is None
    finally:
        gate.set()
        await hub.close()


async def test_closed_transport_does_not_mask_request_failure(peers):
    a, b = peers
    await b.var('waiting').sub(lambda data: None)
    task = asyncio.create_task(a.var('waiting').req())
    await asyncio.sleep(.01)
    await a.connection.active['connection'].close()
    with pytest.raises(KinopioError) as error:
        await task
    assert error.value.code == 'DISCONNECTED'
    assert a.status()['messaging']['pendingBytes'] == 0


async def test_handler_child_can_drain_after_callback_returns(peers):
    a, b = peers
    gate = asyncio.Event()
    tasks = []
    async def handler(data):
        async def outside():
            await gate.wait()
            await b.drain(timeout=.5)
        tasks.append(asyncio.create_task(outside()))
        return 'accepted'
    await b.var('later').handle(handler)
    assert await a.var('later').req() == 'accepted'
    gate.set()
    await tasks[0]
    assert b.closed
