
from kinopio_hub._compat import timeout as async_timeout
import asyncio
from typing import Any, cast

import pytest

from kinopio_hub import KinopioError, KinopioHub, UNSET
from kinopio_hub import _protocol as p


async def test_ram_snapshots_and_independent_watchers():
    async with KinopioHub(mesh=False, servers=[], peer_timeout=0.01) as hub:
        ref = hub.var("room/temperature")
        assert hub.var("room/temperature") is ref
        assert ref.value is UNSET
        values = []

        def callback(value, meta):
            values.append(value)

        first = ref.watch(callback)
        second = ref.watch(callback)
        await ref.set({"x": [1]})
        assert values == [{"x": [1]}, {"x": [1]}]
        snapshot = cast(dict[str, Any], ref.value)
        snapshot["x"].append(2)
        assert ref.value == {"x": [1]}
        first()
        await ref.set(None)
        assert ref.value is None
        assert values[-1] is None
        second()
        await ref.delete()
        assert ref.value is UNSET and ref.meta["exists"] is False
        assert ref.meta["pending"]
    assert ref.value is UNSET
    with pytest.raises(KinopioError, match="closed"):
        await ref.set(1)


async def test_discovery_empty_and_close_waiters():
    hub = KinopioHub(mesh=False, servers=[], peer_timeout=0.01)
    ref = hub.var("s/v")
    await ref.ready()
    assert ref.meta["initialized"] and ref.meta["exists"] is False
    task = asyncio.create_task(hub.connected(timeout=2))
    await asyncio.sleep(0.01)
    await hub.close()
    with pytest.raises(KinopioError) as error:
        await task
    assert error.value.code == "CLOSED"


async def test_capacity_collision_and_clock_rollback():
    async with KinopioHub(mesh=False, servers=[], max_variables=1) as hub:
        ref = hub.var("s/v")
        await ref.set(1)
        with pytest.raises(KinopioError):
            hub.var("s/another")
        original = p.copy(hub.store.records[ref.key])
        bad = {**original, "value": 2}
        with pytest.raises(KinopioError) as error:
            hub.store._commit(bad)
        assert error.value.code == "VERSION_COLLISION"
        hub.options["max_memory_bytes"] = 1
        with pytest.raises(KinopioError):
            await ref.set(3)
        assert ref.value == 1 and hub.store.clock == 1
        assert hub.store.records[ref.key] == original


async def test_callbacks_cannot_reject_write():
    errors: list[Exception] = []
    async with KinopioHub(mesh=False, servers=[], on_callback_error=errors.append) as hub:
        ref = hub.var("s/v")

        def bad(value, meta):
            raise RuntimeError("observer")

        ref.watch(bad)
        await ref.set(1)
        assert ref.value == 1 and len(errors) == 1

        async def asynchronous(value, meta):
            pass

        with pytest.raises(TypeError):
            ref.watch(asynchronous)


def test_construction_outside_loop():
    hub = KinopioHub(mesh=False, servers=[])
    assert hub._start_task is None
    asyncio.run(hub.close())


@pytest.mark.parametrize("websocket", [False, True])
async def test_real_broker_replication_and_peer_snapshot(websocket):
    from kinopio_hub._broker import start_managed_broker

    broker = await start_managed_broker(host="127.0.0.1")
    url = broker.websocket_url if websocket else broker.url
    one = KinopioHub("replication", mesh=False, servers=[url], peer_timeout=0.1, health_interval=0.1)
    two = None
    try:
        ref = one.var("s/v")
        await ref.set({"hello": "世界", "null": None})
        await one.connected()
        await one.flush()
        assert not ref.meta["pending"]
        two = KinopioHub("replication", mesh=False, servers=[url], peer_timeout=0.1, health_interval=0.1)
        other = two.var("s/v")
        await two.connected()
        async with async_timeout(3):
            while other.value != ref.value:
                await asyncio.sleep(0.01)
        await other.delete()
        await two.flush()
        async with async_timeout(3):
            while ref.value is not UNSET:
                await asyncio.sleep(0.01)
        async with async_timeout(3):
            while len(await one.instances.list()) < 2:
                await asyncio.sleep(0.01)
        assert all(row["sdk"] == "python" for row in await one.instances.list())
    finally:
        await one.close()
        if two:
            await two.close()
        await broker.close()


async def test_offline_ram_reconnect_and_empty_restart():
    from kinopio_hub._broker import start_managed_broker

    broker = await start_managed_broker(host="127.0.0.1")
    port = broker.port
    hub = KinopioHub(mesh=False, servers=[broker.url], probe_interval=0.05, timeout=0.2, peer_timeout=0.02)
    ref = hub.var("s/v")
    replacement = None
    try:
        await hub.connected(timeout=3)
        await ref.set(1)
        await hub.flush()
        await broker.close()
        async with async_timeout(3):
            while hub.status()["connection"] == "connected":
                await asyncio.sleep(0.01)
        await ref.set(2)
        assert ref.value == 2 and ref.meta["pending"]
        replacement = await start_managed_broker(host="127.0.0.1", port=port)
        await hub.connected(timeout=3)
        await hub.flush()
        assert ref.value == 2 and not ref.meta["pending"]
        assert hub.var("s/v") is ref
        writer = hub.writer
        await hub.close()
        async with KinopioHub(hub.namespace, mesh=False, servers=[replacement.url], peer_timeout=0.03) as fresh:
            await fresh.var("s/v").ready()
            assert fresh.var("s/v").value is UNSET
            assert fresh.writer != writer
    finally:
        await hub.close()
        await broker.close()
        if replacement:
            await replacement.close()


async def test_offline_writes_schedule_bounded_work():
    async with KinopioHub(mesh=False, servers=[], discovery=False) as hub:
        ref = hub.var("s/v")
        count = len(hub._tasks)
        for index in range(2000):
            await ref.set(index)
        assert len(hub._tasks) == count
        assert len(hub.store.pending) == 1 and ref.value == 1999


def test_mesh_default_has_no_implicit_external_server():
    hub = KinopioHub()
    assert hub.servers == []
    asyncio.run(hub.close())


async def test_first_connection_does_not_wait_for_blackhole_probe():
    from kinopio_hub._broker import start_managed_broker

    closed = asyncio.Event()

    async def blackhole(reader, writer):
        try:
            await reader.read()
        finally:
            writer.close()
            await writer.wait_closed()
            closed.set()

    server = await asyncio.start_server(blackhole, "127.0.0.1", 0)
    broker = await start_managed_broker(host="127.0.0.1")
    port = server.sockets[0].getsockname()[1]
    try:
        async with KinopioHub(
            mesh=False, discovery=False, servers=[f"nats://127.0.0.1:{port}", broker.url], timeout=2
        ) as hub:
            await hub.connected(timeout=0.5)
            assert hub.status()["server"] == broker.url
        await asyncio.wait_for(closed.wait(), 1)
    finally:
        server.close()
        await server.wait_closed()
        await broker.close()


async def test_failed_handoff_keeps_previous_connection():
    from kinopio_hub._broker import start_managed_broker

    one = await start_managed_broker(host="127.0.0.1")
    two = await start_managed_broker(host="127.0.0.1")
    try:
        async with KinopioHub(mesh=False, discovery=False, servers=[one.url]) as hub:
            await hub.connected()
            ref = hub.var("s/v")
            await ref.set("retained")
            await hub.flush()
            old = hub.connection.active
            candidate = await hub.connection._probe(two.url)
            original_flush = candidate["connection"].flush
            calls = 0

            async def fail_second_flush(*args, **kwargs):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise RuntimeError("handoff confirmation failed")
                await original_flush(*args, **kwargs)

            candidate["connection"].flush = fail_second_flush
            with pytest.raises(RuntimeError, match="handoff"):
                await hub.connection._activate(candidate, "test")
            assert hub.connection.active is old and not old["connection"].is_closed
            assert candidate["connection"].is_closed
            assert ref.value == "retained"
            await ref.set("still available")
            await hub.flush()
    finally:
        await one.close()
        await two.close()


async def test_peer_sync_setup_failure_can_retry(monkeypatch):
    from kinopio_hub._broker import start_managed_broker

    broker = await start_managed_broker(host="127.0.0.1")
    try:
        async with KinopioHub(mesh=False, discovery=False, servers=[broker.url], peer_timeout=0.02) as hub:
            await hub.connected()
            candidate = hub.connection.active
            async with async_timeout(2):
                while candidate["syncing"]:
                    await asyncio.sleep(0.005)
            original_publish = hub.connection._publish
            subscriptions = len(candidate["subscriptions"])

            async def fail_publish(*args, **kwargs):
                raise RuntimeError("sync setup failure")

            monkeypatch.setattr(hub.connection, "_publish", fail_publish)
            with pytest.raises(RuntimeError, match="sync setup"):
                await hub.connection._peer_sync(candidate)
            assert not candidate["syncing"]
            assert len(candidate["subscriptions"]) == subscriptions
            monkeypatch.setattr(hub.connection, "_publish", original_publish)
            await hub.connection._peer_sync(candidate)
            assert candidate["syncing"]
            async with async_timeout(2):
                while candidate["syncing"]:
                    await asyncio.sleep(0.005)
            assert len(candidate["subscriptions"]) == subscriptions
    finally:
        await broker.close()


async def test_variable_notifications_follow_pending_and_connection_changes():
    async with KinopioHub(mesh=False, servers=[], discovery=False, peer_timeout=0.01) as hub:
        one = hub.var("s/one")
        two = hub.var("s/two")
        await one.ready()
        first, second = [], []
        one.watch(lambda value, meta: first.append((value, meta)))
        two.watch(lambda value, meta: second.append((value, meta)))
        await one.set(1)
        sent = p.copy(hub.store.records[one.key])
        await one.set(2)
        hub.store._acknowledge([sent])
        assert one.meta["pending"] and len(first) == 3
        hub.store._acknowledge([hub.store.records[one.key]])
        assert not one.meta["pending"] and len(first) == 4
        assert len(second) == 1
        hub._set_state("connected")
        assert first[-1][1]["connected"] and second[-1][1]["connected"]
        hub._set_state("offline")
        assert not first[-1][1]["connected"] and not second[-1][1]["connected"]
    assert one.value is UNSET and one.meta["initialized"]
    assert first[-1][1]["version"] is None


async def test_close_keeps_initialized_metadata_without_initializing_unknown_refs():
    hub = KinopioHub(mesh=False, servers=[], discovery=False, peer_timeout=30)
    known = hub.var("s/known")
    unknown = hub.var("s/unknown")
    await known.set(1)
    await hub.close()
    assert known.meta["initialized"]
    assert not unknown.meta["initialized"]


async def test_watch_after_write_does_not_repeat_unchanged_initial_value():
    async with KinopioHub(mesh=False, servers=[], discovery=False) as hub:
        ref = hub.var("s/v")
        await ref.set(1)
        values = []
        ref.watch(lambda value, meta: values.append(value))
        ref.watch(lambda value, meta: values.append(value))
        hub.store.emit()
        assert values == [1, 1]
        await ref.set(2)
        assert values == [1, 1, 2, 2]


async def test_namespace_identity_and_removed_api():
    import uuid
    async with KinopioHub(mesh=False, servers=[], discovery=False) as one, KinopioHub(mesh=False, servers=[], discovery=False) as two:
        assert str(uuid.UUID(one.namespace, version=4)) == one.namespace
        assert one.namespace != two.namespace
        assert one.namespace not in (one.writer, one.instance_id)
        assert one.status()["namespace"] == one.namespace
        assert "name" not in one.status()
        assert not hasattr(one, "scope")
        with pytest.raises(AttributeError):
            one.namespace = "changed"
        one.options["namespace"] = "changed"
        assert one.base == p.prefix(one.namespace)
        assert one.namespace != "changed"
    for invalid in ["", "\ud800", {}, "x" * 129]:
        with pytest.raises(KinopioError):
            KinopioHub(invalid)
    with pytest.raises(KinopioError, match="no longer supported"):
        KinopioHub(name="legacy")
    with pytest.raises(TypeError):
        KinopioHub("demo", [])


async def test_wire_subject_binding_default_isolation_and_exact_sync_inbox():
    from kinopio_hub._broker import start_managed_broker

    broker = await start_managed_broker(host="127.0.0.1")
    options = dict(mesh=False, discovery=False, servers=broker.url, peer_timeout=.02)
    try:
        async with KinopioHub(**options) as isolated, KinopioHub("literal.*", **options) as hub:
            await asyncio.gather(isolated.connected(), hub.connected())
            raw = hub.connection.active["connection"]
            values = asyncio.Queue()
            replies = asyncio.Queue()
            subject = p.key(hub.namespace, "x.*>")
            subscription = await raw.subscribe(subject, cb=values.put)
            await hub.var("x.*>").set(1)
            await hub.flush()
            message = await asyncio.wait_for(values.get(), 1)
            assert message.subject == subject
            assert "scope" not in p.decode(message.data)
            await isolated.var("x.*>").ready()
            assert isolated.var("x.*>").value is UNSET
            bad = {"name": "different", "version": {"counter": "99", "writer": "raw"}, "value": 2}
            await raw.publish(subject, p.encode(bad))
            await raw.flush()
            async with async_timeout(1):
                while hub.status()["currentError"] is None:
                    await asyncio.sleep(.005)
            assert hub.status()["currentError"]["code"] == "INVALID_RECORD"
            assert hub.var("different").value is UNSET
            assert hub.var("x.*>").value == 1
            bad_reply = f"{hub.base}.inbox.extra.suffix"
            reply_sub = await raw.subscribe(bad_reply, cb=replies.put)
            await raw.publish(f"{hub.base}.sync", p.encode({"instanceId": "requester"}), reply=bad_reply)
            await raw.flush()
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(replies.get(), .05)
            await reply_sub.unsubscribe()
            await subscription.unsubscribe()
    finally:
        await broker.close()
