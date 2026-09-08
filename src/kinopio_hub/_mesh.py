"""Reference-counted LAN managers shared within an event loop."""
from __future__ import annotations

import asyncio
from typing import Any, Callable

from ._mesh_config import config_for
from ._mesh_manager import MeshManager


_managers: dict[tuple[asyncio.AbstractEventLoop, str], dict[str, Any]] = {}


class MeshHandle:
    def __init__(self, status: Callable[[], dict[str, Any]], close: Callable[[], Any]):
        self.status, self._close = status, close
        self._closing: asyncio.Task[Any] | None = None

    async def close(self) -> None:
        if self._closing is None:
            self._closing = asyncio.create_task(self._close())
        await asyncio.shield(self._closing)


async def acquire_mesh(options: dict[str, Any] | None = None, callbacks: dict[str, Any] | None = None) -> MeshHandle:
    options, callbacks = options or {}, callbacks or {}
    if options.get('mesh') is False:
        status = {'role': 'disabled', 'leaderId': None, 'members': 0, 'reason': 'Automatic LAN nodes disabled', 'upstreamConnected': None}
        MeshManager.call(callbacks.get('on_status'), status.copy())
        MeshManager.call(callbacks.get('on_leader'), None)
        async def noop() -> None:
            pass
        return MeshHandle(lambda: status.copy(), noop)
    config = config_for(options)
    key = (asyncio.get_running_loop(), config['cache_key'])
    entry = _managers.get(key)
    if entry is None:
        manager = MeshManager(options)
        entry = {'manager': manager, 'references': 0, 'ready': asyncio.create_task(manager.start())}
        _managers[key] = entry
    entry['references'] += 1
    try:
        await asyncio.shield(entry['ready'])
    except BaseException:
        entry['references'] -= 1
        if entry['references'] == 0:
            _managers.pop(key, None)
            entry['ready'].cancel()
            await asyncio.gather(entry['ready'], return_exceptions=True)
            await entry['manager'].close()
        raise
    unsubscribe = entry['manager'].subscribe(callbacks)
    async def close() -> None:
        unsubscribe()
        entry['references'] -= 1
        if entry['references'] == 0:
            if _managers.get(key) is entry:
                del _managers[key]
            await entry['manager'].close()
    return MeshHandle(entry['manager'].status, close)
