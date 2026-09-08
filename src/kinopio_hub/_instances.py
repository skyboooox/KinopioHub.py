"""Health reports and observation of online SDK instances."""

from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, TYPE_CHECKING

from . import _protocol as p

if TYPE_CHECKING:
    from ._hub import KinopioHub

Callback = Callable[..., Any]


class Instances:
    def __init__(self, hub: KinopioHub):
        self.hub = hub
        self.instance_listeners: dict[object, Callback] = {}
        self.reports: dict[str, Any] = {}

    def receive(self, report: Any, instance: str) -> None:
        interval = report.get("interval") if isinstance(report, dict) else None
        if type(interval) is not int or not 1 <= interval <= 2147483647:
            raise p.KinopioError("INVALID_REPORT", "Invalid report interval")
        status = p.status_of(report.get("status"), instance)
        if instance not in self.reports and len(self.reports) >= self.hub.options["max_instances"]:
            self.reports.pop(next(iter(self.reports)))
        self.reports[instance] = {
            "status": status,
            "interval": interval / 1000,
            "seen": time.monotonic(),
            "lastSeen": int(time.time() * 1000),
        }
        self._expire_instances()

    def close(self) -> None:
        self.reports.clear()
        self._expire_instances()
        self.instance_listeners.clear()

    async def _report(self) -> None:
        if self.hub.connection.active and not self.hub.closed:
            await self.hub.connection._publish(
                self.hub.connection.active,
                f"{self.hub.base}.health.{self.hub.instance_id}",
                {"status": self.hub.status(), "interval": round(self.hub.options["health_interval"] * 1000)},
            )
            self.hub._clear_error("report")

    async def _health_loop(self) -> None:
        while not self.hub.closed:
            await asyncio.sleep(self.hub.options["health_interval"])
            self._expire_instances()
            try:
                await self._report()
            except Exception as error:
                self.hub._error(error, "report")

    def _instance_rows(self) -> list[Any]:
        now = time.monotonic()
        rows = []
        for row in self.reports.values():
            fresh = self.hub.state == "connected" and now - row["seen"] < row["interval"] * 3
            rows.append(
                {
                    **p.copy(row["status"]),
                    "lastSeen": row["lastSeen"],
                    "fresh": fresh,
                    "online": "unknown"
                    if self.hub.state != "connected"
                    else "online"
                    if fresh
                    else "offline",
                }
            )
        return rows

    def _expire_instances(self) -> None:
        for callback in list(self.instance_listeners.values()):
            self.hub._invoke(callback, self._instance_rows())

    def watch(self, callback: Callback) -> Callable[[], None]:
        stop = self.hub._listen(self.instance_listeners, callback)
        self.hub._invoke(callback, self._instance_rows())
        return stop

    async def list(self) -> list[dict[str, Any]]:
        await self.hub.ready()
        return self._instance_rows()
