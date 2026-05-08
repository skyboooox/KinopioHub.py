from __future__ import annotations

import logging
import socket
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

HAS_ZEROCONF = True
try:
    from zeroconf import ServiceInfo, Zeroconf
except ImportError:  # pragma: no cover - dependency is part of normal installs
    HAS_ZEROCONF = False

_SERVICE_TYPE = "_kinopio-hub._tcp.local."


@dataclass
class LeafMDNSPublisher:
    discovery_namespace: str
    node_id: str
    _zeroconf: Any = None
    _service_info: Any = None
    _logger: Any = None

    def __post_init__(self) -> None:
        self._logger = logging.getLogger("kinopio_hub.auto_leaf.mdns")

    @property
    def enabled(self) -> bool:
        return HAS_ZEROCONF

    def publish(self, manifest: dict[str, Any]) -> None:
        if not HAS_ZEROCONF:
            return

        discovery_url = str(manifest.get("discoveryUrl", ""))
        parsed = urlparse(discovery_url)
        host = parsed.hostname
        port = parsed.port
        if not host or port is None:
            return

        address = _resolve_ipv4(host)
        if address is None:
            return

        properties = {
            "nodeId": str(manifest.get("nodeId", self.node_id)),
            "namespace": str(manifest.get("discoveryNamespace", self.discovery_namespace)),
            "wssUrl": str(manifest.get("wssUrl", "")),
            "discoveryUrl": discovery_url,
            "candidateRole": str(manifest.get("candidateRole", "leader")),
            "leaderEpoch": str(manifest.get("leaderEpoch", 0)),
        }
        service_name = (
            f"KinopioHub-{self.discovery_namespace}-{self.node_id[:8]}.{_SERVICE_TYPE}"
        )
        service_info = ServiceInfo(
            type_=_SERVICE_TYPE,
            name=service_name,
            port=port,
            properties=properties,
            parsed_addresses=[address],
            server=f"kinopio-{self.node_id[:8]}.local.",
        )

        if self._zeroconf is None:
            try:
                self._zeroconf = Zeroconf()
                self._zeroconf.register_service(service_info, allow_name_change=True)
                self._service_info = service_info
            except Exception as exc:
                self._logger.warning("mDNS register failed", exc_info=exc)
                if self._zeroconf is not None:
                    self._zeroconf.close()
                self._zeroconf = None
                self._service_info = None
            return

        try:
            self._service_info = service_info
            self._zeroconf.update_service(service_info)
        except Exception as exc:
            self._logger.warning("mDNS update failed", exc_info=exc)

    def stop(self) -> None:
        if self._zeroconf is None:
            return
        try:
            if self._service_info is not None:
                self._zeroconf.unregister_service(self._service_info)
        finally:
            self._zeroconf.close()
            self._zeroconf = None
            self._service_info = None


def _resolve_ipv4(host: str) -> str | None:
    try:
        socket.inet_aton(host)
        return host
    except OSError:
        pass

    try:
        return socket.gethostbyname(host)
    except OSError:
        return None
