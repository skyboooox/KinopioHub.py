"""LAN domain configuration, stable host identity, and wire member validation."""
from __future__ import annotations

import hashlib
import ipaddress
import math
import re
import socket
from typing import Any
from urllib.parse import urlsplit

import psutil

from ._protocol import js_stringify

PORT, MAX_PEERS = 44480, 32


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _stable(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _stable(value[key]) for key in sorted(value) if value[key] is not None}
    if isinstance(value, list):
        return [_stable(item) for item in value]
    return value


def config_for(options: dict[str, Any]) -> dict[str, Any]:
    settings = options.get('mesh', True)
    settings = {} if settings is True else settings
    if not isinstance(settings, dict):
        raise ValueError('mesh must be true, false, or an options object')
    if options.get('authenticator') or options.get('tls'):
        raise ValueError('Automatic LAN nodes do not support custom authenticators or client TLS')
    password = options.get('password', options.get('pass'))
    auth = {'token': options.get('token'), 'user': options.get('user'), 'pass': password}
    if auth['token'] is not None and (auth['user'] is not None or password is not None):
        raise ValueError('Automatic LAN nodes accept either token or user/password')
    if (auth['user'] is None) != (password is None) or any(v is not None and (not isinstance(v, str) or not v) for v in auth.values()):
        raise ValueError('Automatic LAN authentication requires nonempty token or user and password')
    group = settings.get('group', 'default')
    if not isinstance(group, str) or not group or len(group.encode('utf-16-le')) // 2 > 128:
        raise ValueError('mesh.group must be a nonempty string of at most 128 characters')
    binary = settings.get('binary')
    if binary is not None and (not isinstance(binary, str) or not binary or '\0' in binary):
        raise ValueError('mesh.binary must be an executable path')
    upstreams = settings.get('upstreams', [])
    if not isinstance(upstreams, list) or len(upstreams) > 16:
        raise ValueError('mesh.upstreams must be a list of at most 16 leaf endpoints')
    for endpoint in upstreams:
        try:
            parsed = urlsplit(endpoint)
            valid = isinstance(endpoint, str) and parsed.scheme in ('nats', 'tls', 'ws', 'wss') and parsed.hostname and not parsed.username and not parsed.password and not parsed.query and not parsed.fragment and parsed.path in ('', '/')
            parsed.port
        except (ValueError, TypeError):
            valid = False
        if not valid:
            raise ValueError('mesh.upstreams requires explicit leaf endpoints without credentials, query or path')
    tls = settings.get('upstream_tls')
    if tls is not None and not isinstance(tls, dict):
        raise ValueError('mesh.upstream_tls must be a TLS configuration object')
    tls_names = {'handshake_first': 'handshakeFirst', 'ca_file': 'caFile', 'cert_file': 'certFile', 'key_file': 'keyFile'}
    wire_tls = {tls_names.get(k, k): v for k, v in tls.items()} if tls is not None else None
    result: dict[str, Any] = {'group': group, 'binary': binary, 'upstreams': upstreams, 'upstream_tls': tls, **auth}
    defaults = {'heartbeat_ms': 1500, 'settle_ms': 2500, 'expiry_ms': 6500, 'probe_timeout_ms': 750, 'minimum_term_ms': 45000, 'drain_ms': 3000, 'retry_ms': 10000, 'discovery_port': PORT, 'control_port': 0}
    result.update({k: settings.get(k, v) for k, v in defaults.items()})
    for key in defaults:
        value = result[key]
        minimum = 0 if key in ('minimum_term_ms', 'drain_ms', 'control_port') else 1
        if type(value) is not int or not minimum <= value <= (65535 if key.endswith('_port') else 2147483647):
            raise ValueError(f'mesh.{key} is outside its valid range')
    if result['expiry_ms'] <= result['heartbeat_ms'] or result['probe_timeout_ms'] >= result['expiry_ms']:
        raise ValueError('mesh.expiry_ms must exceed heartbeat_ms and probe_timeout_ms')
    identity = js_stringify(_stable({'group': group, **auth, 'upstreams': sorted(upstreams), 'upstreamTls': wire_tls}))
    result['domain'], result['key'] = _digest(identity), _digest('kinopio-mesh-control-v1:' + identity)
    result['cache_key'] = _digest(js_stringify(_stable(result)))
    return result


def host_identity() -> str:
    macs = set()
    for items in psutil.net_if_addrs().values():
        if not any(a.family in (socket.AF_INET, socket.AF_INET6) for a in items) or any(a.family == socket.AF_INET and ipaddress.ip_address(a.address).is_loopback for a in items):
            continue
        for address in items:
            if address.family == psutil.AF_LINK and address.address != '00:00:00:00:00:00' and address.address:
                macs.add(address.address.lower())
    return _digest(socket.gethostname() + '|' + ','.join(sorted(macs)))[:32]


def valid_id(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r'[a-zA-Z0-9_-]{1,80}', value) is not None and value not in ('__proto__', 'constructor', 'prototype')


def _bounded(value: Any, maximum: float) -> bool:
    return type(value) in (int, float) and math.isfinite(value) and 0 <= value <= maximum


def _port(value: Any) -> bool:
    return type(value) is int and 0 < value <= 65535


def valid_member(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    try:
        if not (
            valid_id(value['id'])
            and valid_id(value['hostId'])
            and _port(value['port'])
            and type(value['seq']) is int
            and 0 <= value['seq'] <= 9007199254740991
            and (value['vote'] is None or valid_id(value['vote']))
            and _bounded(value.get('retryAfterMs', 0), 2147483647)
        ):
            return False
        uplink = value.get('uplink')
        if uplink is not None and not (
            isinstance(uplink, dict)
            and (uplink.get('reachable') is None or type(uplink['reachable']) is bool)
            and (uplink.get('rtt') is None or _bounded(uplink['rtt'], 60000))
        ):
            return False
        load = value['load']
        if not isinstance(load, dict) or not all(_bounded(load.get(k), 1) for k in ('cpu', 'memory')):
            return False
        observations = value['observations']
        if not (
            isinstance(observations, dict)
            and len(observations) <= MAX_PEERS + 1
            and all(
                valid_id(key) and isinstance(sample, dict)
                and _bounded(sample.get('rtt'), 60000) and _bounded(sample.get('loss'), 1)
                for key, sample in observations.items()
            )
        ):
            return False
        broker = value['broker']
        return broker is None or (
            isinstance(broker, dict)
            and _port(broker.get('port'))
            and _port(broker.get('wsPort'))
            and (broker.get('upstreamConnected') is None or type(broker['upstreamConnected']) is bool)
        )
    except (KeyError, TypeError, ValueError):
        return False
