# KinopioHub

云原生通信中间件，用于把远程变量和函数组织为带作用域的本地对象。

![License: GPL-3.0-or-later](https://img.shields.io/badge/license-GPL--3.0--or--later-green)

[English](./README.md)

## 特性

- 自动值跟踪与本地缓存
- 支持动态属性访问的层级作用域系统
- 统一的发布订阅与请求响应接口
- 自动重连与退避重试
- 原生支持 TCP、TLS、WebSocket 与安全 WebSocket NATS 端点
- 支持自定义编解码器

## 安装

```bash
pip install kinopio-hub
```

默认安装现在也会包含 `kinopio_hub.leaf` 所需的运行时依赖，但本地 `nats-server`
二进制仍然只会在真正启用 leaf runtime 时按需解析或下载，不会在 `pip install`
阶段偷偷执行。安装后还会自动带上 `kinopio-hub` console script，用于本地 leaf
runtime 的 CLI 启动方式。

## 快速开始

```python
import asyncio

from kinopio_hub import KinopioHub


async def main() -> None:
    hub = KinopioHub(
        servers=["wss://demo.nats.io:8443"],
        debug=True,
    )

    await hub.wait_connected()

    chat_messages = hub.chat.messages

    await chat_messages.publish({"user": "alice", "message": "hello"})

    async def on_message(data, _message) -> None:
        print("收到消息:", data)

    await chat_messages.subscribe(on_message)


asyncio.run(main())
```

## 核心概念

### `KinopioHub`

`KinopioHub` 负责管理 NATS 连接生命周期，并提供作用域入口。

```python
hub = KinopioHub(
    servers=["wss://demo.nats.io:8443"],
    debug=True,
    no_echo=False,
    reconnect_timeout=5.0,
)
```

### 作用域

作用域用于把相关变量组织到同一个 subject 前缀下。

```python
users = hub.get_scope("users")
online = users.get_variable("online")
count = users.get_variable("count")

same_online = hub.users.online
```

### 变量

变量是基于 subject 的对象，支持发布、订阅、请求响应和服务处理。

```python
await hub.system.health.publish({"cpu": 42.1})

async def on_update(data, _message) -> None:
    print("值已更新:", data)

await hub.system.health.subscribe(on_update)

response = await hub.math.calculator.request({"operation": "add", "a": 3, "b": 4})

await hub.math.calculator.serve(lambda request, _message: {"result": request["a"] + request["b"]})
```

每个变量都提供 `value` 属性，用于返回当前已知的最新值。

## 配置项

| 选项 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `servers` | `Sequence[str]` | `["wss://demo.nats.io:8443", "wss://demo.nats.io:4443"]` | NATS 服务端地址列表 |
| `debug` | `bool` | `False` | 是否启用调试日志 |
| `no_echo` | `bool` | `False` | 是否忽略当前客户端自己发布的消息 |
| `server_selection_mode` | `"ordered" \| "random" \| "latency"` | `None` | 显式指定多服务端选择策略 |
| `no_randomize` | `bool` | `True` | 是否保持服务端列表顺序不变 |
| `max_reconnect_attempts` | `int` | `-1` | 底层客户端最大重连次数 |
| `wait_on_first_connect` | `bool` | `True` | 当事件循环可用时立即开始连接 |
| `reconnect_timeout` | `float` | `5.0` | 初始连接超时时间，单位秒 |
| `reconnect_time_wait` | `float` | `0.5` | 底层重连间隔，单位秒 |
| `ping_interval` | `int` | `3` | 底层 NATS 客户端 ping 间隔 |
| `max_ping_out` | `int` | `3` | 最大未响应 ping 次数 |
| `timeout` | `float` | `3.0` | 默认请求超时时间，单位秒 |
| `health_report` | `float` | `5.0` | 调试模式下的健康报告间隔 |
| `auto_retry` | `bool` | `True` | 初始连接失败后是否自动重试 |
| `retry_delay` | `float` | `1.0` | 初始重试延迟 |
| `retry_backoff_factor` | `float` | `1.5` | 重试退避倍数 |
| `max_retry_delay` | `float` | `30.0` | 最大重试延迟 |
| `codec` | `KinopioCodec` | `None` | 自定义 `encode()` / `decode()` 编解码器 |
| `json_default` | `Callable` | `None` | `json.dumps()` 回退钩子 |
| `json_object_hook` | `Callable` | `None` | `json.loads()` 回退钩子 |
| `tls` | `ssl.SSLContext` | `None` | 自定义 TLS 上下文 |
| `tls_hostname` | `str` | `None` | 显式 TLS 主机名 |
| `tls_handshake_first` | `bool` | `False` | 对启用了 `handshake_first: true` 的服务端使用 TLS-first 握手 |
| `ws_connection_headers` | `Mapping[str, Sequence[str]]` | `None` | WebSocket 连接附加头 |
| `name` | `str` | `None` | NATS 客户端名称 |

`no_randomize` 目前作为兼容入口保留。如果没有显式设置 `server_selection_mode`，
则 `no_randomize=True` 会映射为 `"ordered"`，`no_randomize=False` 会映射为
`"random"`。如果显式设置了 `server_selection_mode`，则以它为准。`"latency"`
模式会为每个候选服务端建立短生命周期连接，并用一次 `flush()` 往返来量测 RTT，
随后按 `健康优先 -> RTT 更低优先 -> 原始输入顺序兜底` 重排候选列表；如果全部
探测都失败，则保留调用方原始顺序，并继续沿用现有连接失败流程。当 `"latency"`
模式连接到多个候选服务端后，客户端会每 10 分钟后台复测一次，并且只有在新的健康
服务端至少快 30ms 时才会执行热切换。热切换的极短双订阅窗口内，普通回调可能出现
少量重复投递，但逻辑订阅、服务处理和 `value` 跟踪会继续保持有效。

如果你的 NATS 服务端启用了 TLS-first 握手（服务端侧配置了 `tls.handshake_first: true`），
还需要同时设置 `tls_handshake_first=True`。这类端点不会先明文发送初始 `INFO`，而是
要求客户端先完成 TLS 握手。

## 连接管理

```python
await hub.wait_connected()
await hub.reconnect()
await hub.aclose()
```

监听状态变化：

```python
from kinopio_hub import ConnectionState


def handle_state(state: ConnectionState) -> None:
    print("state:", state.value)


stop = hub.on_state_change(handle_state)
stop()
```

## 本地 Leaf Runtime

阶段四新增了独立的 `kinopio_hub.leaf` 子模块，用于手动拉起本地 leaf runtime，
同时不改变主 `KinopioHub` 的根导出面。

```python
import ssl

from kinopio_hub import KinopioHub
from kinopio_hub.leaf import LeafNodeOptions, start_leaf_node


with start_leaf_node(
    LeafNodeOptions(
        backbone_servers=("nats://127.0.0.1:7422",),
    )
) as leaf:
    print(leaf.client_url)
    print(leaf.wss_url)
    print(leaf.discovery_url)
    print(leaf.monitor_url)
    print(leaf.status().bridge_state)

    tls_context = ssl.create_default_context(cafile=str(leaf.ca_cert_file))
    # 本地 TCP listener 默认保持回环；WSS 和 discovery 默认绑定局域网地址。
    hub = KinopioHub(servers=[leaf.wss_url], tls=tls_context)
```

`start_leaf_node()` 会返回 `LeafNodeHandle`，其中包含 `status()`、`stop()` 以及稳定
的 `client_url`、`wss_url`、`discovery_url`、`monitor_url`。二进制解析顺序固定为：
显式 `binary_path`、系统 `PATH`、用户缓存中的已有条目、最后才是官方最新稳定版
`nats-server` 下载。如果没有手动提供 PEM，KinopioHub 会自动生成本地 CA 和本次
运行用的 leaf 证书，但不会默认修改系统信任库。即使配置了 `backbone_servers`
但上游暂时不可达，本地节点仍然会成功启动，并把 `bridge_state` 维持在
`"connecting"`，同时保持本地流量可用。

默认缓存根目录会按平台落在 macOS 的 `~/Library/Caches/kinopio-hub`、Windows 的
`%LOCALAPPDATA%\\kinopio-hub`、以及 Linux 的 `$XDG_CACHE_HOME/kinopio-hub`
或 `~/.cache/kinopio-hub`；也可以通过 `KINOPIO_HUB_CACHE_DIR` 覆盖。默认安装已经
直接包含 leaf runtime 用到的 Python 依赖，因此不再有单独的 `leaf` extra。local
leaf runtime 的支持边界是“能启动本地 `nats-server` 子进程，并且允许本机 TCP/UDP
socket 的环境”；受限浏览器运行时和无本地进程能力的 serverless 环境不在当前支持
范围内。

手动 leaf runtime 产出的 discovery manifest 现在也会带上 JS 侧可直接识别的租约元数据，
包括 `expiresAt`、`leaseExpiresAt`、`leaderEpoch`、`isLeader` 和 `candidateRole`，
这样即使不启用自动选主 runtime，浏览器侧消费者也能先把它规范化处理。

## 自动 Leaf

阶段五在手动 runtime 之上新增了 `enable_auto_leaf()`。它会用 UDP 组播心跳作为基础
协调层，在 `zeroconf` 可用时额外做 mDNS leader 广播，把稳定 `node_id` 落到
KinopioHub 用户缓存目录，并通过一个更高层的状态面暴露发现、跟随和接管行为。

```python
import time

from kinopio_hub.leaf import AutoLeafOptions, enable_auto_leaf


handle = enable_auto_leaf(
    AutoLeafOptions(
        discovery_namespace="studio-a",
        backbone_servers=("nats://127.0.0.1:7422",),
        leader_missing_grace_ms=10_000,
    )
)

try:
    while True:
        status = handle.status()
        print(status.state, status.role, status.current_leader)
        time.sleep(1)
finally:
    handle.stop()
```

公开状态机固定为：

- `discovering`
- `following-leader`
- `leader-missing-grace`
- `electing`
- `starting-leaf`
- `leader`
- `stopped`

`AutoLeafHandle` 还提供 `state()`、`role()`、`current_leader()`、`status()` 和 `stop()`。
leader discovery manifest 现在会包含面向 JS 兼容的字段：`version`、`expiresAt`、
`leaderEpoch`、`advertisedHostname`、`wssUrl`、`fallbackServers`、`backboneRttMs`、
`discoveryUrl`、`leaseExpiresAt`、`nodeId`、`discoveryNamespace`、`isLeader`、
`candidateRole`。健康 leader 不会被立即抢主；只有当另一节点在 backbone RTT 上持续
至少领先 50ms 一段时间后，才允许执行接管。
Python 侧本身不会直接实现 JS 包里的浏览器 local probe，但会产出可被浏览器客户端
继续消费的兼容 discovery manifest。

## CLI

阶段六还补齐了 `kinopio-hub` console script，对外暴露两条 leaf 相关命令：

```bash
kinopio-hub leaf start \
  --backbone-server nats://127.0.0.1:7422 \
  --lan-bind-address 192.168.1.20 \
  --json

kinopio-hub leaf auto \
  --discovery-namespace studio-a \
  --backbone-server nats://127.0.0.1:7422
```

两条命令都支持 `--backbone-server`（可重复）、`--binary-path`、`--lan-bind-address`、
`--client-port`、`--websocket-port`、`--discovery-port`、`--monitor-port` 和 `--json`。
其中 `kinopio-hub leaf auto` 还会额外提供 `--discovery-namespace` 与
`--leader-missing-grace-ms`。`--lan-bind-address` 会映射为对外发布的 WSS / discovery
地址；本地 TCP client 与 monitor listener 默认仍保持 loopback，除非你改用 Python
API 显式覆盖。`--json` 则用于输出便于自动化消费的机器可读状态快照。

## 序列化

默认序列化规则如下：

- `None` 会编码为空 payload
- `bytes`、`bytearray`、`memoryview` 会直接转为字节
- `str` 会按 UTF-8 编码
- 其它类型默认走 JSON

如果提供了自定义 codec，会优先调用其 `encode()` 和 `decode()`。

## 服务错误

如果服务处理器抛出异常，KinopioHub 会回发如下结构：

```python
{"error": True, "message": "details"}
```

## 示例

完整示例见 [examples](./examples) 目录：

- [connection.py](./examples/connection.py)
- [publish.py](./examples/publish.py)
- [subscribe.py](./examples/subscribe.py)
- [request_reply.py](./examples/request_reply.py)
- [scope.py](./examples/scope.py)
- [leaf_runtime.py](./examples/leaf_runtime.py)
- [auto_leaf.py](./examples/auto_leaf.py)

## 开发

开发环境和校验步骤见 [How_To_Dev.md](./How_To_Dev.md)。

## 许可证

GPL-3.0-or-later
