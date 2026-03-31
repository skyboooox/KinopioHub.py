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
| `ws_connection_headers` | `Mapping[str, Sequence[str]]` | `None` | WebSocket 连接附加头 |
| `name` | `str` | `None` | NATS 客户端名称 |

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

## 开发

开发环境和校验步骤见 [How_To_Dev.md](./How_To_Dev.md)。

## 许可证

GPL-3.0-or-later
