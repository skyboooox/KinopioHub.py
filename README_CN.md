# KinopioHub.py

[English](README.md) · [Wiki](https://github.com/skyboooox/KinopioHub/wiki/Python-ZH)

使用 asyncio，在不同语言和设备间共享 JSON 当前值。自动选择并托管共享的局域网节点，无需 Node.js 运行时。

需要 Python 3.10+。请使用配套源码。

## 开始使用

在本仓库目录执行：

```sh
uv sync --extra dev
uv run python examples/watch.py
# In another terminal:
uv run python examples/basic.py
```

使用 `python -m pip install -e /path/to/KinopioHub.py` 安装本源码。

命名空间在创建后只读；省略时每个实例生成独立 UUID。共享数据请显式使用相同命名空间，例如 `KinopioHub("demo")`，再通过 `hub.var("battery")` 访问变量。

变量只保存在 SDK 内存。断网期间可保留当前值；最后一个副本退出后状态消失。

安装、API、配置和测试说明均在 [集中手册](https://github.com/skyboooox/KinopioHub/blob/main/docs/python.zh.md) 与 [Wiki](https://github.com/skyboooox/KinopioHub/wiki/Python-ZH)。

问题与反馈：[GitHub Issues](https://github.com/skyboooox/KinopioHub.py/issues) · [许可证](LICENSE)

## 事件与请求

Hub 连接后，在同一个稳定引用上使用状态、事件和请求：

```python
battery = hub.var("battery")
stop = battery.watch_value(lambda value: print("State:", value))
subscription = await battery.sub(lambda value: print("Event:", value))
responder = await battery.handle(lambda _: battery.get(0))
await battery.set(80)
await battery.pub(81)  # Independent event; local state remains 80.
print(await battery.req())  # Omitted request data is JSON null.
await hub.drain(timeout=5)
stop()
```

`pub/sub/req` 分别是 `publish/subscribe/request` 的短名。消息回调默认只接收数据；通过 `with_context=True` 读取主题、Headers 或显式回复。事件要求活动连接，不会重放。`request_many` 有界收集回复；`handle` 自动回复回调返回值。详见[消息 API](https://github.com/skyboooox/KinopioHub/blob/main/docs/python-api.zh.md#messaging)和[可运行示例](examples/messaging.py)。
