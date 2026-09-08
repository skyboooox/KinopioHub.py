# KinopioHub.py

项目版本：**3.0.0**（尚未发布）。

[English](README.md) · [Wiki](https://github.com/skyboooox/KinopioHub/wiki/Python-ZH)

使用 asyncio，在不同语言和设备间共享 JSON 当前值。自动选择并托管共享的局域网节点，无需 Node.js 运行时。

需要 Python 3.10+。当前工作树是尚未发布的 v3 重写版，请从源码安装。

## 开始使用

在本仓库目录执行：

```sh
uv sync --extra dev
uv run python examples/watch.py
# In another terminal:
uv run python examples/basic.py
```

变量只保存在 SDK 内存。断网期间可保留当前值；最后一个副本退出后状态消失。

安装、API、配置和测试说明均在 [Wiki](https://github.com/skyboooox/KinopioHub/wiki/Python-ZH)。

问题与反馈：[GitHub Issues](https://github.com/skyboooox/KinopioHub.py/issues) · [许可证](LICENSE)
