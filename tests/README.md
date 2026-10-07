# 插件测试

测试依赖包含 `app/testing` 和 `tests/conftest.py` 的 MoviePilot v2 官方源码及其运行依赖，并使用该宿主的 pytest。把宿主源码根目录加入 `PYTHONPATH`，从插件仓库运行：

```bash
PYTHONPATH=/path/to/MoviePilot env -u CONFIG_DIR /path/to/MoviePilot/venv/bin/python -m pytest -q tests
```

此云环境已准备好宿主，可直接执行：

```bash
source /workspace/.onboarding/moviepilot-plugins/activate.sh
cd /workspace/MoviePilot-Plugins
env -u CONFIG_DIR python -m pytest -q tests
```

官方测试引导使用临时 SQLite 配置，并阻止真实外部网络。回归覆盖缺少 `_session` 的 API、任务列表去重、媒体库查询、普通与精确扫描，以及真实宿主的认证、中文请求体和签名；不需要 NAS 账号。
