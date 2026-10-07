"""使用 MoviePilot 官方源码测试引导，隔离数据库并禁止真实外部请求。"""
import runpy
from pathlib import Path

from app.testing import bootstrap

backend_root = Path(bootstrap.__file__).resolve().parents[2]
backend_harness = runpy.run_path(str(backend_root / "tests" / "conftest.py"))
bootstrap.prepare_v2_backend(Path(__file__).resolve().parents[1])
block_real_network = backend_harness["block_real_network"]
pytest_sessionfinish = backend_harness["pytest_sessionfinish"]
