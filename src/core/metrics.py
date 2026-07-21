"""
进程内轻量指标：供 /metrics 暴露，便于跨区运维观测。
"""
from __future__ import annotations

import threading
import time
from collections import defaultdict
from typing import DefaultDict

_lock = threading.Lock()
_start_ts = time.time()

_counters: DefaultDict[str, int] = defaultdict(int)
_gauges: dict[str, float] = {}


def incr(name: str, amount: int = 1) -> None:
  with _lock:
    _counters[name] += amount


def set_gauge(name: str, value: float) -> None:
  with _lock:
    _gauges[name] = value


def snapshot() -> dict:
  with _lock:
    return {
      "uptime_seconds": round(time.time() - _start_ts, 1),
      "counters": dict(_counters),
      "gauges": dict(_gauges),
    }


def render_prometheus(region: str) -> str:
  """导出 Prometheus text exposition 子集。"""
  data = snapshot()
  lines: list[str] = [
    f'# HELP storagent_uptime_seconds Process uptime',
    f'# TYPE storagent_uptime_seconds gauge',
    f'storagent_uptime_seconds{{region="{region}"}} {data["uptime_seconds"]}',
  ]
  for name, value in sorted(data["counters"].items()):
    metric = f"storagent_{name}"
    lines.append(f"# TYPE {metric} counter")
    lines.append(f'{metric}{{region="{region}"}} {value}')
  for name, value in sorted(data["gauges"].items()):
    metric = f"storagent_{name}"
    lines.append(f"# TYPE {metric} gauge")
    lines.append(f'{metric}{{region="{region}"}} {value}')
  return "\n".join(lines) + "\n"
