"""Node monitoring backend (runs on the cluster head node).

Each poll runs a single ``clush -w worker01,...,workerNN <script>`` on the
head node; clush fans the sampling script out to all workers in parallel
over their passwordless SSH. The per-host ``KEY=VALUE`` lines clush prints
are parsed and kept in a short in-memory history so the frontend can
render trend charts.

Requires ``clush`` on $PATH and key-based auth from the head node to each
worker (clush reuses your existing keys and ``~/.ssh/config``).
"""

from __future__ import annotations

import json
import os
import subprocess
import threading
import time
from collections import deque

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
NODES_FILE = os.path.join(BASE_DIR, "nodes.json")

# Remote snippet: samples /proc/stat twice (1s apart) for an accurate
# overall CPU%, plus load, memory, core count, and tegrastats (Jetson
# Orin Nano) for GPU utilisation (GR3D_FREQ) and per-core CPU% (the
# "CPU [79%@1651,62%@1675,...]" list). Prints flat KEY=VALUE lines.
REMOTE_SCRIPT = r"""
read -r _ u n s i iw irq sirq st _ < /proc/stat 2>/dev/null
t1=$((u+n+s+i+iw+irq+sirq+st)); i1=$((i+iw))
sleep 1
read -r _ u n s i iw irq sirq st _ < /proc/stat 2>/dev/null
t2=$((u+n+s+i+iw+irq+sirq+st)); i2=$((i+iw))
dt=$((t2-t1)); di=$((i2-i1))
if [ "$dt" -le 0 ]; then cpu=0; else cpu=$(( (dt-di)*100/dt )); fi
read -r l1 l5 l15 _ < /proc/loadavg 2>/dev/null
mt=$(awk '/^MemTotal:/{print $2}' /proc/meminfo)
ma=$(awk '/^MemAvailable:/{print $2}' /proc/meminfo)
cc=$(nproc 2>/dev/null || echo 1)
echo "CPU=$cpu"
echo "LOAD1=$l1"
echo "LOAD5=$l5"
echo "LOAD15=$l15"
echo "MEM_TOTAL_KB=$mt"
echo "MEM_AVAIL_KB=$ma"
echo "CPU_COUNT=$cc"
# Per‑core utilisation using /proc/stat
readarray -t cores1 < <(grep -E '^cpu[0-9]+' /proc/stat)
sleep 1
readarray -t cores2 < <(grep -E '^cpu[0-9]+' /proc/stat)
for idx in "${!cores1[@]}"; do
  line1=${cores1[$idx]}
  line2=${cores2[$idx]}
  set -- $line1
  label=$1; shift
  vals1=($@)
  set -- $line2
  shift
  vals2=($@)
  total1=0; idle1=0; total2=0; idle2=0
  for v in "${vals1[@]}"; do total1=$((total1+v)); done
  idle1=${vals1[3]}
  for v in "${vals2[@]}"; do total2=$((total2+v)); done
  idle2=${vals2[3]}
  dt=$((total2-total1)); di=$((idle2-idle1))
  if [ $dt -le 0 ]; then util=0; else util=$(( (dt-di)*100/dt )); fi
  core_index=${label#cpu}
  printf "CPU_CORE_%s=%d\n" "$core_index" "$util"
done
if command -v tegrastats >/dev/null 2>&1; then
  ts=$(timeout 2 tegrastats 2>/dev/null | head -n 1)
  gu=$(echo "$ts" | awk '{for (i=1; i<=NF; i++) if ($i=="GR3D_FREQ") v=$(i+1); sub(/%/,"",v); print v}')
  [ -n "$gu" ] && echo "GPU_0_UTIL=$gu"
  echo "$ts" | awk '
    {
      for (i = 1; i <= NF; i++) if ($i == "CPU" && $(i+1) ~ /^\[/) {
        s = $(i+1)
        gsub(/[\[\]]/, "", s)
        n = split(s, C, ",")
        for (k = 1; k <= n; k++) {
          split(C[k], P, "@")
          u = P[1]
          sub(/%/, "", u)
          printf "CPU_CORE_%d=%d\n", k - 1, u
        }
        break
      }
    }'
fi
"""


def _to_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value, default=0):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def load_nodes(path=NODES_FILE):
    """Load worker specs from ``nodes.json``.

    Accepts a JSON list of host strings, or the object form
    ``{"nodes": ["host", ...]}`` (entries may also be
    ``{"host": "h", "name": "n"}`` objects).
    Returns a list of dicts: ``{"name", "host"}``.
    """
    with open(path) as fh:
        raw = json.load(fh)
    items = raw.get("nodes", raw) if isinstance(raw, dict) else raw
    nodes = []
    for i, item in enumerate(items):
        if isinstance(item, str):
            nodes.append({"name": item, "host": item})
        else:
            name = item.get("name") or item.get("host") or f"node{i}"
            nodes.append({"name": name, "host": item["host"]})
    return nodes


def _clush_command(nodes, script):
    hosts = ",".join(n["host"] for n in nodes)
    return ["clush", "-w", hosts, script]


def _parse_clush_output(stdout, stderr, hosts):
    """Split clush output into per-host values and errors.

    clush prefixes every remote line with ``host: ``. Non-metric prefixed
    lines are kept as per-host error text; the trailing "Failed hosts"
    report is captured as a set of failed hosts.
    """
    known = set(hosts)
    values, errors, failed = {}, {}, set()
    lines = (stdout or "").splitlines() + (stderr or "").splitlines()
    in_failures = False
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        host, sep, rest = stripped.partition(":")
        host, rest = host.strip(), rest.strip()
        if host in known and sep:
            if "=" in rest:
                k, v = rest.split("=", 1)
                values.setdefault(host, {})[k.strip()] = v.strip()
            else:
                errors.setdefault(host, rest)
            in_failures = False
        elif host.lower().startswith("failed hosts"):
            in_failures = True
            failed.update(tok for tok in rest.split() if tok in known)
        elif in_failures and not sep and host in known:
            failed.add(host)
    return values, errors, failed


def _fill_metrics(status, vals, started):
    status["online"] = True
    status["error"] = None
    status["cpu"] = _to_int(vals.get("CPU"))
    status["cpu_count"] = _to_int(vals.get("CPU_COUNT"))
    status["load1"] = _to_float(vals.get("LOAD1"))
    status["load5"] = _to_float(vals.get("LOAD5"))
    status["load15"] = _to_float(vals.get("LOAD15"))

    mem_total_kb = _to_int(vals.get("MEM_TOTAL_KB"))
    mem_avail_kb = _to_int(vals.get("MEM_AVAIL_KB"))
    if mem_total_kb > 0:
        used_kb = max(0, mem_total_kb - mem_avail_kb)
        status["mem_total_mb"] = mem_total_kb // 1024
        status["mem_used_mb"] = used_kb // 1024
        status["mem_pct"] = round(used_kb * 100.0 / mem_total_kb, 1)

    gpus = []
    for key, value in vals.items():
        prefix, sep, rest = key.partition("_")
        if prefix != "GPU" or not sep:
            continue
        parts = rest.split("_")  # e.g. ["0", "UTIL"]
        if len(parts) < 2:
            continue
        idx = _to_int(parts[0])
        metric = "_".join(parts[1:])
        gpu = next((g for g in gpus if g["index"] == idx), None)
        if gpu is None:
            gpu = {"index": idx, "util": 0, "mem_used_mb": 0, "mem_total_mb": 0}
            gpus.append(gpu)
        if metric == "UTIL":
            gpu["util"] = _to_int(value)
        elif metric == "MEM_USED_MB":
            gpu["mem_used_mb"] = _to_int(value)
        elif metric == "MEM_TOTAL_MB":
            gpu["mem_total_mb"] = _to_int(value)
    gpus.sort(key=lambda g: g["index"])
    status["gpus"] = gpus

    cores = []
    for key, value in vals.items():
        if key.startswith("CPU_CORE_"):
            cores.append(
                {"index": _to_int(key.rsplit("_", 1)[1]), "util": _to_int(value)}
            )
    cores.sort(key=lambda c: c["index"])
    status["cores"] = cores
    status["checked_at"] = started


def collect_batch(nodes, timeout=6):
    """Sample every node with one clush run. Returns ``{name: status}``."""
    started = time.time()

    def _status(node, error=None):
        return {
            "name": node["name"],
            "host": node["host"],
            "online": False,
            "error": error,
            "cpu": None,
            "cpu_count": None,
            "load1": None,
            "load5": None,
            "load15": None,
            "mem_total_mb": None,
            "mem_used_mb": None,
            "mem_pct": None,
            "gpus": [],
            "cores": [],
            "checked_at": started,
        }

    results = {n["name"]: _status(n) for n in nodes}

    cmd = _clush_command(nodes, REMOTE_SCRIPT)
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout + 15
        )
    except FileNotFoundError:
        for n in nodes:
            results[n["name"]]["error"] = "clush not found on this host"
        return results
    except subprocess.TimeoutExpired:
        for n in nodes:
            results[n["name"]]["error"] = "timeout"
        return results
    except Exception as exc:  # noqa: BLE001
        for n in nodes:
            results[n["name"]]["error"] = f"clush error: {exc}"
        return results

    values, errors, failed = _parse_clush_output(
        proc.stdout, proc.stderr, [n["host"] for n in nodes]
    )
    for node in nodes:
        host = node["host"]
        vals = values.get(host) or {}
        if "CPU" not in vals or "MEM_TOTAL_KB" not in vals:
            if host in failed:
                msg = errors.get(host) or "clush reported host as failed"
            else:
                msg = errors.get(host) or "no output from clush"
            results[node["name"]]["error"] = msg
            continue
        _fill_metrics(results[node["name"]], vals, started)
    return results


class Monitor:
    """Holds per-node current status + bounded history, polled in a thread."""

    def __init__(self, interval=5.0, history=180, timeout=6):
        self.nodes = load_nodes()
        self.interval = interval
        self.history = history
        self.timeout = timeout
        self._lock = threading.Lock()
        self._current = {}
        self._hist = {}
        self._stop = threading.Event()
        self._thread = None
        for node in self.nodes:
            self._hist[node["name"]] = deque(maxlen=history)

    # -- polling ---------------------------------------------------------
    def _poll_all(self):
        results = collect_batch(self.nodes, self.timeout)
        now = time.time()
        with self._lock:
            for name, st in results.items():
                self._current[name] = st
                hist = self._hist.get(name)
                if hist is None:
                    continue
                hist.append(
                    {
                        "ts": round(now, 2),
                        "cpu": st.get("cpu"),
                        "mem": st.get("mem_pct"),
                        "gpu": max((g["util"] for g in st.get("gpus", [])), default=None),
                        "online": st.get("online", False),
                    }
                )

    def _run(self):
        while not self._stop.is_set():
            self._poll_all()
            self._stop.wait(self.interval)

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=self.interval + 2)

    # -- snapshot for the API -------------------------------------------
    def snapshot(self):
        with self._lock:
            nodes = []
            for node in self.nodes:
                name = node["name"]
                st = dict(self._current.get(name, {
                    "name": name,
                    "host": node["host"],
                    "online": False,
                    "error": "waiting for first poll",
                }))
                st["history"] = list(self._hist.get(name, []))
                nodes.append(st)
            return {"interval": self.interval, "nodes": nodes}


MONITOR = Monitor(
    interval=float(os.environ.get("MONITOR_INTERVAL", "5")),
    history=int(os.environ.get("MONITOR_HISTORY", "180")),
    timeout=int(os.environ.get("MONITOR_SSH_TIMEOUT", "6")),
)
