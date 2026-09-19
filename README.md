# Cluster Node Monitor

A small, self-contained dashboard for an HPC cluster. It runs on the
**head node** and shows live CPU, memory, load and GPU utilisation for a
set of compute workers, with short trend charts per node.

## How it works

- `collector.py` polls every 5 s (configurable). Each poll runs a single
  `clush -w worker01,worker02,worker03,worker04 <script>` on the head
  node; clush fans the script out to all workers in parallel over their
  passwordless SSH.
- The script samples `/proc/stat` (two reads 1 s apart → overall CPU %),
  `/proc/loadavg`, `/proc/meminfo` and `nproc`, plus `tegrastats` (Jetson
  Orin Nano) for GPU utilisation and the per-core CPU % list, and prints
  flat `KEY=VALUE` lines.
- The collector parses the clush output, keeps a short in-memory history
  per node (no disk, no database), and serves it as JSON.
- `app.py` (Flask) + `templates/index.html` render one card per node with
  gauges, load, GPU bars and Chart.js trend lines. Each card has a
  per-core CPU breakdown behind a click toggle. The header shows the
  EESSI logo and links to the [EESSI status page](https://status.eessi.io/).

## Requirements

- Python 3.8+ with Flask (a ready `venv/` is included; otherwise
  `python -m venv venv && venv/bin/pip install -r requirements.txt`)
- `clush` installed on the head node and on `$PATH`
- Passwordless SSH from the head node to every worker (clush reuses your
  existing keys and `~/.ssh/config`)

## Configuration

`nodes.json` — the workers to monitor:

```json
{
  "nodes": ["worker01", "worker02", "worker03", "worker04"]
}
```

Add or remove entries and restart the app; nothing else is persisted.

Environment variables:

| Variable             | Default  | Meaning                                        |
| -------------------- | -------- | ---------------------------------------------- |
| `MONITOR_INTERVAL`   | `5`      | Seconds between polls                          |
| `MONITOR_HISTORY`    | `180`    | Samples kept per node (≈ minutes at 5 s)       |
| `MONITOR_SSH_TIMEOUT`| `6`      | Base of the per-poll time budget (seconds)     |
| `PORT`               | `8080`   | Web port                                       |

## Running

On the head node:

```sh
venv/bin/python app.py
```

The app binds to `127.0.0.1` only. Open `http://localhost:8080` on the head
node, or tunnel it: `ssh -L 8080:localhost:8080 <head-node>`.
JSON API: `GET /api/status`.

To keep it alive, run it under systemd or `tmux`/`nohup`, e.g.:

```sh
nohup venv/bin/python app.py >/var/tmp/monitor.log 2>&1 &
```

## Troubleshooting

- **Node offline — "clush not found on this host"**: the app is not
  running where clush is installed; start it on the head node (or add
  clush to `PATH`).
- **Node offline — "no output from clush"**: clush produced nothing for
  that worker; check manually with `clush -w workerXX hostname`.
- **Node offline — "ssh: connect to host …"**: that worker is
  unreachable from the head node (down, or SSH keys broken for it).
- **Node offline — "Permission denied (publickey)"**: fix key-based auth
  from the head node to that worker.
- **Node offline — "timeout"**: the whole clush run exceeded
  `MONITOR_SSH_TIMEOUT + 15` s, usually because a hung worker held an SSH
  connection open.
- Data is in-memory only: restarting the app resets the trend charts.
