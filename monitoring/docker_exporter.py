#!/usr/bin/env python3
"""Minimal Prometheus exporter for Docker container metrics (WSL2-compatible)."""
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
import docker

client = docker.from_env()

_cache: dict = {"metrics": "", "ts": 0}
_lock = threading.Lock()
CACHE_TTL = 10


def fetch_container(container) -> list[str]:
    name = container.name
    state = 1 if container.status == "running" else 0
    lines = [f'docker_container_running{{name="{name}"}} {state}']

    if container.status != "running":
        return lines

    try:
        stats = container.stats(stream=False)
    except Exception:
        return lines

    cpu_delta = (
        stats["cpu_stats"]["cpu_usage"]["total_usage"]
        - stats["precpu_stats"]["cpu_usage"]["total_usage"]
    )
    sys_delta = stats["cpu_stats"].get("system_cpu_usage", 0) - stats[
        "precpu_stats"
    ].get("system_cpu_usage", 0)
    ncpu = stats["cpu_stats"].get("online_cpus") or len(
        stats["cpu_stats"]["cpu_usage"].get("percpu_usage", [1])
    )
    cpu_pct = (cpu_delta / sys_delta * ncpu * 100.0) if sys_delta > 0 else 0

    mem = stats["memory_stats"]
    mem_usage = mem.get("usage", 0) - mem.get("stats", {}).get("cache", 0)
    mem_limit = mem.get("limit", 1)

    nets = stats.get("networks", {})
    rx = sum(v["rx_bytes"] for v in nets.values())
    tx = sum(v["tx_bytes"] for v in nets.values())

    lines += [
        f'docker_container_cpu_percent{{name="{name}"}} {cpu_pct:.4f}',
        f'docker_container_memory_bytes{{name="{name}"}} {mem_usage}',
        f'docker_container_memory_limit_bytes{{name="{name}"}} {mem_limit}',
        f'docker_container_network_rx_bytes{{name="{name}"}} {rx}',
        f'docker_container_network_tx_bytes{{name="{name}"}} {tx}',
    ]
    return lines


def refresh_cache():
    containers = client.containers.list(all=True)
    results: list[list[str]] = [[] for _ in containers]

    threads = []
    for i, c in enumerate(containers):
        def worker(idx=i, cont=c):
            results[idx] = fetch_container(cont)
        t = threading.Thread(target=worker, daemon=True)
        threads.append(t)
        t.start()
    for t in threads:
        t.join(timeout=8)

    all_lines = [line for chunk in results for line in chunk]
    with _lock:
        _cache["metrics"] = "\n".join(all_lines) + "\n"
        _cache["ts"] = time.time()


def get_metrics() -> str:
    if time.time() - _cache["ts"] > CACHE_TTL:
        refresh_cache()
    with _lock:
        return _cache["metrics"]


# Warm up cache at startup in background
threading.Thread(target=refresh_cache, daemon=True).start()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return
        body = get_metrics().encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    print("Docker exporter démarré sur :8000/metrics")
    HTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
