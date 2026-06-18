"""
measure_failover.py — Latency Measurement & Failover Timing Script
===================================================================
Workflow
--------
  Phase 1 — Normal operation
    Write 100 key-value pairs through the Raft cluster and record latency.

  Phase 2 — Leader failure
    Identify the current leader and kill its Docker container
    (docker stop <container>).  Then write 100 more keys, recording
    the spike during re-election and stabilisation afterwards.

  Output
    • CSV of all latency samples  (latency_results.csv)
    • PNG plot                    (latency_plot.png)
    • Console summary (avg, p50, p95, p99 for each phase + failover time)

Usage
-----
    # Make sure your Docker Compose cluster is running:
    #   docker compose up -d
    # Then:
    python measure_failover.py

    # Override defaults via env vars:
    RAFT_PEERS=http://node1:5000,http://node2:5000,http://node3:5000 \
    WRITES_PER_PHASE=100 \
    LEADER_CONTAINER=raft-node1 \
    python measure_failover.py
"""

import csv
import os
import subprocess
import time
import requests

# ---------------------------------------------------------------------------
# Optional matplotlib — gracefully degrades to CSV-only if absent
# ---------------------------------------------------------------------------
try:
    import matplotlib
    matplotlib.use("Agg")          # headless backend for containers
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    print("[warn] matplotlib not installed — skipping PNG plot. "
          "Install with:  pip install matplotlib")

from raft_client import RaftClient   # our enhanced client from raft_client.py

# ---------------------------------------------------------------------------
# Configuration (overridable via environment variables)
# ---------------------------------------------------------------------------
_raw_peers   = os.getenv("RAFT_PEERS",
               "http://localhost:5001,http://localhost:5002,http://localhost:5003")
PEERS        = [p.strip() for p in _raw_peers.split(",") if p.strip()]
WRITES_PHASE = int(os.getenv("WRITES_PER_PHASE", "100"))
WRITE_DELAY  = float(os.getenv("WRITE_DELAY_S", "0.02"))   # 20 ms between writes
CSV_PATH     = os.getenv("CSV_PATH", "latency_results.csv")
PNG_PATH     = os.getenv("PNG_PATH", "latency_plot.png")

# Docker container name for the current leader (auto-detected or set via env)
LEADER_CONTAINER_OVERRIDE = os.getenv("LEADER_CONTAINER", "")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def find_leader_container() -> str | None:
    """
    Ask each peer who the leader is, then map that URL to a Docker
    container name using `docker ps`.

    Heuristic: container names follow the pattern  <service>_<n>  or
    <service>-<n>  matching the peer hostname.
    Returns the container name or None if not found.
    """
    if LEADER_CONTAINER_OVERRIDE:
        return LEADER_CONTAINER_OVERRIDE

    leader_url = None
    for url in PEERS:
        try:
            r    = requests.get(f"{url}/raft/status", timeout=1)
            data = r.json()
            if data.get("state") == "leader":
                leader_url = url
                break
        except Exception:
            pass

    if not leader_url:
        return None

    # Extract hostname from URL, e.g. "http://node1:5000" → "node1"
    from urllib.parse import urlparse
    hostname = urlparse(leader_url).hostname

    try:
        out = subprocess.check_output(
            ["docker", "ps", "--format", "{{.Names}}"], text=True
        )
        for name in out.splitlines():
            if hostname and hostname.lower() in name.lower():
                return name.strip()
    except Exception:
        pass

    return hostname   # fall back to hostname as container name


def kill_leader(container: str):
    """Stop the Docker container running the current leader."""
    print(f"\n  >>> Killing leader container: {container}")
    try:
        subprocess.run(["docker", "stop", container], check=True, timeout=5)
        print(f"  >>> Container {container} stopped.")
    except subprocess.CalledProcessError as exc:
        print(f"  [warn] docker stop failed: {exc}")
    except FileNotFoundError:
        print("  [warn] docker not found — simulating kill by sleeping 3 s")
        time.sleep(3)


def percentile(data: list[float], pct: float) -> float:
    if not data:
        return 0.0
    s = sorted(data)
    k = (len(s) - 1) * pct / 100
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (k - lo) * (s[hi] - s[lo])


def print_stats(label: str, latencies: list[float]):
    if not latencies:
        print(f"  {label}: no data")
        return
    avg = sum(latencies) / len(latencies)
    p50 = percentile(latencies, 50)
    p95 = percentile(latencies, 95)
    p99 = percentile(latencies, 99)
    print(f"  {label:30s}  n={len(latencies):4d}  "
          f"avg={avg:7.1f} ms  p50={p50:7.1f}  p95={p95:7.1f}  p99={p99:7.1f}")


# ---------------------------------------------------------------------------
# Main measurement routine
# ---------------------------------------------------------------------------

def run():
    client = RaftClient(PEERS, http_timeout=2.0, max_retries=30)

    samples: list[dict] = []   # {"phase", "index", "latency_ms", "ok"}

    # ------------------------------------------------------------------
    # Phase 1: Normal operation
    # ------------------------------------------------------------------
    print(f"\n{'='*60}")
    print(f"Phase 1 — Normal operation ({WRITES_PHASE} writes)")
    print(f"{'='*60}")

    for i in range(WRITES_PHASE):
        ok, latency = client.write(f"key-p1-{i:04d}", f"value-{i}")
        samples.append({"phase": 1, "index": i, "latency_ms": latency, "ok": ok})
        if i % 20 == 0:
            status = "OK" if ok else "FAIL"
            print(f"  [{status}] write {i:3d}  latency={latency:7.1f} ms")
        time.sleep(WRITE_DELAY)

    phase1_latencies = [s["latency_ms"] for s in samples if s["phase"] == 1 and s["ok"]]
    print_stats("Phase 1 (normal)", phase1_latencies)

    # ------------------------------------------------------------------
    # Leader failure
    # ------------------------------------------------------------------
    print(f"\n{'='*60}")
    print("Detecting and killing the current leader …")
    print(f"{'='*60}")

    leader_container = find_leader_container()
    if leader_container:
        t_kill = time.monotonic()
        kill_leader(leader_container)
    else:
        print("  [warn] Could not detect leader container — proceeding anyway.")
        t_kill = time.monotonic()

    # ------------------------------------------------------------------
    # Phase 2: After leader failure (writes during/after re-election)
    # ------------------------------------------------------------------
    print(f"\n{'='*60}")
    print(f"Phase 2 — After leader failure ({WRITES_PHASE} writes)")
    print(f"{'='*60}")

    t_first_success_after_kill: float | None = None

    for i in range(WRITES_PHASE):
        ok, latency = client.write(f"key-p2-{i:04d}", f"value-{i}")
        elapsed_since_kill = (time.monotonic() - t_kill) * 1000  # ms
        samples.append({
            "phase": 2, "index": i, "latency_ms": latency,
            "ok": ok, "elapsed_ms": elapsed_since_kill
        })

        if ok and t_first_success_after_kill is None:
            t_first_success_after_kill = time.monotonic()
            failover_ms = (t_first_success_after_kill - t_kill) * 1000
            print(f"\n  *** First successful write after failover: "
                  f"{failover_ms:.0f} ms after kill ***\n")

        if i % 20 == 0:
            status = "OK" if ok else "FAIL"
            print(f"  [{status}] write {i:3d}  latency={latency:7.1f} ms  "
                  f"(+{elapsed_since_kill:.0f} ms since kill)")
        time.sleep(WRITE_DELAY)

    phase2_latencies = [s["latency_ms"] for s in samples if s["phase"] == 2 and s["ok"]]
    print_stats("Phase 2 (after failover)", phase2_latencies)

    # ------------------------------------------------------------------
    # Save CSV
    # ------------------------------------------------------------------
    with open(CSV_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["phase", "index", "latency_ms", "ok"])
        writer.writeheader()
        for s in samples:
            writer.writerow({k: s.get(k, "") for k in ["phase", "index", "latency_ms", "ok"]})
    print(f"\n  CSV saved → {CSV_PATH}")

    # ------------------------------------------------------------------
    # Plot
    # ------------------------------------------------------------------
    if HAS_MATPLOTLIB:
        _plot(samples, t_kill)
    else:
        print("  (skipping plot — matplotlib not available)")

    # ------------------------------------------------------------------
    # Failover summary
    # ------------------------------------------------------------------
    if t_first_success_after_kill:
        failover_ms = (t_first_success_after_kill - t_kill) * 1000
        print(f"\n  Failover time (kill → first successful write): "
              f"{failover_ms:.0f} ms")
    else:
        print("\n  No successful writes recorded in Phase 2.")


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

def _plot(samples: list[dict], t_kill: float):
    fig, ax = plt.subplots(figsize=(12, 5))

    p1 = [(s["index"],          s["latency_ms"]) for s in samples if s["phase"] == 1]
    p2 = [(s["index"] + WRITES_PHASE, s["latency_ms"]) for s in samples if s["phase"] == 2]

    if p1:
        xs, ys = zip(*p1)
        ax.plot(xs, ys, color="#4ade80", linewidth=0.8, label="Phase 1 (normal)")

    if p2:
        xs, ys = zip(*p2)
        ax.plot(xs, ys, color="#f87171", linewidth=0.8, label="Phase 2 (after failover)")

    # Mark the boundary
    ax.axvline(x=WRITES_PHASE, color="#fbbf24", linestyle="--", linewidth=1.5, label="Leader killed")

    ax.set_xlabel("Write index (sequential)")
    ax.set_ylabel("Latency (ms)")
    ax.set_title("Raft Client Write Latency — Normal Operation vs. Leader Failover")
    ax.legend()
    ax.set_yscale("symlog", linthresh=10)   # log scale to show spike clearly
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(PNG_PATH, dpi=150)
    print(f"  Plot saved  → {PNG_PATH}")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    run()
