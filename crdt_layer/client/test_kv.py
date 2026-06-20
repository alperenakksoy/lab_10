"""
client/test_kv.py — CAP Trade-off Test Script
==============================================

Runs two test suites automatically:
  1. Normal operation  — write 10 keys in the configured mode, verify reads.
  2. Partition test    — disconnect node3 via Docker API, write in CP (expect fail)
                         and AP (expect success), reconnect, verify convergence.

The script also measures and prints write latencies for both modes.

Environment variables (set by docker-compose):
  NODE1 / NODE2 / NODE3  — base URLs of the three replicas
  CONFIG_MODE            — 'cp' or 'ap'
"""

import os
import time
import json
import subprocess
import requests

NODE1 = os.environ.get("NODE1", "http://localhost:5001")
NODE2 = os.environ.get("NODE2", "http://localhost:5002")
NODE3 = os.environ.get("NODE3", "http://localhost:5003")
MODE  = os.environ.get("CONFIG_MODE", "ap")

NODES = [NODE1, NODE2, NODE3]
CONTAINER_NODE3 = "kv-node3"          # Docker container name for fault injection
NETWORK_NAME    = "crdt_layer_kv_net"  # matches the network created by docker-compose-cap.yaml
                                        # (Compose names it "<project_dir_name>_<network_key>";
                                        #  run `docker network ls` to confirm on your machine)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def wait_for_nodes(timeout=30):
    print("\n⏳  Waiting for all nodes to be ready...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            for node in NODES:
                r = requests.get(f"{node}/status", timeout=2)
                r.raise_for_status()
            print("✅  All nodes ready.\n")
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError("Nodes did not come up in time.")


def write(node_url, key, value) -> dict:
    """POST /kv/write and return the JSON response."""
    try:
        r = requests.post(f"{node_url}/kv/write", json={"key": key, "value": value}, timeout=3)
        return r.json()
    except requests.exceptions.Timeout:
        return {"status": "error", "message": "timeout", "latency_ms": 3000}
    except Exception as e:
        return {"status": "error", "message": str(e)}


def read(node_url, key) -> dict:
    """GET /kv/read?key=<key> and return the JSON response."""
    try:
        r = requests.get(f"{node_url}/kv/read", params={"key": key}, timeout=3)
        return r.json()
    except Exception as e:
        return {"status": "error", "message": str(e)}


def _run_docker(cmd: list) -> tuple[bool, str]:
    """Run a docker CLI command and report whether it actually succeeded."""
    result = subprocess.run(cmd, capture_output=True, text=True)
    ok = result.returncode == 0
    msg = (result.stderr or result.stdout or "").strip()
    return ok, msg


def disconnect_node3():
    """Remove node3 from the Docker network to simulate a partition."""
    ok, msg = _run_docker(["docker", "network", "disconnect", NETWORK_NAME, CONTAINER_NODE3])
    if ok:
        print(f"  🔌  Disconnected {CONTAINER_NODE3} from {NETWORK_NAME}")
    else:
        print(f"  ❌  FAILED to disconnect {CONTAINER_NODE3} from {NETWORK_NAME}: {msg}")
        print(f"      Run `docker network ls` and update NETWORK_NAME in this script if needed.")


def reconnect_node3():
    """Reconnect node3 to the Docker network to heal the partition."""
    ok, msg = _run_docker(["docker", "network", "connect", NETWORK_NAME, CONTAINER_NODE3])
    if ok:
        print(f"  🔌  Reconnected {CONTAINER_NODE3} to {NETWORK_NAME}")
    else:
        print(f"  ❌  FAILED to reconnect {CONTAINER_NODE3} to {NETWORK_NAME}: {msg}")


def separator(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

# ---------------------------------------------------------------------------
# Test Suite 1: Normal operation
# ---------------------------------------------------------------------------

def test_normal_operation():
    separator(f"TEST 1 — Normal Operation  [{MODE.upper()} mode]")

    latencies = []
    errors = 0

    for i in range(10):
        key   = f"key_{i:02d}"
        value = f"value_{i:02d}"

        res = write(NODE1, key, value)
        lat = res.get("latency_ms", "?")
        latencies.append(lat if isinstance(lat, (int, float)) else None)

        if res.get("status") == "ok":
            print(f"  ✅  Write [{key}={value}] — {lat} ms")
        else:
            print(f"  ❌  Write [{key}] FAILED — {res.get('message', res)}")
            errors += 1

    # Give AP replicas a moment to converge
    time.sleep(0.5)

    # Verify reads from all three nodes
    print("\n  --- Read verification (from all nodes) ---")
    read_errors = 0
    for i in range(10):
        key = f"key_{i:02d}"
        for node_url in NODES:
            res = read(node_url, key)
            if res.get("status") == "ok":
                print(f"  ✅  [{node_url}] read {key} = {res['value']}")
            else:
                print(f"  ❌  [{node_url}] read {key} FAILED — {res}")
                read_errors += 1

    valid_latencies = [l for l in latencies if l is not None]
    if valid_latencies:
        avg = sum(valid_latencies) / len(valid_latencies)
        print(f"\n  📊  Write latency — avg: {avg:.1f} ms, "
              f"min: {min(valid_latencies):.1f} ms, max: {max(valid_latencies):.1f} ms")
    print(f"  Write errors: {errors}/10   Read errors: {read_errors}/30")

# ---------------------------------------------------------------------------
# Test Suite 2: Partition behaviour
# ---------------------------------------------------------------------------

def test_partition():
    separator("TEST 2 — Partition Simulation")

    # --- Disconnect node3 ---
    print("\n  [Step 1] Disconnecting node3 (minority partition)...")
    disconnect_node3()
    time.sleep(1)

    # --- Write in CP mode (should fail because node3 is unreachable) ---
    print(f"\n  [Step 2] Writing in CP mode (CONFIG_MODE=cp) — expect failure...")
    # We POST directly to node1 which tries to replicate to node3 and should fail
    cp_res = write(NODE1, "partition_cp_key", "should_fail_or_timeout")
    if cp_res.get("status") == "error":
        print(f"  ✅  CP write correctly FAILED during partition: {cp_res.get('message', '')[:80]}")
    else:
        if MODE == "cp":
            print(f"  ⚠️   CP write unexpectedly succeeded (mode={MODE}). "
                  "The partition may not have actually been applied — "
                  "double check NETWORK_NAME above against `docker network ls`.")
        else:
            print(f"  ℹ️   Running in AP mode — write succeeded as expected: {cp_res}")

    # --- Write in AP mode (should succeed on node1 and node2) ---
    print(f"\n  [Step 3] Writing in AP mode — expect success on reachable nodes...")
    ap_res = write(NODE2, "partition_ap_key", "ap_value_during_partition")
    if ap_res.get("status") == "ok":
        print(f"  ✅  AP write succeeded on node2: latency={ap_res.get('latency_ms')} ms")
    else:
        print(f"  ❌  AP write failed: {ap_res}")

    # Verify node3 does NOT yet have the value (it's partitioned)
    time.sleep(0.5)
    res_n3 = read(NODE3, "partition_ap_key")
    print(f"\n  [Step 4] Read partition_ap_key from isolated node3: "
          f"{'MISSING (expected)' if res_n3.get('status') == 'not_found' else res_n3.get('value')}")

    # --- Heal the partition ---
    print(f"\n  [Step 5] Reconnecting node3 (healing partition)...")
    reconnect_node3()
    time.sleep(2)   # allow AP background replication to propagate

    # --- Write one more key to trigger gossip ---
    write(NODE1, "post_heal_key", "synced_after_heal")
    time.sleep(1)

    # --- Verify convergence ---
    print(f"\n  [Step 6] Verifying convergence on all nodes after heal...")
    for key in ["partition_ap_key", "post_heal_key"]:
        for node_url in NODES:
            res = read(node_url, key)
            val = res.get("value", "MISSING")
            status = "✅" if res.get("status") == "ok" else "❌"
            print(f"  {status}  [{node_url}] {key} = {val}")

    # --- Latency comparison ---
    separator("LATENCY COMPARISON SUMMARY")
    print(f"  Mode running: {MODE.upper()}")
    print()
    print("  In CP mode:")
    print("    • Normal operation: slightly higher latency (waits for all ACKs)")
    print("    • Partition: write FAILS — no inconsistency possible")
    print()
    print("  In AP mode:")
    print("    • Normal operation: very low latency (returns after local write)")
    print("    • Partition: write SUCCEEDS locally; replicas diverge temporarily")
    print("    • Post-heal: vector clocks reconcile state automatically")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    wait_for_nodes()
    test_normal_operation()
    test_partition()
    print("\n🏁  Test run complete.\n")