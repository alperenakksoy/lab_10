"""
demo_offline_sync.py — Automated Offline/Online Convergence Demo
=================================================================
Simulates the full Task 5 scenario end-to-end:

  Phase 0  Setup     — verify all 8 replicas are reachable
  Phase 1  Online    — add shared items on multiple clients, sync to Raft
  Phase 2  Partition — disconnect 3 clients (docker network disconnect)
                       or flip them to /go_offline (no Docker required)
  Phase 3  Offline   — each disconnected client makes independent local edits
  Phase 4  Reconnect — bring clients back online, trigger sync
  Phase 5  Check     — assert all 8 replicas converge to the same CRDT state

Usage
-----
  # Full demo with Docker network simulation:
  CLIENTS=http://h1:6000,http://h2:6000,...,http://h8:6000 \
  RAFT_PEERS=http://node1:5000,http://node2:5000,http://node3:5000 \
  python demo_offline_sync.py

  # Quick local demo (all replicas on localhost, different ports):
  python demo_offline_sync.py          # uses defaults below

  # Skip Docker network manipulation (pure HTTP offline flag):
  USE_DOCKER=0 python demo_offline_sync.py
"""

import json
import os
import subprocess
import time
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
_raw_clients = os.getenv(
    "CLIENTS",
    "http://localhost:6001,http://localhost:6002,http://localhost:6003,"
    "http://localhost:6004,http://localhost:6005,http://localhost:6006,"
    "http://localhost:6007,http://localhost:6008",
)
CLIENTS     = [c.strip() for c in _raw_clients.split(",") if c.strip()]
_raw_peers  = os.getenv("RAFT_PEERS",
              "http://localhost:5001,http://localhost:5002,http://localhost:5003")
RAFT_PEERS  = [p.strip() for p in _raw_peers.split(",") if p.strip()]
USE_DOCKER  = os.getenv("USE_DOCKER", "0") == "1"

# Which clients to disconnect in Phase 2 (indices into CLIENTS list)
OFFLINE_CLIENTS_IDX = [2, 3, 4]   # clients 3, 4, 5 go offline
ONLINE_CLIENTS_IDX  = [i for i in range(len(CLIENTS)) if i not in OFFLINE_CLIENTS_IDX]

# Docker network name (used only if USE_DOCKER=1)
DOCKER_NETWORK = os.getenv("DOCKER_NETWORK", "raft_raft-net")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get(url, path, timeout=2.0):
    try:
        r = requests.get(f"{url}{path}", timeout=timeout)
        return r.json()
    except Exception as e:
        return {"error": str(e)}

def _post(url, path, body=None, timeout=3.0):
    try:
        r = requests.post(f"{url}{path}",
                          json=body or {}, timeout=timeout)
        return r.json()
    except Exception as e:
        return {"error": str(e)}

def banner(title: str):
    print(f"\n{'='*62}")
    print(f"  {title}")
    print(f"{'='*62}")

def ok_fail(result: dict) -> str:
    if "error" in result:
        return f"  ✗  ERROR: {result['error']}"
    return "  ✓  OK" if result.get("ok") else f"  ✗  FAIL: {result}"

def _docker_offline(container: str):
    subprocess.run(
        ["docker", "network", "disconnect", DOCKER_NETWORK, container],
        capture_output=True,
    )

def _docker_online(container: str):
    subprocess.run(
        ["docker", "network", "connect", DOCKER_NETWORK, container],
        capture_output=True,
    )

def _container_for(url: str) -> str:
    """Heuristic: extract hostname as container name."""
    from urllib.parse import urlparse
    return urlparse(url).hostname or url

def go_offline(idx: int):
    url = CLIENTS[idx]
    if USE_DOCKER:
        _docker_offline(_container_for(url))
        print(f"  [docker] disconnected {_container_for(url)}")
    else:
        r = _post(url, "/go_offline")
        print(f"  {url}  →  offline  {ok_fail(r)}")

def go_online(idx: int):
    url = CLIENTS[idx]
    if USE_DOCKER:
        _docker_online(_container_for(url))
        time.sleep(0.3)   # brief settle
        r = _post(url, "/sync")
        print(f"  {url}  →  online + sync  {ok_fail(r)}")
    else:
        r = _post(url, "/go_online")
        print(f"  {url}  →  online  {ok_fail(r)}")

# ---------------------------------------------------------------------------
# Phase implementations
# ---------------------------------------------------------------------------

def phase0_check_reachability():
    banner("Phase 0 — Reachability Check")
    all_ok = True
    for i, url in enumerate(CLIENTS):
        d = _get(url, "/status")
        if "error" in d:
            print(f"  Client {i+1} {url:35s}  ✗  UNREACHABLE: {d['error']}")
            all_ok = False
        else:
            print(f"  Client {i+1} {url:35s}  ✓  replica={d.get('replica_id')!r}")
    if not all_ok:
        print("\n  [warn] Some clients unreachable. Demo may produce partial results.\n")
    return all_ok


def phase1_online_writes():
    banner("Phase 1 — Online writes (all clients)")
    items_per_client = [
        ["milk", "eggs", "bread"],
        ["butter", "cheese"],
        ["apples", "bananas", "milk"],   # 'milk' added by two clients → OR-Set handles it
        ["pasta", "tomatoes"],
        ["coffee", "tea"],
        ["sugar", "flour"],
        ["chicken", "beef"],
        ["olive oil", "salt"],
    ]

    for i, url in enumerate(CLIENTS):
        items = items_per_client[i % len(items_per_client)]
        for item in items:
            r = _post(url, "/local/add", {"item": item})
            if r.get("ok"):
                _post(url, "/local/inc", {"item": item, "amount": (i % 3) + 1})
        # Sync to Raft
        r = _post(url, "/sync")
        print(f"  Client {i+1}  added {items}  sync: {ok_fail(r)}")
        time.sleep(0.1)

    print("\n  Waiting 1 s for Raft replication …")
    time.sleep(1.0)


def phase2_partition():
    banner(f"Phase 2 — Disconnect clients {[i+1 for i in OFFLINE_CLIENTS_IDX]}")
    for idx in OFFLINE_CLIENTS_IDX:
        go_offline(idx)
    print(f"\n  {len(OFFLINE_CLIENTS_IDX)} clients now isolated from the cluster.")


def phase3_offline_edits():
    banner("Phase 3 — Offline edits (on disconnected clients)")
    offline_ops = [
        [("local/add",  {"item": "yogurt"}),
         ("local/inc",  {"item": "yogurt", "amount": 2}),
         ("local/add",  {"item": "juice"}),
         ("local/remove", {"item": "eggs"})],    # remove something added earlier

        [("local/add",  {"item": "cake"}),
         ("local/inc",  {"item": "cake", "amount": 1}),
         ("local/add",  {"item": "cookies"})],

        [("local/add",  {"item": "rice"}),
         ("local/inc",  {"item": "rice", "amount": 3}),
         ("local/add",  {"item": "noodles"}),
         ("local/inc",  {"item": "milk", "amount": 5})],  # concurrent inc of shared item
    ]

    for pos, idx in enumerate(OFFLINE_CLIENTS_IDX):
        url = CLIENTS[idx]
        ops = offline_ops[pos % len(offline_ops)]
        print(f"\n  Client {idx+1} (offline) — {len(ops)} local operations:")
        for endpoint, body in ops:
            r = _post(url, f"/{endpoint}", body)
            print(f"    POST /{endpoint} {body}  →  {ok_fail(r)}")

    print("\n  Attempting sync from offline client (should fail):")
    url = CLIENTS[OFFLINE_CLIENTS_IDX[0]]
    r = _post(url, "/sync")
    print(f"  {ok_fail(r)}  (expected failure — node is offline)")


def phase4_reconnect():
    banner("Phase 4 — Reconnect and sync")
    for idx in OFFLINE_CLIENTS_IDX:
        go_online(idx)
        time.sleep(0.3)

    print("\n  Waiting 2 s for Raft to replicate merged state …")
    time.sleep(2.0)

    # Trigger a sync on all online clients too so they pull merged state
    for _ in range(2):
        for idx in range(len(CLIENTS)):
            _post(CLIENTS[idx], "/sync")
        time.sleep(1.0)


def phase5_convergence_check():
    banner("Phase 5 — Convergence Check")

    states = []
    for i, url in enumerate(CLIENTS):
        d = _get(url, "/local/state")
        if "error" not in d:
            states.append({"idx": i+1, "url": url,
                           "items": sorted(d.get("items", [])),
                           "quantities": d.get("quantities", {})})
            print(f"  Client {i+1:2d}  items={sorted(d.get('items', []))}")
        else:
            print(f"  Client {i+1:2d}  UNREACHABLE")

    if len(states) < 2:
        print("\n  Not enough reachable clients to check convergence.")
        return False

    # Compare all item sets
    all_item_sets = [frozenset(s["items"]) for s in states]
    converged_items = len(set(all_item_sets)) == 1

    # Compare quantities for shared items
    all_qtys = [s["quantities"] for s in states]
    shared_items = set.intersection(*[set(q.keys()) for q in all_qtys]) if all_qtys else set()
    converged_qtys = all(
        all_qtys[0].get(item) == q.get(item)
        for item in shared_items
        for q in all_qtys[1:]
    )

    print(f"\n  Item sets converged   : {'✓ YES' if converged_items else '✗ NO'}")
    print(f"  Quantities converged  : {'✓ YES' if converged_qtys else '✗ NO (shared items)'}")

    if converged_items:
        ref = states[0]["items"]
        print(f"\n  Final converged list  : {ref}")
    else:
        print("\n  Divergent states detected:")
        for s in states:
            print(f"    Client {s['idx']}: {s['items']}")

    return converged_items


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("\n" + "█"*62)
    print("  Task 5 — Offline-Capable CRDT + Raft Convergence Demo")
    print("  HSRW Mobile & Internet Computing — Week 10")
    print("█"*62)
    print(f"\n  Clients   : {len(CLIENTS)}")
    print(f"  Raft peers: {len(RAFT_PEERS)}")
    print(f"  Mode      : {'Docker network disconnect' if USE_DOCKER else 'HTTP offline flag'}")

    t_start = time.monotonic()

    phase0_check_reachability()
    phase1_online_writes()
    phase2_partition()
    phase3_offline_edits()
    phase4_reconnect()
    converged = phase5_convergence_check()

    elapsed = time.monotonic() - t_start
    banner(f"Demo complete in {elapsed:.1f} s  —  "
           f"{'CONVERGED ✓' if converged else 'NOT FULLY CONVERGED ✗'}")

if __name__ == "__main__":
    main()
