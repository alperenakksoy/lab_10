"""
kv_store/app.py — Replicated Key-Value Store (CP / AP modes)
=============================================================

Consistency modes (set via CONFIG_MODE env var or config.yaml):
  CP (Strong Consistency):
    - Writes must be acknowledged by ALL replicas before returning.
    - During a partition, writes to isolated minority nodes fail.
  AP (Eventual Consistency):
    - Writes succeed after local acknowledgement only.
    - Replicas diverge during a partition and re-converge on heal
      using vector clocks to resolve conflicts (last-write-wins per key).

Each replica runs this same Flask app.  Replica identity and peer list
come from environment variables injected by Docker Compose.
"""

import json
import os
import time
import threading
import requests
import yaml
from flask import Flask, request, jsonify

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

app = Flask(__name__)

NODE_ID   = os.environ.get("NODE_ID", "node1")           # e.g. "node1"
NODE_PORT = int(os.environ.get("NODE_PORT", 5000))

# Comma-separated list of peer base URLs, e.g. "http://node2:5000,http://node3:5000"
PEERS_ENV = os.environ.get("PEERS", "")
PEERS     = [p.strip() for p in PEERS_ENV.split(",") if p.strip()]

# Load consistency mode from YAML config (overridable by env var)
CONFIG_FILE = os.environ.get("CONFIG_FILE", "/app/config.yaml")
MODE = os.environ.get("CONFIG_MODE", "ap")   # default fallback
if os.path.exists(CONFIG_FILE):
    with open(CONFIG_FILE) as f:
        cfg = yaml.safe_load(f) or {}
        MODE = cfg.get("mode", MODE)
MODE = os.environ.get("CONFIG_MODE", MODE)   # env var wins over file

REPLICATION_TIMEOUT = float(os.environ.get("REPLICATION_TIMEOUT", "1.0"))  # seconds

# ---------------------------------------------------------------------------
# In-memory store
# ---------------------------------------------------------------------------
# Each entry: { "value": <any>, "vector_clock": { node_id: int, ... }, "timestamp": float }

store: dict[str, dict] = {}
store_lock = threading.Lock()

def _my_clock(vc: dict) -> int:
    return vc.get(NODE_ID, 0)

def _increment_clock(vc: dict) -> dict:
    new_vc = dict(vc)
    new_vc[NODE_ID] = new_vc.get(NODE_ID, 0) + 1
    return new_vc

def _merge_clocks(vc1: dict, vc2: dict) -> dict:
    all_keys = set(vc1) | set(vc2)
    return {k: max(vc1.get(k, 0), vc2.get(k, 0)) for k in all_keys}

def _dominates(vc1: dict, vc2: dict) -> bool:
    """Return True if vc1 >= vc2 component-wise (vc1 is at least as recent)."""
    all_keys = set(vc1) | set(vc2)
    return all(vc1.get(k, 0) >= vc2.get(k, 0) for k in all_keys)

# ---------------------------------------------------------------------------
# Internal replication endpoint (called node-to-node)
# ---------------------------------------------------------------------------

@app.route("/internal/replicate", methods=["POST"])
def internal_replicate():
    """
    Receive a replicated write from the primary node.
    Apply it only if the incoming vector clock is not dominated by our local one
    (i.e. it carries new information).
    """
    data = request.get_json(force=True)
    key  = data["key"]
    val  = data["value"]
    vc   = data["vector_clock"]
    ts   = data.get("timestamp", time.time())

    with store_lock:
        existing = store.get(key)
        if existing is None or not _dominates(existing["vector_clock"], vc):
            # Remote state is newer (or concurrent) — merge clocks, keep remote value
            merged_vc = _merge_clocks(existing["vector_clock"] if existing else {}, vc)
            store[key] = {"value": val, "vector_clock": merged_vc, "timestamp": ts}
            return jsonify({"status": "ok", "applied": True}), 200
        else:
            # Our local state is strictly newer; discard
            return jsonify({"status": "ok", "applied": False}), 200

# ---------------------------------------------------------------------------
# Public API: POST /kv/write
# ---------------------------------------------------------------------------

@app.route("/kv/write", methods=["POST"])
def kv_write():
    """
    Write a key-value pair.

    Body (JSON): { "key": str, "value": any }

    CP mode: replicate to ALL peers synchronously; fail if any peer is unreachable.
    AP mode: replicate to ALL peers asynchronously (fire-and-forget); always succeed locally.
    """
    body = request.get_json(force=True)
    key  = body.get("key")
    val  = body.get("value")
    if key is None:
        return jsonify({"error": "missing key"}), 400

    t_start = time.time()

    # Build new vector clock
    with store_lock:
        existing = store.get(key)
        old_vc   = existing["vector_clock"] if existing else {}
        new_vc   = _increment_clock(old_vc)
        ts       = time.time()
        store[key] = {"value": val, "vector_clock": new_vc, "timestamp": ts}

    replication_payload = {"key": key, "value": val, "vector_clock": new_vc, "timestamp": ts}

    if MODE == "cp":
        # ---- CP: synchronous replication to all peers ----
        failed_peers = []
        for peer in PEERS:
            try:
                r = requests.post(
                    f"{peer}/internal/replicate",
                    json=replication_payload,
                    timeout=REPLICATION_TIMEOUT,
                )
                if r.status_code != 200:
                    failed_peers.append(peer)
            except requests.exceptions.RequestException as e:
                failed_peers.append(peer)

        if failed_peers:
            # Roll back local write so we don't have inconsistent state
            with store_lock:
                if existing:
                    store[key] = existing
                else:
                    del store[key]
            latency_ms = (time.time() - t_start) * 1000
            return jsonify({
                "status":      "error",
                "mode":        "cp",
                "message":     f"Replication failed to {failed_peers}. Write aborted (CP).",
                "latency_ms":  round(latency_ms, 2),
            }), 503

        latency_ms = (time.time() - t_start) * 1000
        return jsonify({
            "status":     "ok",
            "mode":       "cp",
            "key":        key,
            "value":      val,
            "peers_acked": len(PEERS),
            "latency_ms": round(latency_ms, 2),
        }), 200

    else:
        # ---- AP: async replication (fire-and-forget) ----
        def _async_replicate():
            for peer in PEERS:
                try:
                    requests.post(
                        f"{peer}/internal/replicate",
                        json=replication_payload,
                        timeout=REPLICATION_TIMEOUT,
                    )
                except Exception:
                    pass   # AP: ignore failures

        threading.Thread(target=_async_replicate, daemon=True).start()

        latency_ms = (time.time() - t_start) * 1000
        return jsonify({
            "status":     "ok",
            "mode":       "ap",
            "key":        key,
            "value":      val,
            "peers_acked": 0,  # async — not waited
            "latency_ms": round(latency_ms, 2),
        }), 200

# ---------------------------------------------------------------------------
# Public API: GET /kv/read?key=<key>
# ---------------------------------------------------------------------------

@app.route("/kv/read", methods=["GET"])
def kv_read():
    """
    Read a value by key from this replica's local store.
    In AP mode the value may be stale if a partition has occurred.
    """
    key = request.args.get("key")
    if not key:
        return jsonify({"error": "missing key param"}), 400

    with store_lock:
        entry = store.get(key)

    if entry is None:
        return jsonify({"status": "not_found", "key": key}), 404

    return jsonify({
        "status":       "ok",
        "key":          key,
        "value":        entry["value"],
        "vector_clock": entry["vector_clock"],
        "node_id":      NODE_ID,
        "mode":         MODE,
    }), 200

# ---------------------------------------------------------------------------
# Debug / status
# ---------------------------------------------------------------------------

@app.route("/status", methods=["GET"])
def status():
    with store_lock:
        store_snapshot = {k: v["value"] for k, v in store.items()}
    return jsonify({
        "node_id":    NODE_ID,
        "mode":       MODE,
        "peers":      PEERS,
        "store_size": len(store),
        "store":      store_snapshot,
    }), 200

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"[{NODE_ID}] Starting in {MODE.upper()} mode on port {NODE_PORT}")
    print(f"[{NODE_ID}] Peers: {PEERS}")
    app.run(host="0.0.0.0", port=NODE_PORT, threaded=True)
