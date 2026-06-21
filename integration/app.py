"""
app.py — Flask HTTP API for a Raft node (Task 3/4 + Task 5 CRDT sync)
======================================================================
Exposes all Raft RPCs, KV endpoints, AND the CRDT sync endpoints used by
offline-capable clients (offline_app.py) in Task 5.

Environment variables
---------------------
  NODE_ID   : unique ID for this node, e.g. "node1"  (default: "node1")
  PEERS     : comma-separated peer base-URLs,
              e.g. "http://node2:5000,http://node3:5000"
  PORT      : TCP port to listen on (default: 5000)
"""

import os
from flask import Flask, request, jsonify
from raft import RaftNode
from crdt_or_set import ORSet
from crdt_pn_counter import PNCounter

# ---------------------------------------------------------------------------
# Bootstrap the Raft node from environment
# ---------------------------------------------------------------------------
NODE_ID = os.getenv("NODE_ID", "node1")
_raw    = os.getenv("PEERS", "")
PEERS   = [p.strip() for p in _raw.split(",") if p.strip()]
PORT    = int(os.getenv("PORT", "5000"))

node = RaftNode(node_id=NODE_ID, peers=PEERS)
node.start()

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Raft RPC endpoints (called by other Raft nodes)
# ---------------------------------------------------------------------------

@app.post("/raft/request_vote")
def request_vote():
    return jsonify(node.handle_request_vote(request.json))

@app.post("/raft/append_entries")
def append_entries():
    return jsonify(node.handle_append_entries(request.json))

# ---------------------------------------------------------------------------
# Status (polled by dashboard and clients)
# ---------------------------------------------------------------------------

@app.get("/raft/status")
def status():
    return jsonify(node.status())

# ---------------------------------------------------------------------------
# Raft proposal endpoint (used directly by clients)
# ---------------------------------------------------------------------------

@app.post("/raft/propose")
def propose():
    cmd = request.json
    return jsonify(node.propose(cmd))

# ---------------------------------------------------------------------------
# KV store endpoints (client-facing)
# ---------------------------------------------------------------------------

@app.post("/kv/write")
def kv_write():
    body  = request.json or {}
    key   = body.get("key")
    value = body.get("value")
    if not key:
        return jsonify({"ok": False, "reason": "missing_key"}), 400
    result = node.propose({"op": "set", "key": key, "value": value})
    return jsonify(result)

@app.get("/kv/<key>")
def kv_read(key: str):
    return jsonify(node.kv_read(key))

@app.post("/kv/<key>")
def kv_write_path(key: str):
    body  = request.json or {}
    value = body.get("value", "")
    result = node.propose({"op": "set", "key": key, "value": value})
    return jsonify(result)

# ---------------------------------------------------------------------------
# Task 5: CRDT sync endpoints
# ---------------------------------------------------------------------------
# The Raft cluster holds an authoritative merged copy of the shared CRDT
# state (shopping list items + per-item quantities). Offline-capable clients
# (offline_app.py) push their local state here on /crdt/sync and pull the
# merged result; they also poll /crdt/state on reconnect.
#
# This merged state is replicated to all 3 Raft nodes independently (each
# node merges whatever it receives), rather than going through the Raft log.
# This is intentional for Task 5: CRDT merges don't need ordering/consensus,
# only the underlying KV writes (Task 3/4) need Raft's log replication.

_merged_items: ORSet | None = None
_merged_qtys: dict[str, PNCounter] = {}

@app.post("/crdt/sync")
def crdt_sync():
    """Receive a client's local CRDT state, merge it into this node's
    authoritative copy, and return the merged result."""
    global _merged_items
    body = request.json or {}

    if "items_state" in body:
        incoming = ORSet.from_json(body["items_state"])
        if _merged_items is None:
            _merged_items = ORSet("raft-cluster")
        _merged_items.merge(incoming)

    for item_name, qty_json in body.get("qtys_state", {}).items():
        incoming_qty = PNCounter.from_json(qty_json)
        if item_name not in _merged_qtys:
            _merged_qtys[item_name] = PNCounter("raft-cluster")
        _merged_qtys[item_name].merge(incoming_qty)

    merged = {
        "items_state": _merged_items.to_json() if _merged_items else ORSet("raft-cluster").to_json(),
        "qtys_state":  {k: v.to_json() for k, v in _merged_qtys.items()},
    }
    return jsonify({"ok": True, "merged_state": merged})

@app.get("/crdt/state")
def crdt_state():
    """Return the current merged CRDT state (clients pull this on reconnect)."""
    if _merged_items is None:
        return jsonify({"items": [], "quantities": {}})
    return jsonify({
        "items_state": _merged_items.to_json(),
        "qtys_state":  {k: v.to_json() for k, v in _merged_qtys.items()},
        "items":       sorted(_merged_items.value()),
        "quantities":  {k: v.value() for k, v in _merged_qtys.items()},
    })

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(f"Starting Raft node {NODE_ID!r} on port {PORT}")
    print(f"Peers: {PEERS}")
    app.run(host="0.0.0.0", port=PORT, debug=False)