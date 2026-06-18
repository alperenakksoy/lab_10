"""
app.py — Flask HTTP API for a Raft node
========================================
Exposes all Raft RPCs and KV endpoints.  One process per node.

Environment variables
---------------------
  NODE_ID   : unique ID for this node, e.g. "node1"  (default: "node1")
  PEERS     : comma-separated peer base-URLs,
              e.g. "http://node2:5000,http://node3:5000"
  PORT      : TCP port to listen on (default: 5000)

Run (example for a 3-node local cluster):
  NODE_ID=node1 PEERS=http://localhost:5002,http://localhost:5003 PORT=5001 python app.py
  NODE_ID=node2 PEERS=http://localhost:5001,http://localhost:5003 PORT=5002 python app.py
  NODE_ID=node3 PEERS=http://localhost:5001,http://localhost:5002 PORT=5003 python app.py
"""

import os
from flask import Flask, request, jsonify
from raft import RaftNode

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
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(f"Starting Raft node {NODE_ID!r} on port {PORT}")
    print(f"Peers: {PEERS}")
    app.run(host="0.0.0.0", port=PORT, debug=False)
