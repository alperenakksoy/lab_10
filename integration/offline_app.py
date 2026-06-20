"""
offline_app.py — Offline-Capable Collaborative Shopping List
=============================================================
Architecture
------------
  Local layer  : OR-Set CRDT (add/remove items) + PN-Counter (item quantities)
                 All operations work immediately, no network required.
  Sync layer   : When online, POST /sync sends the full local CRDT state to
                 the Raft cluster as a single ordered transaction.
                 On success, the Raft cluster broadcasts merged state back via
                 GET /crdt/state which all clients poll periodically.

HTTP Endpoints (this node)
--------------------------
  GET  /            Web UI
  POST /local/add   Add item to local OR-Set
  POST /local/remove Remove item from local OR-Set
  POST /local/inc   Increment quantity of item (PN-Counter)
  POST /local/dec   Decrement quantity of item
  GET  /local/state Current local CRDT state (JSON)
  POST /sync        Push local state to Raft cluster + pull merged state
  GET  /status      Online/offline flag + sync metadata

Environment
-----------
  REPLICA_ID   : unique string for this node, e.g. "client-3" (default: hostname)
  RAFT_PEERS   : comma-separated Raft node URLs
  PORT         : local Flask port (default: 6000)
  OFFLINE      : set to "1" to start in offline mode (blocks sync)
"""

import json
import os
import socket
import threading
import time
import requests
from flask import Flask, request, jsonify, render_template_string

# Local CRDT imports
from crdt_layer.crdt_or_set import ORSet
from crdt_layer.crdt_pn_counter import PNCounter

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
REPLICA_ID  = os.getenv("REPLICA_ID", socket.gethostname())
_raw_peers  = os.getenv("RAFT_PEERS",
              "http://localhost:5001,http://localhost:5002,http://localhost:5003")
RAFT_PEERS  = [p.strip() for p in _raw_peers.split(",") if p.strip()]
PORT        = int(os.getenv("PORT", "6000"))
OFFLINE     = os.getenv("OFFLINE", "0") == "1"
SYNC_INTERVAL = float(os.getenv("SYNC_INTERVAL_S", "5.0"))  # auto-sync every N seconds

# ---------------------------------------------------------------------------
# Local CRDT state
# ---------------------------------------------------------------------------
# items   : OR-Set  — which items exist in the list
# qtys    : PN-Counter per item — quantity of each item
_lock  = threading.Lock()
items  = ORSet(REPLICA_ID)
qtys   : dict[str, PNCounter] = {}   # keyed by item name

# Sync metadata
last_sync_time: float | None = None
sync_count    : int = 0
is_offline    : bool = OFFLINE

# ---------------------------------------------------------------------------
# Helper: get or create a per-item PNCounter
# ---------------------------------------------------------------------------

def _get_qty(item: str) -> PNCounter:
    if item not in qtys:
        qtys[item] = PNCounter(REPLICA_ID)
    return qtys[item]

# ---------------------------------------------------------------------------
# Raft communication helpers
# ---------------------------------------------------------------------------

def _find_raft_leader() -> str | None:
    """Return the URL of the current Raft leader, or None if unreachable."""
    for url in RAFT_PEERS:
        try:
            r = requests.get(f"{url}/raft/status", timeout=1.0)
            data = r.json()
            if data.get("state") == "leader":
                return url
        except Exception:
            pass
    # No node identified itself as leader; try any reachable node
    for url in RAFT_PEERS:
        try:
            requests.get(f"{url}/raft/status", timeout=0.5)
            return url
        except Exception:
            pass
    return None


def _push_to_raft(payload: dict) -> dict | None:
    """
    Submit the CRDT sync payload to the Raft cluster via /crdt/sync.
    Tries each peer until one accepts (handles leader rotation automatically).
    Returns the merged state from the cluster, or None on failure.
    """
    for url in RAFT_PEERS:
        try:
            r = requests.post(f"{url}/crdt/sync", json=payload, timeout=2.0)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
    return None


def _pull_from_raft() -> dict | None:
    """Fetch the latest merged CRDT state from any reachable Raft node."""
    for url in RAFT_PEERS:
        try:
            r = requests.get(f"{url}/crdt/state", timeout=1.0)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
    return None

# ---------------------------------------------------------------------------
# Sync logic
# ---------------------------------------------------------------------------

def _do_sync() -> dict:
    """
    1. Serialize local CRDT state.
    2. Push to Raft cluster as a single ordered transaction.
    3. Pull the merged state broadcast by Raft.
    4. Merge remote state into local CRDTs.
    Returns a status dict.
    """
    global last_sync_time, sync_count

    with _lock:
        local_payload = {
            "replica_id": REPLICA_ID,
            "items_state": items.to_json(),
            "qtys_state":  {k: v.to_json() for k, v in qtys.items()},
            "timestamp":   time.time(),
        }

    result = _push_to_raft(local_payload)
    if result is None:
        return {"ok": False, "reason": "raft_unreachable"}

    # Merge remote state back in
    remote = result.get("merged_state", {})
    if remote:
        _merge_remote_state(remote)

    with _lock:
        last_sync_time = time.time()
        sync_count    += 1

    return {"ok": True, "sync_count": sync_count}


def _merge_remote_state(remote: dict):
    """Merge a remote CRDT state dict into our local CRDTs (under lock)."""
    with _lock:
        # Merge OR-Set
        if "items_state" in remote:
            try:
                remote_items = ORSet.from_json(remote["items_state"])
                items.merge(remote_items)
            except Exception as e:
                print(f"  [sync] items merge error: {e}")

        # Merge per-item PN-Counters
        for item_name, qty_json in remote.get("qtys_state", {}).items():
            try:
                remote_qty = PNCounter.from_json(qty_json)
                _get_qty(item_name).merge(remote_qty)
            except Exception as e:
                print(f"  [sync] qty merge error for {item_name!r}: {e}")

# ---------------------------------------------------------------------------
# Background auto-sync thread
# ---------------------------------------------------------------------------

def _auto_sync_loop():
    while True:
        time.sleep(SYNC_INTERVAL)
        if not is_offline:
            result = _do_sync()
            if result["ok"]:
                print(f"  [auto-sync] ok  (sync #{result['sync_count']})")
            else:
                print(f"  [auto-sync] failed: {result.get('reason')}")

threading.Thread(target=_auto_sync_loop, daemon=True, name="auto-sync").start()

# ---------------------------------------------------------------------------
# Flask application
# ---------------------------------------------------------------------------
app = Flask(__name__)

@app.post("/local/add")
def local_add():
    item = (request.json or {}).get("item", "").strip()
    if not item:
        return jsonify({"ok": False, "reason": "missing item"}), 400
    with _lock:
        items.add(item)
        _get_qty(item)  # ensure qty entry exists
    return jsonify({"ok": True, "item": item, "list": sorted(items.value())})

@app.post("/local/remove")
def local_remove():
    item = (request.json or {}).get("item", "").strip()
    if not item:
        return jsonify({"ok": False, "reason": "missing item"}), 400
    with _lock:
        items.remove(item)
    return jsonify({"ok": True, "item": item, "list": sorted(items.value())})

@app.post("/local/inc")
def local_inc():
    body   = request.json or {}
    item   = body.get("item", "").strip()
    amount = int(body.get("amount", 1))
    if not item:
        return jsonify({"ok": False, "reason": "missing item"}), 400
    with _lock:
        items.add(item)
        _get_qty(item).increment(amount)
    return jsonify({"ok": True, "item": item,
                    "qty": _get_qty(item).value()})

@app.post("/local/dec")
def local_dec():
    body   = request.json or {}
    item   = body.get("item", "").strip()
    amount = int(body.get("amount", 1))
    if not item:
        return jsonify({"ok": False, "reason": "missing item"}), 400
    with _lock:
        _get_qty(item).decrement(amount)
    return jsonify({"ok": True, "item": item,
                    "qty": _get_qty(item).value()})

@app.get("/local/state")
def local_state():
    with _lock:
        item_list = sorted(items.value())
        qty_map   = {k: v.value() for k, v in qtys.items()
                     if k in items.value()}
    return jsonify({
        "replica_id": REPLICA_ID,
        "items":      item_list,
        "quantities": qty_map,
        "offline":    is_offline,
        "last_sync":  last_sync_time,
        "sync_count": sync_count,
    })

@app.post("/sync")
def manual_sync():
    if is_offline:
        return jsonify({"ok": False, "reason": "node is offline"}), 503
    return jsonify(_do_sync())

@app.post("/go_offline")
def go_offline():
    global is_offline
    is_offline = True
    return jsonify({"ok": True, "offline": True})

@app.post("/go_online")
def go_online():
    global is_offline
    is_offline = False
    # Immediately sync on reconnect
    result = _do_sync()
    return jsonify({"ok": True, "offline": False, "sync": result})

@app.get("/status")
def status():
    with _lock:
        item_count = len(items.value())
    return jsonify({
        "replica_id":  REPLICA_ID,
        "offline":     is_offline,
        "item_count":  item_count,
        "sync_count":  sync_count,
        "last_sync":   last_sync_time,
        "raft_peers":  RAFT_PEERS,
    })

# ---------------------------------------------------------------------------
# Raft-side endpoint: receive CRDT syncs from clients
# (In production these live on the Raft nodes.  For a standalone demo they
#  can run here or be proxied through app.py.)
# ---------------------------------------------------------------------------

# Shared merged state (Raft nodes maintain this; here for standalone mode)
_raft_merged_items: ORSet     | None = None
_raft_merged_qtys : dict[str, PNCounter] = {}
_raft_lock = threading.Lock()

@app.post("/crdt/sync")
def raft_crdt_sync():
    """
    Raft node endpoint: receive a client CRDT state, merge it into the
    cluster's authoritative copy, then return the merged state.
    """
    global _raft_merged_items
    body = request.json or {}

    with _raft_lock:
        # Merge incoming items OR-Set
        if "items_state" in body:
            incoming = ORSet.from_json(body["items_state"])
            if _raft_merged_items is None:
                _raft_merged_items = ORSet("raft-cluster")
            _raft_merged_items.merge(incoming)

        # Merge per-item PN-Counters
        for item_name, qty_json in body.get("qtys_state", {}).items():
            incoming_qty = PNCounter.from_json(qty_json)
            if item_name not in _raft_merged_qtys:
                _raft_merged_qtys[item_name] = PNCounter("raft-cluster")
            _raft_merged_qtys[item_name].merge(incoming_qty)

        merged = {
            "items_state": _raft_merged_items.to_json() if _raft_merged_items else ORSet("raft-cluster").to_json(),
            "qtys_state":  {k: v.to_json() for k, v in _raft_merged_qtys.items()},
        }

    return jsonify({"ok": True, "merged_state": merged})

@app.get("/crdt/state")
def raft_crdt_state():
    """Return the current merged CRDT state (clients pull on reconnect)."""
    with _raft_lock:
        if _raft_merged_items is None:
            return jsonify({"items": [], "quantities": {}})
        merged = {
            "items_state": _raft_merged_items.to_json(),
            "qtys_state":  {k: v.to_json() for k, v in _raft_merged_qtys.items()},
            "items":       sorted(_raft_merged_items.value()),
            "quantities":  {k: v.value() for k, v in _raft_merged_qtys.items()},
        }
    return jsonify(merged)

# ---------------------------------------------------------------------------
# Simple web UI
# ---------------------------------------------------------------------------
UI_TEMPLATE = """
<!DOCTYPE html><html><head>
<meta charset="utf-8">
<title>Offline Shopping List — {{ replica_id }}</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root {
    --bg: #0d1117; --surface: #161b22; --border: #30363d;
    --text: #e6edf3; --muted: #8b949e; --green: #3fb950;
    --red: #f85149; --yellow: #d29922; --blue: #58a6ff;
    --font-mono: 'JetBrains Mono', 'Fira Code', monospace;
    --font-ui: -apple-system, 'Segoe UI', sans-serif;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: var(--font-ui);
         min-height: 100vh; padding: 2rem; }

  header { display: flex; align-items: center; gap: 1rem; margin-bottom: 2rem; }
  header h1 { font-size: 1.4rem; font-weight: 600; }
  .badge { font-size: .72rem; padding: 3px 10px; border-radius: 20px;
           font-family: var(--font-mono); font-weight: 600; letter-spacing: .04em; }
  .badge-online  { background: #0d2818; color: var(--green); border: 1px solid var(--green); }
  .badge-offline { background: #2d0f0f; color: var(--red);   border: 1px solid var(--red); }
  .badge-id      { background: var(--surface); color: var(--muted); border: 1px solid var(--border); }

  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 1.5rem; max-width: 960px; }
  @media (max-width: 600px) { .grid { grid-template-columns: 1fr; } }

  .card { background: var(--surface); border: 1px solid var(--border);
          border-radius: 8px; padding: 1.2rem; }
  .card h2 { font-size: .8rem; text-transform: uppercase; letter-spacing: .1em;
             color: var(--muted); margin-bottom: 1rem; }

  .item-row { display: flex; align-items: center; justify-content: space-between;
              padding: .45rem .6rem; border-radius: 5px; margin-bottom: .35rem;
              background: #0d1117; border: 1px solid var(--border); }
  .item-name { font-family: var(--font-mono); font-size: .9rem; }
  .qty-badge { background: #1c2a3a; color: var(--blue); border-radius: 4px;
               padding: 2px 8px; font-family: var(--font-mono); font-size: .8rem; }
  .btn-row   { display: flex; gap: .4rem; }
  button { cursor: pointer; border: none; border-radius: 5px; padding: 5px 12px;
           font-size: .82rem; font-weight: 600; transition: opacity .15s; }
  button:hover { opacity: .8; }
  .btn-add  { background: #0d2818; color: var(--green); border: 1px solid var(--green); }
  .btn-rm   { background: #2d0f0f; color: var(--red);   border: 1px solid var(--red); }
  .btn-inc  { background: #1c2a3a; color: var(--blue);  border: 1px solid var(--blue); }
  .btn-dec  { background: #21201a; color: var(--yellow); border: 1px solid var(--yellow); }
  .btn-sync { background: #1c2a3a; color: var(--blue);  border: 1px solid var(--blue);
              padding: 6px 18px; }
  .btn-offline { background: #2d0f0f; color: var(--red);
                 border: 1px solid var(--red); padding: 6px 18px; }
  .btn-online  { background: #0d2818; color: var(--green);
                 border: 1px solid var(--green); padding: 6px 18px; }

  .add-row { display: flex; gap: .5rem; margin-top: .8rem; }
  .add-row input { flex: 1; background: #0d1117; border: 1px solid var(--border);
                   color: var(--text); padding: .45rem .7rem; border-radius: 5px;
                   font-family: var(--font-mono); font-size: .9rem; }

  .log { font-family: var(--font-mono); font-size: .78rem; color: var(--muted);
         max-height: 180px; overflow-y: auto; padding: .5rem; background: #0d1117;
         border: 1px solid var(--border); border-radius: 5px; }
  .log-entry { padding: 2px 0; border-bottom: 1px solid #1a1f27; }
  .log-ok  { color: var(--green); }
  .log-err { color: var(--red); }
  .log-info{ color: var(--blue); }

  .meta { font-size: .78rem; color: var(--muted); font-family: var(--font-mono);
          margin-top: .6rem; }
  .ctrl-row { display: flex; gap: .6rem; align-items: center; flex-wrap: wrap;
              margin-bottom: 1rem; }
  .empty { color: var(--muted); font-size: .85rem; padding: .6rem 0; }
</style>
</head><body>

<header>
  <h1>🛒 Collaborative Shopping List</h1>
  <span class="badge badge-id">{{ replica_id }}</span>
  <span class="badge" id="online-badge">…</span>
</header>

<div class="grid">
  <div class="card">
    <h2>Local List</h2>
    <div id="item-list"><span class="empty">No items yet.</span></div>
    <div class="add-row">
      <input id="new-item" type="text" placeholder="Add item…">
      <button class="btn-add" onclick="addItem()">+ Add</button>
    </div>
  </div>

  <div class="card">
    <h2>Sync Controls</h2>
    <div class="ctrl-row">
      <button class="btn-sync"    onclick="doSync()">↑ Sync Now</button>
      <button class="btn-offline" onclick="goOffline()">✕ Go Offline</button>
      <button class="btn-online"  onclick="goOnline()">✓ Go Online</button>
    </div>
    <div class="meta" id="meta">Loading…</div>

    <h2 style="margin-top:1.2rem">Activity Log</h2>
    <div class="log" id="log"></div>
  </div>
</div>

<script>
const replica = {{ replica_id|tojson }};
let logEntries = [];

function addLog(msg, cls='log-info') {
  const t = new Date().toLocaleTimeString();
  logEntries.unshift({t, msg, cls});
  if (logEntries.length > 60) logEntries.pop();
  renderLog();
}

function renderLog() {
  const el = document.getElementById('log');
  el.innerHTML = logEntries.map(e =>
    `<div class="log-entry ${e.cls}">[${e.t}] ${e.msg}</div>`
  ).join('');
}

async function api(method, path, body) {
  try {
    const opts = { method, headers: {'Content-Type':'application/json'} };
    if (body) opts.body = JSON.stringify(body);
    const r = await fetch(path, opts);
    return await r.json();
  } catch(e) { return {ok: false, error: String(e)}; }
}

async function refreshState() {
  const d = await api('GET', '/local/state');
  if (!d) return;

  // Update badge
  const badge = document.getElementById('online-badge');
  badge.textContent = d.offline ? 'OFFLINE' : 'ONLINE';
  badge.className   = 'badge ' + (d.offline ? 'badge-offline' : 'badge-online');

  // Update meta
  const ts = d.last_sync ? new Date(d.last_sync*1000).toLocaleTimeString() : 'never';
  document.getElementById('meta').textContent =
    `syncs: ${d.sync_count}  |  last: ${ts}  |  items: ${d.items.length}`;

  // Render item list
  const listEl = document.getElementById('item-list');
  if (!d.items.length) {
    listEl.innerHTML = '<span class="empty">No items yet.</span>';
    return;
  }
  listEl.innerHTML = d.items.map(item => {
    const qty = d.quantities[item] ?? 0;
    return `<div class="item-row">
      <span class="item-name">${item}</span>
      <div class="btn-row">
        <span class="qty-badge">×${qty}</span>
        <button class="btn-inc" onclick="incItem('${item}')">+</button>
        <button class="btn-dec" onclick="decItem('${item}')">-</button>
        <button class="btn-rm"  onclick="removeItem('${item}')">✕</button>
      </div>
    </div>`;
  }).join('');
}

async function addItem() {
  const input = document.getElementById('new-item');
  const item  = input.value.trim();
  if (!item) return;
  const d = await api('POST', '/local/add', {item});
  if (d.ok) { input.value = ''; addLog(`Added "${item}"`, 'log-ok'); }
  else       { addLog(`Error: ${d.reason}`, 'log-err'); }
  refreshState();
}

async function removeItem(item) {
  const d = await api('POST', '/local/remove', {item});
  if (d.ok) addLog(`Removed "${item}"`, 'log-ok');
  refreshState();
}

async function incItem(item) {
  const d = await api('POST', '/local/inc', {item, amount:1});
  if (d.ok) addLog(`Inc "${item}" → ${d.qty}`, 'log-info');
  refreshState();
}

async function decItem(item) {
  const d = await api('POST', '/local/dec', {item, amount:1});
  if (d.ok) addLog(`Dec "${item}" → ${d.qty}`, 'log-info');
  refreshState();
}

async function doSync() {
  addLog('Syncing…', 'log-info');
  const d = await api('POST', '/sync');
  if (d.ok) addLog(`Sync #${d.sync_count} complete`, 'log-ok');
  else      addLog(`Sync failed: ${d.reason}`, 'log-err');
  refreshState();
}

async function goOffline() {
  await api('POST', '/go_offline');
  addLog('Went OFFLINE — edits stored locally', 'log-err');
  refreshState();
}

async function goOnline() {
  addLog('Reconnecting and syncing…', 'log-info');
  const d = await api('POST', '/go_online');
  if (d.sync?.ok) addLog('Back ONLINE — sync complete', 'log-ok');
  else            addLog('Back ONLINE — sync failed (will retry)', 'log-err');
  refreshState();
}

// Enter key in add-item field
document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('new-item').addEventListener('keydown', e => {
    if (e.key === 'Enter') addItem();
  });
  refreshState();
  setInterval(refreshState, 2000);
  addLog(`Node ${replica} started`, 'log-info');
});
</script>
</body></html>
"""

@app.get("/")
def ui():
    return render_template_string(UI_TEMPLATE, replica_id=REPLICA_ID)

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    mode = "OFFLINE" if is_offline else "ONLINE"
    print(f"Starting offline app  replica={REPLICA_ID!r}  mode={mode}  port={PORT}")
    print(f"Raft peers: {RAFT_PEERS}")
    app.run(host="0.0.0.0", port=PORT, debug=False)
