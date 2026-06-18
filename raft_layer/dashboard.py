"""
dashboard.py — Raft Cluster Monitoring Dashboard
=================================================
A lightweight Flask app (~25 lines of logic) that polls all cluster
nodes every second and renders a live HTML table showing:
  - Current leader
  - Current term per node
  - Log length per node
  - Node status (up / down)
  - Last heartbeat (last successful poll) timestamp

Run:
    RAFT_PEERS=http://node1:5000,http://node2:5000,http://node3:5000 \
    python dashboard.py

Or just:
    python dashboard.py          # defaults to localhost ports 5001-5003
"""

import os
import time
import threading
import requests
from flask import Flask, render_template_string

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
_raw = os.getenv("RAFT_PEERS", "http://localhost:5001,http://localhost:5002,http://localhost:5003")
PEERS: list[str] = [p.strip() for p in _raw.split(",") if p.strip()]
POLL_INTERVAL = 1.0   # seconds between polls

# ---------------------------------------------------------------------------
# Shared node-status store (updated by background poller)
# ---------------------------------------------------------------------------
node_status: dict[str, dict] = {
    url: {"url": url, "up": False, "term": "-", "state": "-",
          "log_length": "-", "commit_index": "-", "leader": "-",
          "last_seen": None}
    for url in PEERS
}
_status_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Background poller
# ---------------------------------------------------------------------------

def _poll_nodes():
    """Continuously poll every peer and update node_status."""
    while True:
        for url in PEERS:
            try:
                r = requests.get(f"{url}/raft/status", timeout=0.5)
                data = r.json()
                with _status_lock:
                    node_status[url].update({
                        "up":           True,
                        "term":         data.get("term", "-"),
                        "state":        data.get("state", "-").upper(),
                        "log_length":   data.get("log_length", "-"),
                        "commit_index": data.get("commit_index", "-"),
                        "leader":       data.get("leader", "-"),
                        "last_seen":    time.strftime("%H:%M:%S"),
                    })
            except Exception:
                with _status_lock:
                    node_status[url]["up"] = False
                    node_status[url]["state"] = "DOWN"
        time.sleep(POLL_INTERVAL)

threading.Thread(target=_poll_nodes, daemon=True, name="poller").start()

# ---------------------------------------------------------------------------
# HTML template
# ---------------------------------------------------------------------------
TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="2">
  <title>Raft Cluster Monitor</title>
  <style>
    body  { font-family: monospace; background: #0f1117; color: #e0e0e0;
            display: flex; flex-direction: column; align-items: center;
            padding: 2rem; }
    h1    { color: #7dd3fc; margin-bottom: .3rem; }
    p.sub { color: #64748b; font-size: .85rem; margin-bottom: 1.5rem; }
    table { border-collapse: collapse; min-width: 700px; }
    th    { background: #1e293b; color: #94a3b8; text-transform: uppercase;
            font-size: .75rem; letter-spacing: .08em; padding: .6rem 1rem;
            text-align: left; border-bottom: 2px solid #334155; }
    td    { padding: .55rem 1rem; border-bottom: 1px solid #1e293b; }
    tr:hover td { background: #1a2332; }
    .LEADER   { color: #4ade80; font-weight: bold; }
    .FOLLOWER { color: #93c5fd; }
    .CANDIDATE{ color: #fbbf24; }
    .DOWN     { color: #f87171; }
    .dot-up   { display:inline-block; width:9px; height:9px; border-radius:50%;
                background:#4ade80; margin-right:6px; }
    .dot-down { display:inline-block; width:9px; height:9px; border-radius:50%;
                background:#f87171; margin-right:6px; }
    .leader-badge { background:#164e2a; color:#4ade80; border:1px solid #4ade80;
                    border-radius:4px; padding:1px 7px; font-size:.8rem; }
  </style>
</head>
<body>
  <h1>⚡ Raft Cluster Monitor</h1>
  <p class="sub">Auto-refreshes every 2 s &nbsp;|&nbsp; {{ now }}</p>
  <table>
    <thead>
      <tr>
        <th>Node URL</th>
        <th>Status</th>
        <th>Role</th>
        <th>Term</th>
        <th>Log Length</th>
        <th>Commit Index</th>
        <th>Known Leader</th>
        <th>Last Seen</th>
      </tr>
    </thead>
    <tbody>
      {% for node in nodes %}
      <tr>
        <td>{{ node.url }}</td>
        <td>
          {% if node.up %}
            <span class="dot-up"></span>UP
          {% else %}
            <span class="dot-down"></span>DOWN
          {% endif %}
        </td>
        <td class="{{ node.state }}">
          {{ node.state }}
          {% if node.state == "LEADER" %}<span class="leader-badge">★ leader</span>{% endif %}
        </td>
        <td>{{ node.term }}</td>
        <td>{{ node.log_length }}</td>
        <td>{{ node.commit_index }}</td>
        <td>{{ node.leader }}</td>
        <td>{{ node.last_seen or "—" }}</td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
</body>
</html>
"""

# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    with _status_lock:
        nodes = list(node_status.values())
    return render_template_string(TEMPLATE,
                                  nodes=nodes,
                                  now=time.strftime("%Y-%m-%d %H:%M:%S"))

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("DASHBOARD_PORT", "8080"))
    print(f"Dashboard running on http://0.0.0.0:{port}")
    print(f"Watching peers: {PEERS}")
    app.run(host="0.0.0.0", port=port, debug=False)
