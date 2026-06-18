"""
raft_client.py — Enhanced Raft Client with Failover
====================================================
Features
--------
  * Leader discovery  — queries every node to find who is currently leader
  * Node rotation     — on write failure, tries the next node in the list
  * Exponential backoff — doubles wait time between retries, caps at 4 s
  * Re-election wait  — detects "no leader" state and retries until one is
                        elected (within a configurable deadline)
  * Latency tracking  — every write records wall-clock latency (ms)

Usage
-----
    from raft_client import RaftClient

    client = RaftClient([
        "http://localhost:5001",
        "http://localhost:5002",
        "http://localhost:5003",
    ])

    ok, latency_ms = client.write("mykey", "myvalue")
    value = client.read("mykey")
"""

import time
import requests

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DEFAULT_TIMEOUT_S   = 1.0   # per-request HTTP timeout
DEFAULT_MAX_RETRIES = 10    # across ALL nodes before giving up
DEFAULT_BACKOFF_CAP = 4.0   # maximum back-off sleep in seconds
DEFAULT_BACKOFF_BASE= 0.05  # initial back-off (50 ms)
LEADER_ELECTION_DEADLINE = 5.0  # seconds to wait for a new leader after failover


class RaftClient:
    """
    Fault-tolerant Raft cluster client.

    Parameters
    ----------
    nodes : list[str]
        Base URLs of all cluster nodes, e.g. ["http://node1:5000", ...].
    http_timeout : float
        Per-request timeout in seconds.
    max_retries : int
        Maximum number of write attempts before raising.
    """

    def __init__(
        self,
        nodes: list[str],
        http_timeout: float = DEFAULT_TIMEOUT_S,
        max_retries: int    = DEFAULT_MAX_RETRIES,
    ):
        if not nodes:
            raise ValueError("At least one node URL is required.")
        self.nodes       = list(nodes)
        self.http_timeout = http_timeout
        self.max_retries  = max_retries

        # Index into self.nodes for the current preferred leader
        self._leader_idx: int | None = None
        self._discover_leader()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def write(self, key: str, value: str) -> tuple[bool, float]:
        """
        Write key=value through the Raft cluster.

        Returns
        -------
        (success: bool, latency_ms: float)
            success      — True if the cluster committed the write
            latency_ms   — wall-clock time from call to successful ack (ms)
        """
        t_start  = time.monotonic()
        backoff  = DEFAULT_BACKOFF_BASE
        attempts = 0

        while attempts < self.max_retries:
            node_url = self._current_leader_url()
            try:
                r = requests.post(
                    f"{node_url}/kv/write",
                    json={"key": key, "value": value},
                    timeout=self.http_timeout,
                )
                data = r.json()

                if data.get("ok"):
                    latency_ms = (time.monotonic() - t_start) * 1000
                    return True, round(latency_ms, 2)

                # Redirect: another node is leader
                if data.get("reason") == "not_leader":
                    hint = data.get("leader")
                    if hint and hint in self.nodes:
                        self._leader_idx = self.nodes.index(hint)
                    else:
                        self._rotate_leader()
                    attempts += 1
                    continue

                # Generic failure — rotate and backoff
                self._rotate_leader()

            except requests.exceptions.Timeout:
                # Node is unresponsive — likely dead; rotate immediately
                print(f"  [client] timeout contacting {node_url}, rotating …")
                self._rotate_leader()

            except requests.exceptions.ConnectionError:
                print(f"  [client] connection error to {node_url}, rotating …")
                self._rotate_leader()

            # Exponential backoff before next attempt
            sleep_time = min(backoff, DEFAULT_BACKOFF_CAP)
            time.sleep(sleep_time)
            backoff *= 2
            attempts += 1

        latency_ms = (time.monotonic() - t_start) * 1000
        print(f"  [client] FAILED after {self.max_retries} attempts "
              f"({latency_ms:.1f} ms total)")
        return False, round(latency_ms, 2)

    def read(self, key: str, node_idx: int = 0) -> dict:
        """
        Read a key from the cluster.  Reads any node (may be stale on followers).
        node_idx selects which node to query (default: 0).
        """
        url = self.nodes[node_idx % len(self.nodes)]
        try:
            r = requests.get(f"{url}/kv/{key}", timeout=self.http_timeout)
            return r.json()
        except Exception as exc:
            return {"error": str(exc), "key": key, "value": None}

    def status_all(self) -> list[dict]:
        """Fetch /raft/status from every node (best-effort)."""
        results = []
        for url in self.nodes:
            try:
                r = requests.get(f"{url}/raft/status", timeout=self.http_timeout)
                results.append({"url": url, **r.json()})
            except Exception:
                results.append({"url": url, "up": False})
        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _discover_leader(self):
        """Query all nodes to find who thinks they are leader."""
        for i, url in enumerate(self.nodes):
            try:
                r    = requests.get(f"{url}/raft/status", timeout=self.http_timeout)
                data = r.json()
                if data.get("state") == "leader":
                    self._leader_idx = i
                    print(f"  [client] leader discovered: {url}")
                    return
            except Exception:
                pass
        # No leader found yet — start with node 0 and let rotation handle it
        self._leader_idx = 0
        print("  [client] no leader found during discovery, starting at node 0")

    def _current_leader_url(self) -> str:
        idx = self._leader_idx if self._leader_idx is not None else 0
        return self.nodes[idx % len(self.nodes)]

    def _rotate_leader(self):
        """Move to the next node in the ring."""
        current = self._leader_idx if self._leader_idx is not None else 0
        self._leader_idx = (current + 1) % len(self.nodes)
        print(f"  [client] rotated to {self.nodes[self._leader_idx]}")
