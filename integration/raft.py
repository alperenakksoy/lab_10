"""
raft.py — Simplified Raft Consensus Implementation
====================================================

Implements the core Raft protocol:
  - Leader Election   (§5.2): randomised timeout 150–300 ms, term-based voting
  - Log Replication   (§5.3): AppendEntries RPC, majority commit index tracking
  - State Machine     (§5.4): key-value store applied from committed log entries

All inter-node RPCs are plain HTTP POST (via requests).
The Flask HTTP layer lives in app.py; this module is pure protocol logic.

References:
  Ongaro & Ousterhout (2014). "In Search of an Understandable Consensus Algorithm."
  USENIX ATC 2014.  Sections 1–5.
"""

import os
import random
import threading
import time
import logging
import requests

log = logging.getLogger("raft")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  [%(name)s]  %(message)s",
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ELECTION_TIMEOUT_MIN = 0.150   # 150 ms
ELECTION_TIMEOUT_MAX = 0.300   # 300 ms
HEARTBEAT_INTERVAL   = 0.075   # 75 ms  (< min election timeout)
RPC_TIMEOUT          = 0.100   # 100 ms per RPC call

# Node states
FOLLOWER  = "follower"
CANDIDATE = "candidate"
LEADER    = "leader"

# ---------------------------------------------------------------------------
# RaftNode
# ---------------------------------------------------------------------------

class RaftNode:
    """
    A single Raft node.

    Parameters
    ----------
    node_id   : unique string, e.g. "node1"
    peers     : list of peer base URLs, e.g. ["http://node2:5000", "http://node3:5000"]
    """

    def __init__(self, node_id: str, peers: list[str]):
        self.node_id = node_id
        self.peers   = peers          # URLs of the *other* nodes

        # ---- Persistent Raft state (would survive restarts in production) ----
        self.current_term = 0         # latest term seen
        self.voted_for    = None      # candidate_id voted for in current term

        # Log: list of {"term": int, "command": dict}
        # Index 0 is a sentinel (no-op at term 0, index 0)
        self.log = [{"term": 0, "command": None}]

        # ---- Volatile state ----
        self.commit_index = 0         # highest log index known to be committed
        self.last_applied = 0         # highest log index applied to state machine

        # ---- Leader volatile state (reinitialized after election) ----
        # next_index[peer]  = next log index to send to that peer
        # match_index[peer] = highest log index known to be replicated on peer
        self.next_index  : dict[str, int] = {}
        self.match_index : dict[str, int] = {}

        # ---- Role ----
        self.state  = FOLLOWER
        self.leader_id = None

        # ---- State machine: simple KV store ----
        self.kv_store: dict[str, str] = {}

        # ---- Timers & synchronisation ----
        self._lock = threading.RLock()
        self._election_deadline = self._new_election_deadline()
        self._running = False

    # ------------------------------------------------------------------
    # Public control
    # ------------------------------------------------------------------

    def start(self):
        """Start background threads (ticker + applier)."""
        self._running = True
        threading.Thread(target=self._ticker,  daemon=True, name="raft-ticker").start()
        threading.Thread(target=self._applier, daemon=True, name="raft-applier").start()
        log.info("[%s] started as %s, term=%d", self.node_id, self.state, self.current_term)

    def stop(self):
        self._running = False

    # ------------------------------------------------------------------
    # Client-facing: propose a command
    # ------------------------------------------------------------------

    def propose(self, command: dict) -> dict:
        """
        Submit a command to the cluster.
        Only the leader can accept proposals.
        Returns {"ok": True, "index": <log_index>} or {"ok": False, "leader": <url>}.
        """
        with self._lock:
            if self.state != LEADER:
                return {"ok": False, "leader": self.leader_id, "reason": "not_leader"}
            entry = {"term": self.current_term, "command": command}
            self.log.append(entry)
            idx = len(self.log) - 1
            log.info("[%s] leader appended entry idx=%d  cmd=%s", self.node_id, idx, command)
            # Replicate immediately (don't wait for next heartbeat tick)
            self._replicate_to_all()
            return {"ok": True, "index": idx}

    # ------------------------------------------------------------------
    # RPC handlers (called from Flask routes)
    # ------------------------------------------------------------------

    def handle_request_vote(self, args: dict) -> dict:
        """
        RequestVote RPC receiver.
        args: { term, candidate_id, last_log_index, last_log_term }
        """
        with self._lock:
            term          = args["term"]
            candidate_id  = args["candidate_id"]
            last_log_idx  = args["last_log_index"]
            last_log_term = args["last_log_term"]

            # If we see a higher term, revert to follower immediately
            if term > self.current_term:
                self._become_follower(term)

            vote_granted = False
            if (term >= self.current_term and
                    (self.voted_for is None or self.voted_for == candidate_id) and
                    self._candidate_log_ok(last_log_idx, last_log_term)):
                self.voted_for = candidate_id
                vote_granted   = True
                self._reset_election_timer()
                log.info("[%s] granted vote to %s for term %d",
                         self.node_id, candidate_id, term)

            return {"term": self.current_term, "vote_granted": vote_granted}

    def handle_append_entries(self, args: dict) -> dict:
        """
        AppendEntries RPC receiver (also serves as heartbeat when entries=[]).
        args: { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit }
        """
        with self._lock:
            term            = args["term"]
            leader_id       = args["leader_id"]
            prev_log_index  = args["prev_log_index"]
            prev_log_term   = args["prev_log_term"]
            entries         = args["entries"]          # list of {term, command}
            leader_commit   = args["leader_commit"]

            # Reject stale leaders
            if term < self.current_term:
                return {"term": self.current_term, "success": False}

            # Valid leader — reset timer, update state
            if term > self.current_term:
                self._become_follower(term)
            elif self.state == CANDIDATE:
                self._become_follower(term)

            self.leader_id = leader_id
            self._reset_election_timer()

            # Log consistency check (§5.3)
            if prev_log_index >= len(self.log):
                return {"term": self.current_term, "success": False,
                        "conflict_index": len(self.log), "conflict_term": -1}

            if self.log[prev_log_index]["term"] != prev_log_term:
                # Find first index of conflicting term for fast back-tracking
                conflict_term  = self.log[prev_log_index]["term"]
                conflict_index = prev_log_index
                for i in range(1, prev_log_index + 1):
                    if self.log[i]["term"] == conflict_term:
                        conflict_index = i
                        break
                return {"term": self.current_term, "success": False,
                        "conflict_index": conflict_index, "conflict_term": conflict_term}

            # Append new entries, deleting conflicting suffix if needed
            for i, entry in enumerate(entries):
                idx = prev_log_index + 1 + i
                if idx < len(self.log):
                    if self.log[idx]["term"] != entry["term"]:
                        del self.log[idx:]   # delete conflicting suffix
                        self.log.append(entry)
                else:
                    self.log.append(entry)

            # Advance commit index
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, len(self.log) - 1)

            return {"term": self.current_term, "success": True,
                    "match_index": len(self.log) - 1}

    # ------------------------------------------------------------------
    # Status / KV read
    # ------------------------------------------------------------------

    def status(self) -> dict:
        with self._lock:
            return {
                "node_id":      self.node_id,
                "state":        self.state,
                "term":         self.current_term,
                "leader":       self.leader_id,
                "log_length":   len(self.log) - 1,   # exclude sentinel
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
                "kv_size":      len(self.kv_store),
            }

    def kv_read(self, key: str) -> dict:
        """Read from local state machine (may be slightly stale on followers)."""
        with self._lock:
            is_leader = (self.state == LEADER)
            val = self.kv_store.get(key)
        result = {"key": key, "value": val, "found": val is not None,
                  "node_id": self.node_id, "is_leader": is_leader}
        if not is_leader:
            result["warning"] = "reading from follower — value may be stale"
        return result

    # ------------------------------------------------------------------
    # Background: ticker (elections + heartbeats)
    # ------------------------------------------------------------------

    def _ticker(self):
        while self._running:
            time.sleep(0.010)   # 10 ms tick resolution
            with self._lock:
                now = time.monotonic()
                if self.state == LEADER:
                    self._send_heartbeats()
                elif now >= self._election_deadline:
                    self._start_election()

    def _send_heartbeats(self):
        """Leader sends AppendEntries (possibly with new entries) to all peers."""
        self._replicate_to_all()

    def _replicate_to_all(self):
        """Send AppendEntries to every peer (non-blocking; threads per peer)."""
        for peer in self.peers:
            threading.Thread(
                target=self._replicate_to_peer,
                args=(peer,),
                daemon=True,
            ).start()

    def _replicate_to_peer(self, peer_url: str):
        with self._lock:
            if self.state != LEADER:
                return
            ni         = self.next_index.get(peer_url, len(self.log))
            prev_idx   = ni - 1
            prev_term  = self.log[prev_idx]["term"] if prev_idx >= 0 else 0
            entries    = self.log[ni:]
            args = {
                "term":           self.current_term,
                "leader_id":      self.node_id,
                "prev_log_index": prev_idx,
                "prev_log_term":  prev_term,
                "entries":        entries,
                "leader_commit":  self.commit_index,
            }
            my_term = self.current_term

        try:
            r    = requests.post(f"{peer_url}/raft/append_entries",
                                 json=args, timeout=RPC_TIMEOUT)
            resp = r.json()
        except Exception:
            return   # peer unreachable — will retry next heartbeat

        with self._lock:
            # Ignore stale replies
            if self.state != LEADER or my_term != self.current_term:
                return

            if resp.get("term", 0) > self.current_term:
                self._become_follower(resp["term"])
                return

            if resp.get("success"):
                match = resp.get("match_index", prev_idx + len(entries))
                self.match_index[peer_url] = max(
                    self.match_index.get(peer_url, 0), match)
                self.next_index[peer_url]  = self.match_index[peer_url] + 1
                self._advance_commit_index()
            else:
                # Back-track next_index (use conflict hint if provided)
                conflict_idx = resp.get("conflict_index", ni - 1)
                self.next_index[peer_url] = max(1, conflict_idx)

    def _advance_commit_index(self):
        """Advance commit_index to the highest index replicated on a majority."""
        # Must be called under self._lock
        n = len(self.log)
        for idx in range(self.commit_index + 1, n):
            if self.log[idx]["term"] != self.current_term:
                continue   # only commit entries from current term (§5.4.2)
            replicated_on = 1   # count self
            for peer in self.peers:
                if self.match_index.get(peer, 0) >= idx:
                    replicated_on += 1
            majority = (len(self.peers) + 1) // 2 + 1
            if replicated_on >= majority:
                self.commit_index = idx
                log.info("[%s] commit_index advanced to %d", self.node_id, idx)

    # ------------------------------------------------------------------
    # Background: applier (commit → state machine)
    # ------------------------------------------------------------------

    def _applier(self):
        """Apply committed log entries to the KV state machine."""
        while self._running:
            time.sleep(0.010)
            with self._lock:
                while self.last_applied < self.commit_index:
                    self.last_applied += 1
                    entry = self.log[self.last_applied]
                    cmd   = entry.get("command")
                    if cmd and cmd.get("op") == "set":
                        self.kv_store[cmd["key"]] = cmd["value"]
                        log.info("[%s] applied idx=%d  SET %s=%s",
                                 self.node_id, self.last_applied,
                                 cmd["key"], cmd["value"])

    # ------------------------------------------------------------------
    # Leader election
    # ------------------------------------------------------------------

    def _start_election(self):
        """Transition to candidate and solicit votes."""
        self.state         = CANDIDATE
        self.current_term += 1
        self.voted_for     = self.node_id   # vote for self
        self.leader_id     = None
        self._reset_election_timer()

        term          = self.current_term
        last_log_idx  = len(self.log) - 1
        last_log_term = self.log[last_log_idx]["term"]

        log.info("[%s] starting election for term %d", self.node_id, term)

        votes_received = [1]   # mutable list so closure can mutate
        vote_lock      = threading.Lock()

        def request_vote(peer_url):
            args = {
                "term":           term,
                "candidate_id":   self.node_id,
                "last_log_index": last_log_idx,
                "last_log_term":  last_log_term,
            }
            try:
                r    = requests.post(f"{peer_url}/raft/request_vote",
                                     json=args, timeout=RPC_TIMEOUT)
                resp = r.json()
            except Exception:
                return

            with self._lock:
                if self.state != CANDIDATE or self.current_term != term:
                    return
                if resp.get("term", 0) > self.current_term:
                    self._become_follower(resp["term"])
                    return
                if resp.get("vote_granted"):
                    with vote_lock:
                        votes_received[0] += 1
                        if votes_received[0] > (len(self.peers) + 1) / 2:
                            self._become_leader()

        threads = [threading.Thread(target=request_vote, args=(p,), daemon=True)
                   for p in self.peers]
        for t in threads:
            t.start()

    def _become_leader(self):
        """Transition to leader (called under self._lock)."""
        if self.state != CANDIDATE:
            return
        self.state     = LEADER
        self.leader_id = self.node_id
        log.info("[%s] became LEADER for term %d", self.node_id, self.current_term)

        # Reinitialize next_index and match_index
        for peer in self.peers:
            self.next_index[peer]  = len(self.log)
            self.match_index[peer] = 0

        # Send immediate heartbeat so followers learn the new leader fast
        self._replicate_to_all()

    def _become_follower(self, term: int):
        """Revert to follower (called under self._lock)."""
        log.info("[%s] becoming FOLLOWER (term %d → %d)", self.node_id,
                 self.current_term, term)
        self.state        = FOLLOWER
        self.current_term = term
        self.voted_for    = None
        self._reset_election_timer()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _candidate_log_ok(self, last_log_idx: int, last_log_term: int) -> bool:
        """Return True if candidate's log is at least as up-to-date as ours (§5.4.1)."""
        my_last_term = self.log[-1]["term"]
        my_last_idx  = len(self.log) - 1
        if last_log_term != my_last_term:
            return last_log_term > my_last_term
        return last_log_idx >= my_last_idx

    def _reset_election_timer(self):
        timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
        self._election_deadline = time.monotonic() + timeout

    def _new_election_deadline(self) -> float:
        timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
        return time.monotonic() + timeout