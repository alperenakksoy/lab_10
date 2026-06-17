"""
crdt_counter.py — G-Counter (Grow-only Counter) CRDT
=====================================================
Merge semantics: STATE-BASED
  Each replica maintains a vector of per-node counters.
  Merge performs element-wise maximum, so concurrent increments
  on different nodes converge without coordination.

Convergence proof (informal):
  - Merge is idempotent:  merge(s, s)      == s
  - Merge is commutative: merge(s1, s2)    == merge(s2, s1)
  - Merge is associative: merge(s1, merge(s2, s3)) == merge(merge(s1, s2), s3)
  Therefore the state lattice grows monotonically and all replicas converge.
"""

import json
import unittest

class GCounter:
    """
    Grow-only Counter CRDT.

    Parameters
    ----------
    node_id : str
        Unique identifier for this replica (e.g. "node-A").
    """

    def __init__(self, node_id: str):
        self.node_id = node_id
        # Map from node_id -> integer count.  Only this replica writes
        # to its own slot; all other slots are updated via merge().
        self._counts: dict[str, int] = {node_id: 0}

    # ------------------------------------------------------------------
    # Local operations
    # ------------------------------------------------------------------

    def increment(self, amount: int = 1) -> None:
        """Increment this replica's own slot. Amount must be positive."""
        if amount < 1:
            raise ValueError("G-Counter only supports positive increments.")
        self._counts[self.node_id] += amount
    # ------------------------------------------------------------------
    # State query
    # ------------------------------------------------------------------

    def value(self) -> int:
        """Return the current counter value (sum of all per-node slots)."""
        return sum(self._counts.values())

    def state(self) -> dict[str, int]:
        """Return a *copy* of the internal vector (safe for inspection)."""
        return dict(self._counts)

    # ------------------------------------------------------------------
    # Merge
    # ------------------------------------------------------------------

    def merge(self, remote: "GCounter") -> None:
        """
        Merge a remote replica's state into this one (element-wise max).
        This replica is mutated in place; the remote is left unchanged.
        """
        all_nodes = set(self._counts) | set(remote._counts)
        for node in all_nodes:
            self._counts[node] = max(
                self._counts.get(node, 0),
                remote._counts.get(node, 0),
            )

    # ------------------------------------------------------------------
    # JSON serialization / deserialization
    # ------------------------------------------------------------------

    def to_json(self) -> str:
        """Serialize full state to a JSON string for transport or storage."""
        payload = {"node_id": self.node_id, "counts": self._counts}
        return json.dumps(payload)

    @classmethod
    def from_json(cls, data: str) -> "GCounter":
        """Reconstruct a GCounter from a JSON string produced by to_json()."""
        payload = json.loads(data)
        obj = cls(payload["node_id"])
        obj._counts = payload["counts"]
        return obj

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return f"GCounter(node={self.node_id!r}, value={self.value()}, state={self._counts})"

# ======================================================================
# Unit Tests
# ======================================================================

class TestGCounter(unittest.TestCase):

    # 1. Local operations -------------------------------------------------

    def test_initial_value_is_zero(self):
        c = GCounter("A")
        self.assertEqual(c.value(), 0)

    def test_single_increment(self):
        c = GCounter("A")
        c.increment()
        self.assertEqual(c.value(), 1)

    def test_multiple_increments(self):
        c = GCounter("A")
        for _ in range(5):
            c.increment()
        self.assertEqual(c.value(), 5)

    def test_increment_by_amount(self):
        c = GCounter("A")
        c.increment(10)
        self.assertEqual(c.value(), 10)

    def test_negative_increment_raises(self):
        c = GCounter("A")
        with self.assertRaises(ValueError):
            c.increment(-1)

    def test_zero_increment_raises(self):
        c = GCounter("A")
        with self.assertRaises(ValueError):
            c.increment(0)

    # 2. Serialization ----------------------------------------------------

    def test_to_json_contains_node_id_and_counts(self):
        c = GCounter("node-X")
        c.increment(3)
        payload = json.loads(c.to_json())
        self.assertEqual(payload["node_id"], "node-X")
        self.assertEqual(payload["counts"]["node-X"], 3)

    def test_round_trip_json(self):
        c = GCounter("node-Y")
        c.increment(7)
        restored = GCounter.from_json(c.to_json())
        self.assertEqual(restored.node_id, "node-Y")
        self.assertEqual(restored.value(), 7)
        self.assertEqual(restored.state(), c.state())

    # 3. Merge ------------------------------------------------------------

    def test_merge_disjoint_nodes(self):
        """Two replicas with independent increments; merge sums both slots."""
        a = GCounter("A")
        b = GCounter("B")
        a.increment(3)
        b.increment(5)

        a.merge(b)

        self.assertEqual(a.value(), 8)          # 3 + 5
        self.assertIn("B", a.state())

    def test_merge_is_idempotent(self):
        a = GCounter("A")
        b = GCounter("B")
        a.increment(4)
        b.increment(2)

        a.merge(b)
        a.merge(b)  # merging twice should not change value

        self.assertEqual(a.value(), 6)

    def test_merge_is_commutative(self):
        """merge(a, b) and merge(b, a) should yield the same total value."""
        a = GCounter("A")
        b = GCounter("B")
        a.increment(3)
        b.increment(7)

        a_copy = GCounter.from_json(a.to_json())
        b_copy = GCounter.from_json(b.to_json())

        a.merge(b)       # a absorbs b
        b_copy.merge(a_copy)  # b_copy absorbs a_copy

        self.assertEqual(a.value(), b_copy.value())

    def test_merge_takes_max_not_sum_for_same_node(self):
        """
        If two replicas both have a slot for the same node (e.g. from
        a previous sync), merge must take the max, not add, to avoid
        double-counting.
        """
        a = GCounter("A")
        a.increment(5)

        # Simulate b having received an older state of a (count=3) before
        # a incremented twice more
        b = GCounter("B")
        b._counts["A"] = 3   # stale knowledge of A
        b.increment(10)

        a.merge(b)

        # A's slot stays at 5 (max of 5 and 3); B's slot is 10 → total 15
        self.assertEqual(a.state()["A"], 5)
        self.assertEqual(a.value(), 15)

    # 4. Convergence across 2–3 replicas ----------------------------------

    def test_two_replica_convergence(self):
        """
        Scenario: Two replicas make concurrent edits without communicating,
        then exchange state.  Both must converge to the same value.
        """
        r1 = GCounter("R1")
        r2 = GCounter("R2")

        # Concurrent, independent increments
        r1.increment(4)
        r2.increment(6)

        # Exchange state (each merges the other's state)
        r1.merge(r2)
        r2.merge(r1)

        self.assertEqual(r1.value(), r2.value())
        self.assertEqual(r1.value(), 10)

    def test_three_replica_convergence(self):
        """
        Scenario: Three replicas each increment independently.
        After a full gossip round all reach the same state.
        """
        r1 = GCounter("R1")
        r2 = GCounter("R2")
        r3 = GCounter("R3")

        r1.increment(1)
        r2.increment(2)
        r3.increment(3)

        # Gossip: everyone merges everyone else
        r1.merge(r2); r1.merge(r3)
        r2.merge(r1); r2.merge(r3)
        r3.merge(r1); r3.merge(r2)

        self.assertEqual(r1.value(), r2.value())
        self.assertEqual(r2.value(), r3.value())
        self.assertEqual(r1.value(), 6)

    def test_convergence_with_out_of_order_merges(self):
        """
        Replicas receive state in different orders; final values must match.
        """
        r1 = GCounter("R1")
        r2 = GCounter("R2")
        r3 = GCounter("R3")

        r1.increment(5)
        r2.increment(3)
        r3.increment(2)

        # R1 learns R3 first, then R2
        r1.merge(r3)
        r1.merge(r2)

        # R2 learns R1 first (already has R3 via r1's merged state), then R3
        r2.merge(r1)
        r2.merge(r3)

        # R3 learns R2 (which has R1), then R1
        r3.merge(r2)
        r3.merge(r1)

        self.assertEqual(r1.value(), r2.value())
        self.assertEqual(r2.value(), r3.value())
        self.assertEqual(r1.value(), 10)

if __name__ == "__main__":
    unittest.main(verbosity=2)