import json
import unittest
from crdt_counter import GCounter  # Importing GCounter from the separate file

class PNCounter:

    def __init__(self, node_id: str):
        self.node_id = node_id
        self.P = GCounter(node_id)  # positive side
        self.N = GCounter(node_id)  # negative side

    def increment(self, amount: int = 1):
        """Add a positive amount."""
        self.P.increment(amount)

    def decrement(self, amount: int = 1):
        """Add a negative amount (via N counter)."""
        if amount < 1:
            raise ValueError("Decrement amount must be positive.")
        self.N.increment(amount)

    def value(self) -> int:
        """Current counter value = sum(P) - sum(N)."""
        return self.P.value() - self.N.value()

    def merge(self, other: "PNCounter"):
        """
        Merge another PNCounter into this one.
        Merges P and N G-Counters independently.
        State-based merge: element-wise max on both sides.
        """
        self.P.merge(other.P)
        self.N.merge(other.N)

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps({
            "node_id": self.node_id,
            "P": json.loads(self.P.to_json()),
            "N": json.loads(self.N.to_json()),
        })

    @classmethod
    def from_json(cls, data: str) -> "PNCounter":
        """Deserialize from JSON string."""
        d = json.loads(data)
        obj = cls(d["node_id"])
        obj.P = GCounter.from_json(json.dumps(d["P"]))
        obj.N = GCounter.from_json(json.dumps(d["N"]))
        return obj

    def __repr__(self):
        return (
            f"PNCounter(node={self.node_id}, "
            f"value={self.value()}, "
            f"P={self.P._counts}, N={self.N._counts})"
        )


# ===========================================================================
# Unit Tests
# ===========================================================================

class TestPNCounter(unittest.TestCase):

    def test_increment(self):
        c = PNCounter("A")
        c.increment(5)
        self.assertEqual(c.value(), 5)

    def test_decrement(self):
        c = PNCounter("A")
        c.increment(10)
        c.decrement(3)
        self.assertEqual(c.value(), 7)

    def test_value_can_go_negative(self):
        """PN-Counter can go below zero — no floor constraint."""
        c = PNCounter("A")
        c.decrement(5)
        self.assertEqual(c.value(), -5)

    def test_invalid_decrement(self):
        c = PNCounter("A")
        with self.assertRaises(ValueError):
            c.decrement(0)

    # --- Serialization ---

    def test_serialization_roundtrip(self):
        c = PNCounter("A")
        c.increment(7)
        c.decrement(2)

        serialized = c.to_json()
        restored = PNCounter.from_json(serialized)

        self.assertEqual(restored.value(), c.value())
        self.assertEqual(restored.node_id, "A")

    def test_serialization_preserves_internal_state(self):
        c = PNCounter("node-1")
        c.increment(3)
        c.decrement(1)

        d = json.loads(c.to_json())
        self.assertEqual(d["P"]["counts"]["node-1"], 3)
        self.assertEqual(d["N"]["counts"]["node-1"], 1)

    # --- Merge ---

    def test_merge_two_replicas(self):
        """
        Two replicas each do some local ops, then merge.
        Result should be: increments from both - decrements from both.
        """
        replica_A = PNCounter("A")
        replica_A.increment(10)
        replica_A.decrement(2)

        replica_B = PNCounter("B")
        replica_B.increment(5)
        replica_B.decrement(1)

        # Merge B into A
        replica_A.merge(replica_B)

        self.assertEqual(replica_A.value(), 12)

    def test_merge_commutativity(self):
        """
        Merge(A, B) == Merge(B, A) — order of merge doesn't matter.
        """
        c1 = PNCounter("node1")
        c1.increment(6)
        c1.decrement(2)

        c2 = PNCounter("node2")
        c2.increment(3)
        c2.decrement(1)

        # Merge A←B
        import copy
        c1_copy = copy.deepcopy(c1)
        c2_copy = copy.deepcopy(c2)

        c1.merge(c2)          # A absorbs B
        c2_copy.merge(c1_copy)  # B absorbs A

        self.assertEqual(c1.value(), c2_copy.value())

    def test_merge_idempotency(self):
        """Merging the same replica twice must not change the value."""
        c1 = PNCounter("A")
        c1.increment(5)

        c2 = PNCounter("B")
        c2.increment(3)

        c1.merge(c2)
        value_after_first_merge = c1.value()

        c1.merge(c2)  # merge again — should be no-op
        self.assertEqual(c1.value(), value_after_first_merge)

    # --- Convergence under concurrent modifications ---

    def test_convergence_two_replicas(self):
        """
        Replica A and B diverge with concurrent edits,
        then both merge each other → must reach the same final state.
        """
        import copy

        a = PNCounter("A")
        b = PNCounter("B")

        # Concurrent independent edits
        a.increment(10)
        a.decrement(3)

        b.increment(7)
        b.decrement(1)

        # Cross-merge: each merges the other
        a_snapshot = copy.deepcopy(a)
        b_snapshot = copy.deepcopy(b)

        a.merge(b_snapshot)
        b.merge(a_snapshot)

        # Both should converge to the same value
        self.assertEqual(a.value(), b.value())
        self.assertEqual(a.value(), 13)

    def test_convergence_three_replicas(self):
        """
        Three replicas with concurrent edits all merge into one final state.
        """
        import copy

        r1 = PNCounter("r1")
        r2 = PNCounter("r2")
        r3 = PNCounter("r3")

        r1.increment(5)
        r1.decrement(1)

        r2.increment(3)
        r2.decrement(2)

        r3.increment(8)
        r3.decrement(4)

        # Simulate gossip: each merges all others
        snapshots = [copy.deepcopy(r1), copy.deepcopy(r2), copy.deepcopy(r3)]

        for replica in [r1, r2, r3]:
            for snap in snapshots:
                replica.merge(snap)

        self.assertEqual(r1.value(), r2.value())
        self.assertEqual(r2.value(), r3.value())
        self.assertEqual(r1.value(), 9)

    def test_merge_does_not_lose_data_on_stale_replica(self):
        """
        A stale replica (missed some ops) merges with an up-to-date one.
        No data should be lost — element-wise max protects us.
        """
        import copy

        current = PNCounter("A")
        current.increment(20)
        current.decrement(5)

        stale = copy.deepcopy(current)  # snapshot before more ops

        # current does more work
        current.increment(10)
        current.decrement(3)

        # stale merges current → should catch up, not overwrite
        stale.merge(current)
        self.assertEqual(stale.value(), current.value())


if __name__ == "__main__":
    unittest.main(verbosity=2)