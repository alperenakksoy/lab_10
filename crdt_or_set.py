import json
import unittest

class ORSet:
    """
    Observed-Remove Set CRDT.

    Internal state:
        adds: dict[element, set[tuple[replica_id, clock]]]
        removes: dict[element, set[tuple[replica_id, clock]]]

    Tag format: (replica_id: str, clock: int)
    """

    def __init__(self, replica_id: str):
        self.replica_id = replica_id
        self.clock = 0
        self.adds: dict = {}
        self.removes: dict = {}

    # ------------------------------------------------------------------
    # Local Operations
    # ------------------------------------------------------------------

    def add(self, element) -> tuple:
        """
        Add element to the set.
        Generates a unique tag (replica_id, clock) for this operation.
        """
        self.clock += 1
        tag = (self.replica_id, self.clock)

        if element not in self.adds:
            self.adds[element] = set()
        self.adds[element].add(tag)

        return tag

    def remove(self, element):
        """
        Remove element from the set.
        Copies all currently observed tags for this element into the 'removes' set.
        """
        if element in self.adds:
            observed_tags = self.adds[element]
            if element not in self.removes:
                self.removes[element] = set()
            self.removes[element].update(observed_tags)

    def contains(self, element) -> bool:
        """Element is present if it has at least one tag in adds NOT in removes."""
        add_tags = self.adds.get(element, set())
        remove_tags = self.removes.get(element, set())
        return bool(add_tags - remove_tags)

    def value(self) -> set:
        """Return the set of all currently present elements."""
        result = set()
        for element, add_tags in self.adds.items():
            remove_tags = self.removes.get(element, set())
            if add_tags - remove_tags:
                result.add(element)
        return result

    # ------------------------------------------------------------------
    # Merge
    # ------------------------------------------------------------------

    def merge(self, other: "ORSet"):
        """
        Merge another ORSet into this one.
        Unions both 'adds' and 'removes' dictionaries.
        """
        # Merge Adds
        for element, tags in other.adds.items():
            if element not in self.adds:
                self.adds[element] = set()
            self.adds[element].update(tags)

        # Merge Removes
        for element, tags in other.removes.items():
            if element not in self.removes:
                self.removes[element] = set()
            self.removes[element].update(tags)

        # Advance clock past any remote clock values to avoid tag collisions
        for tags in other.adds.values():
            for (rid, clk) in tags:
                if rid == self.replica_id and clk >= self.clock:
                    self.clock = clk + 1

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_json(self) -> str:
        """Serialize to JSON."""
        serializable_adds = {
            str(e): list(list(tag) for tag in tags)
            for e, tags in self.adds.items()
        }
        serializable_removes = {
            str(e): list(list(tag) for tag in tags)
            for e, tags in self.removes.items()
        }
        return json.dumps({
            "replica_id": self.replica_id,
            "clock": self.clock,
            "adds": serializable_adds,
            "removes": serializable_removes,
        })

    @classmethod
    def from_json(cls, data: str) -> "ORSet":
        """Deserialize from JSON string."""
        d = json.loads(data)
        obj = cls(d["replica_id"])
        obj.clock = d["clock"]

        obj.adds = {
            e: {tuple(tag) for tag in tags}
            for e, tags in d.get("adds", {}).items()
        }
        obj.removes = {
            e: {tuple(tag) for tag in tags}
            for e, tags in d.get("removes", {}).items()
        }
        return obj

    def __repr__(self):
        return (
            f"ORSet(replica={self.replica_id}, "
            f"clock={self.clock}, "
            f"value={sorted(str(e) for e in self.value())})"
        )


# ===========================================================================
# Unit Tests
# ===========================================================================

class TestORSet(unittest.TestCase):

    # --- Local operations ---

    def test_add_single_element(self):
        s = ORSet("A")
        s.add("apple")
        self.assertIn("apple", s.value())

    def test_add_multiple_elements(self):
        s = ORSet("A")
        s.add("apple")
        s.add("banana")
        s.add("cherry")
        self.assertEqual(s.value(), {"apple", "banana", "cherry"})

    def test_remove_existing_element(self):
        s = ORSet("A")
        s.add("apple")
        s.remove("apple")
        self.assertNotIn("apple", s.value())

    def test_remove_nonexistent_is_noop(self):
        s = ORSet("A")
        s.remove("ghost")  # should not raise
        self.assertEqual(s.value(), set())

    def test_add_after_remove(self):
        s = ORSet("A")
        s.add("apple")
        s.remove("apple")
        s.add("apple")
        self.assertIn("apple", s.value())

    def test_contains(self):
        s = ORSet("A")
        s.add("x")
        self.assertTrue(s.contains("x"))
        s.remove("x")
        self.assertFalse(s.contains("x"))

    def test_clock_increments_on_add(self):
        s = ORSet("A")
        s.add("x")
        s.add("y")
        self.assertEqual(s.clock, 2)

    # --- Serialization ---

    def test_serialization_roundtrip_empty(self):
        s = ORSet("A")
        restored = ORSet.from_json(s.to_json())
        self.assertEqual(restored.value(), set())
        self.assertEqual(restored.replica_id, "A")

    def test_serialization_roundtrip_with_elements(self):
        s = ORSet("A")
        s.add("apple")
        s.add("banana")
        s.remove("banana")

        restored = ORSet.from_json(s.to_json())
        self.assertEqual(restored.value(), {"apple"})
        self.assertEqual(restored.replica_id, "A")
        self.assertEqual(restored.clock, s.clock)

    def test_serialization_preserves_tags(self):
        s = ORSet("node-1")
        s.add("item")
        s.remove("item") # remove to test both adds and removes

        raw = json.loads(s.to_json())
        self.assertIn("item", raw["adds"])
        self.assertIn("item", raw["removes"])

        tag_list = raw["adds"]["item"]
        self.assertEqual(len(tag_list), 1)
        self.assertEqual(tag_list[0], ["node-1", 1])

    # --- Merge ---

    def test_merge_disjoint_adds(self):
        a = ORSet("A")
        a.add("apple")

        b = ORSet("B")
        b.add("banana")

        a.merge(b)
        self.assertEqual(a.value(), {"apple", "banana"})

    def test_merge_same_element_both_replicas(self):
        a = ORSet("A")
        a.add("apple")

        b = ORSet("B")
        b.add("apple")

        a.merge(b)
        self.assertIn("apple", a.value())
        self.assertEqual(len(a.adds["apple"]), 2)

    def test_merge_idempotent(self):
        import copy
        a = ORSet("A")
        a.add("apple")

        b = ORSet("B")
        b.add("banana")

        a.merge(b)
        value_after_first = copy.deepcopy(a.value())

        a.merge(b)
        self.assertEqual(a.value(), value_after_first)

    def test_merge_commutative(self):
        import copy
        a = ORSet("A")
        a.add("apple")
        a.add("fig")

        b = ORSet("B")
        b.add("banana")
        b.remove("fig")

        a2 = copy.deepcopy(a)
        b2 = copy.deepcopy(b)

        a.merge(b)
        b2.merge(a2)

        self.assertEqual(a.value(), b2.value())

    # --- Convergence ---

    def test_concurrent_add_and_remove_add_wins(self):
        import copy
        a = ORSet("A")
        a.add("apple")

        b = copy.deepcopy(a)
        b.replica_id = "B"
        b.remove("apple")

        a.add("apple")

        a.merge(b)
        self.assertIn("apple", a.value())

    def test_sequential_add_then_remove_remove_wins(self):
        import copy
        a = ORSet("A")
        a.add("apple")

        b = ORSet("B")
        b.merge(copy.deepcopy(a))
        b.remove("apple")

        a.merge(b)
        self.assertNotIn("apple", a.value())

    def test_convergence_two_replicas(self):
        import copy
        a = ORSet("A")
        a.add("apple")
        a.add("banana")
        a.remove("banana")

        b = ORSet("B")
        b.add("cherry")
        b.add("apple")

        a_snap = copy.deepcopy(a)
        b_snap = copy.deepcopy(b)

        a.merge(b_snap)
        b.merge(a_snap)

        self.assertEqual(a.value(), b.value())
        self.assertIn("apple", a.value())
        self.assertIn("cherry", a.value())
        self.assertNotIn("banana", a.value())

    def test_convergence_three_replicas(self):
        import copy
        r1 = ORSet("r1")
        r1.add("x")
        r1.add("y")

        r2 = ORSet("r2")
        r2.add("y")
        r2.add("z")
        r2.remove("y")

        r3 = ORSet("r3")
        r3.add("x")
        r3.add("w")

        snapshots = [copy.deepcopy(r1), copy.deepcopy(r2), copy.deepcopy(r3)]
        for replica in [r1, r2, r3]:
            for snap in snapshots:
                replica.merge(snap)

        self.assertEqual(r1.value(), r2.value())
        self.assertEqual(r2.value(), r3.value())
        self.assertIn("x", r1.value())
        self.assertIn("y", r1.value())
        self.assertIn("z", r1.value())
        self.assertIn("w", r1.value())

    def test_no_tag_collision_after_merge(self):
        import copy
        a = ORSet("A")
        a.add("x")
        a.add("y")

        a_remote = copy.deepcopy(a)
        a_remote.add("z")

        a_fresh = ORSet("A")
        a_fresh.merge(a_remote)

        tag = a_fresh.add("w")
        self.assertGreater(tag[1], 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)