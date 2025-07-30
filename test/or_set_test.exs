defmodule ORSetTest do
  use ExUnit.Case, async: true
  doctest ORSet

  test "new/0 creates an empty ORSet" do
    assert ORSet.new() == %{elements: MapSet.new(), tombstones: MapSet.new()}
  end

  test "add/3 adds an element with a unique tag" do
    or_set = ORSet.new()
    or_set = ORSet.add(or_set, "apple", "tag1")
    assert or_set.elements == MapSet.new([{"apple", "tag1"}])
    assert or_set.tombstones == MapSet.new()

    or_set = ORSet.add(or_set, "banana", "tag2")
    assert or_set.elements == MapSet.new([{"apple", "tag1"}, {"banana", "tag2"}])

    # Adding the same element with a different tag
    or_set = ORSet.add(or_set, "apple", "tag3")
    assert or_set.elements == MapSet.new([{"apple", "tag1"}, {"banana", "tag2"}, {"apple", "tag3"}])
  end

  test "remove/2 marks all instances of an element as removed by their tags" do
    or_set = ORSet.new()
    or_set = ORSet.add(or_set, "apple", "tag1")
    or_set = ORSet.add(or_set, "banana", "tag2")
    or_set = ORSet.add(or_set, "apple", "tag3")

    or_set = ORSet.remove(or_set, "apple")
    assert or_set.elements == MapSet.new([{"apple", "tag1"}, {"banana", "tag2"}, {"apple", "tag3"}])
    assert or_set.tombstones == MapSet.new(["tag1", "tag3"])

    # Removing an element not in the set
    or_set = ORSet.remove(or_set, "grape")
    assert or_set.tombstones == MapSet.new(["tag1", "tag3"])
  end

  test "value/1 returns the current set of elements, excluding those with tombstone tags" do
    or_set = ORSet.new()
    or_set = ORSet.add(or_set, "apple", "tag1")
    or_set = ORSet.add(or_set, "banana", "tag2")
    or_set = ORSet.add(or_set, "apple", "tag3")

    assert ORSet.value(or_set) == MapSet.new(["apple", "banana"])

    or_set = ORSet.remove(or_set, "apple")
    assert ORSet.value(or_set) == MapSet.new(["banana"])

    or_set = ORSet.remove(or_set, "banana")
    assert ORSet.value(or_set) == MapSet.new([])
  end

  test "merge/2 merges two ORSets correctly" do
    or_set1 = ORSet.new()
    or_set1 = ORSet.add(or_set1, "apple", "tag1")
    or_set1 = ORSet.add(or_set1, "banana", "tag2")
    or_set1 = ORSet.remove(or_set1, "apple") # tag1 is in tombstones

    or_set2 = ORSet.new()
    or_set2 = ORSet.add(or_set2, "apple", "tag3")
    or_set2 = ORSet.add(or_set2, "orange", "tag4")
    or_set2 = ORSet.add(or_set2, "banana", "tag2") # Add banana with tag2
    or_set2 = ORSet.remove(or_set2, "banana") # Now tag2 will be in tombstones

    merged_or_set = ORSet.merge(or_set1, or_set2)

    expected_elements = MapSet.new([{"apple", "tag1"}, {"banana", "tag2"}, {"apple", "tag3"}, {"orange", "tag4"}])
    expected_tombstones = MapSet.new(["tag1", "tag2"])

    assert merged_or_set.elements == expected_elements
    assert merged_or_set.tombstones == expected_tombstones
    assert ORSet.value(merged_or_set) == MapSet.new(["apple", "orange"])

    # Test merging with empty ORSet
    merged_with_empty = ORSet.merge(or_set1, ORSet.new())
    assert merged_with_empty == or_set1

    merged_empty_with_or_set = ORSet.merge(ORSet.new(), or_set2)
    assert merged_empty_with_or_set == or_set2
  end

  test "merge/2 is commutative" do
    or_set1 = ORSet.new()
    or_set1 = ORSet.add(or_set1, "apple", "tag1")
    or_set1 = ORSet.add(or_set1, "banana", "tag2")
    or_set1 = ORSet.remove(or_set1, "apple")

    or_set2 = ORSet.new()
    or_set2 = ORSet.add(or_set2, "apple", "tag3")
    or_set2 = ORSet.add(or_set2, "orange", "tag4")
    or_set2 = ORSet.remove(or_set2, "banana")

    merge1 = ORSet.merge(or_set1, or_set2)
    merge2 = ORSet.merge(or_set2, or_set1)

    assert merge1 == merge2
  end

  test "apply_effect/2 adds an element with a tag" do
    or_set = ORSet.new()
    {:ok, or_set} = ORSet.apply_effect({{:add, "apple", "tag1"}, :node1}, or_set)
    assert or_set.elements == MapSet.new([{"apple", "tag1"}])
  end

  test "apply_effect/2 removes elements by adding tags to tombstones" do
    or_set = ORSet.new()
    or_set = ORSet.add(or_set, "apple", "tag1")
    or_set = ORSet.add(or_set, "banana", "tag2")
    or_set = ORSet.add(or_set, "apple", "tag3")
    {:ok, or_set} = ORSet.apply_effect({{:remove, MapSet.new(["tag1", "tag3"])}, :node1}, or_set)
    assert or_set.tombstones == MapSet.new(["tag1", "tag3"])
    assert ORSet.value(or_set) == MapSet.new(["banana"])
  end

  test "retrieve_value/1 returns the current set of elements" do
    or_set = ORSet.new()
    or_set = ORSet.add(or_set, "apple", "tag1")
    assert ORSet.retrieve_value(or_set) == MapSet.new(["apple"])
  end

  test "generate_effect/2 for add operation" do
    or_set = ORSet.new()
    {:ok, effect} = ORSet.generate_effect({:add, "apple", "tag1"}, or_set)
    assert effect == {:add, "apple", "tag1"}
  end

  test "generate_effect/2 for remove operation" do
    or_set = ORSet.new()
    or_set = ORSet.add(or_set, "apple", "tag1")
    or_set = ORSet.add(or_set, "banana", "tag2")
    {:ok, effect} = ORSet.generate_effect({:remove, "apple"}, or_set)
    assert effect == {:remove, MapSet.new(["tag1"])}
  end

  test "requires_state_for_effect/1 returns false for add operation" do
    assert ORSet.requires_state_for_effect({:add, "apple", "tag1"}) == false
  end

  test "requires_state_for_effect/1 returns true for remove operation" do
    assert ORSet.requires_state_for_effect({:remove, "apple"}) == true
  end

  test "are_equal/2 compares two ORSets" do
    or_set1 = ORSet.new()
    or_set1 = ORSet.add(or_set1, "apple", "tag1")
    or_set2 = ORSet.new()
    or_set2 = ORSet.add(or_set2, "apple", "tag1")
    assert ORSet.are_equal(or_set1, or_set2) == true

    or_set3 = ORSet.new()
    or_set3 = ORSet.add(or_set3, "banana", "tag2")
    assert ORSet.are_equal(or_set1, or_set3) == false
  end
end