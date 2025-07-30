defmodule LWWEWSetTest do
  use ExUnit.Case, async: true
  doctest LWWEWSet

  test "new/0 creates an empty LWWEWSet" do
    assert LWWEWSet.new() == %{adds: %{}, removes: %{}}
  end

  test "add/4 adds an element with timestamp and node_id" do
    set = LWWEWSet.new()
    set = LWWEWSet.add(set, "apple", 100, "nodeA")
    assert set.adds == %{"apple" => MapSet.new([{100, "nodeA"}])}

    set = LWWEWSet.add(set, "apple", 200, "nodeB")
    assert set.adds == %{"apple" => MapSet.new([{100, "nodeA"}, {200, "nodeB"}])}

    set = LWWEWSet.add(set, "banana", 150, "nodeA")
    assert set.adds == %{
             "apple" => MapSet.new([{100, "nodeA"}, {200, "nodeB"}]),
             "banana" => MapSet.new([{150, "nodeA"}])
           }
  end

  test "remove/4 removes an element with timestamp and node_id" do
    set = LWWEWSet.new()
    set = LWWEWSet.remove(set, "apple", 100, "nodeA")
    assert set.removes == %{"apple" => MapSet.new([{100, "nodeA"}])}

    set = LWWEWSet.remove(set, "apple", 200, "nodeB")
    assert set.removes == %{"apple" => MapSet.new([{100, "nodeA"}, {200, "nodeB"}])}
  end

  test "value/1 returns elements where latest add timestamp is greater than latest remove timestamp" do
    set = LWWEWSet.new()
    set = LWWEWSet.add(set, "apple", 100, "nodeA") # Add apple
    assert LWWEWSet.value(set) == MapSet.new(["apple"])

    set = LWWEWSet.remove(set, "apple", 50, "nodeB") # Remove with older timestamp
    assert LWWEWSet.value(set) == MapSet.new(["apple"])

    set = LWWEWSet.remove(set, "apple", 150, "nodeC") # Remove with newer timestamp
    assert LWWEWSet.value(set) == MapSet.new([])

    set = LWWEWSet.add(set, "apple", 200, "nodeD") # Add again with newer timestamp
    assert LWWEWSet.value(set) == MapSet.new(["apple"])
  end

  test "value/1 resolves conflicts with 'add wins' tie-breaking" do
    set = LWWEWSet.new()
    set = LWWEWSet.add(set, "apple", 100, "nodeA")
    set = LWWEWSet.remove(set, "apple", 100, "nodeB") # Same timestamp, add wins
    assert LWWEWSet.value(set) == MapSet.new(["apple"])
  end

  test "merge/2 merges adds and removes correctly" do
    set1 = LWWEWSet.new()
    set1 = LWWEWSet.add(set1, "apple", 100, "nodeA")
    set1 = LWWEWSet.remove(set1, "apple", 50, "nodeA")
    set1 = LWWEWSet.add(set1, "banana", 120, "nodeB")

    set2 = LWWEWSet.new()
    set2 = LWWEWSet.add(set2, "apple", 150, "nodeC")
    set2 = LWWEWSet.remove(set2, "banana", 130, "nodeD")
    set2 = LWWEWSet.add(set2, "orange", 160, "nodeE")

    merged_set = LWWEWSet.merge(set1, set2)

    assert merged_set.adds == %{
             "apple" => MapSet.new([{100, "nodeA"}, {150, "nodeC"}]),
             "banana" => MapSet.new([{120, "nodeB"}]),
             "orange" => MapSet.new([{160, "nodeE"}])
           }

    assert merged_set.removes == %{
             "apple" => MapSet.new([{50, "nodeA"}]),
             "banana" => MapSet.new([{130, "nodeD"}])
           }

    assert LWWEWSet.value(merged_set) == MapSet.new(["apple", "orange"]) # apple (150 > 50), banana (120 < 130), orange (160 > no remove)
  end

  test "merge/2 is commutative" do
    set1 = LWWEWSet.new()
    set1 = LWWEWSet.add(set1, "apple", 100, "nodeA")
    set1 = LWWEWSet.remove(set1, "apple", 50, "nodeA")

    set2 = LWWEWSet.new()
    set2 = LWWEWSet.add(set2, "apple", 150, "nodeC")
    set2 = LWWEWSet.remove(set2, "banana", 130, "nodeD")

    merge1 = LWWEWSet.merge(set1, set2)
    merge2 = LWWEWSet.merge(set2, set1)

    assert merge1 == merge2
  end
end