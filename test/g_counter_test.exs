defmodule GCounterTest do
  use ExUnit.Case, async: true
  doctest GCounter

  test "new/0 creates an empty GCounter" do
    assert GCounter.new() == %{}
  end

  test "update/2 increments the count for a given node" do
    counter = GCounter.new()
    counter = GCounter.update(counter, {"node1", 5})
    assert counter == %{"node1" => 5}

    counter = GCounter.update(counter, {"node1", 3})
    assert counter == %{"node1" => 8}

    counter = GCounter.update(counter, {"node2", 10})
    assert counter == %{"node1" => 8, "node2" => 10}
  end

  test "update/2 with non-positive increment does not change the counter" do
    counter = GCounter.new()
    counter = GCounter.update(counter, {"node1", 5})
    assert_raise FunctionClauseError, fn -> GCounter.update(counter, {"node1", -1}) end
  end

  test "value/1 returns the total value of the GCounter" do
    counter = GCounter.new()
    assert GCounter.value(counter) == 0

    counter = GCounter.update(counter, {"node1", 5})
    assert GCounter.value(counter) == 5

    counter = GCounter.update(counter, {"node2", 10})
    assert GCounter.value(counter) == 15

    counter = GCounter.update(counter, {"node1", 2})
    assert GCounter.value(counter) == 17
  end

  test "merge/2 merges two GCounter's correctly" do
    counter1 = GCounter.new()
    counter1 = GCounter.update(counter1, {"node1", 5})
    counter1 = GCounter.update(counter1, {"node2", 3})

    counter2 = GCounter.new()
    counter2 = GCounter.update(counter2, {"node1", 2})
    counter2 = GCounter.update(counter2, {"node2", 7})
    counter2 = GCounter.update(counter2, {"node3", 4})

    merged_counter = GCounter.merge(counter1, counter2)
    assert merged_counter == %{"node1" => 5, "node2" => 7, "node3" => 4}
    assert GCounter.value(merged_counter) == 16

    # Test merging with empty counter
    merged_with_empty = GCounter.merge(counter1, GCounter.new())
    assert merged_with_empty == counter1

    merged_empty_with_counter = GCounter.merge(GCounter.new(), counter2)
    assert merged_empty_with_counter == counter2
  end

  test "merge/2 is commutative" do
    counter1 = GCounter.new()
    counter1 = GCounter.update(counter1, {"node1", 5})
    counter1 = GCounter.update(counter1, {"node2", 3})

    counter2 = GCounter.new()
    counter2 = GCounter.update(counter2, {"node1", 2})
    counter2 = GCounter.update(counter2, {"node2", 7})
    counter2 = GCounter.update(counter2, {"node3", 4})

    merge1 = GCounter.merge(counter1, counter2)
    merge2 = GCounter.merge(counter2, counter1)

    assert merge1 == merge2
  end

  test "apply_effect/2 applies an increment effect from a sender node" do
    counter = GCounter.new()
    {:ok, counter} = GCounter.apply_effect({{:increment, 5}, :nodeA}, counter)
    assert counter == %{:nodeA => 5}

    {:ok, counter} = GCounter.apply_effect({{:increment, 3}, :nodeA}, counter)
    assert counter == %{:nodeA => 8}

    {:ok, counter} = GCounter.apply_effect({{:increment, 10}, :nodeB}, counter)
    assert counter == %{:nodeA => 8, :nodeB => 10}
  end
end