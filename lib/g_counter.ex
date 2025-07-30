defmodule GCounter do
  @behaviour ConflictFreeReplicatedDataType

  @moduledoc """
  Implements a `GCounter` (Grow-only Counter) CRDT.

  A `GCounter` is a Conflict-Free Replicated Data Type (CRDT) that allows only increment operations.
  It is represented as a map where keys are node IDs (representing different replicas) and values
  are the counts from that node. This ensures that the counter can only grow, and conflicts are
  resolved by taking the maximum value for each node.
  """

  @doc """
  @doc """
  Creates a new, empty `GCounter`.

  The initial state is an empty map, representing that no increments have been made from any node.
  """
  @impl ConflictFreeReplicatedDataType
  def new() do
    %{}
  end

  @doc """
  @doc """
  Updates the `GCounter` by incrementing the count for a given node.

  This function takes the current counter state, a node ID, and an increment value.
  It updates the counter by adding the increment to the count associated with the given node ID.
  If the node ID is not already present in the counter, it adds the node ID with the given increment.

  The increment value must be a non-negative integer.
  """
  def update(counter, {node_id, increment}) when is_integer(increment) and increment >= 0 do
    Map.update(counter, node_id, increment, fn current_count -> current_count + increment end)
  end

  @doc """
  @doc """
  Returns the total value of the `GCounter`.

  This function calculates the sum of all counts from all nodes in the counter.
  """
  def value(counter) do
    Enum.reduce(Map.values(counter), 0, fn count, acc -> acc + count end)
  end

  @doc """
  @doc """
  Merges two `GCounter`s.

  This function merges two `GCounter` states by taking the maximum count for each node.
  If a node exists in both counters, the resulting counter will have the maximum value
  from either counter for that node. If a node only exists in one counter, it will be
  included in the resulting counter with its corresponding value.
  """
  def merge(counter1, counter2) do
    Map.merge(counter1, counter2, fn _key, v1, v2 -> max(v1, v2) end)
  end
  @impl ConflictFreeReplicatedDataType
  @doc """
  Applies an increment effect to the `GCounter`.

  This function takes the current counter state, an increment value, and the ID of the node
  that initiated the increment. It then updates the counter state by adding the increment
  to the count associated with the given node ID.

  The increment value must be a non-negative integer.
  """
  def apply_effect({{:increment, increment}, sender_node}, counter) when is_integer(increment) and increment >= 0 do
    new_counter = Map.update(counter, sender_node, increment, fn current_count -> current_count + increment end)
    {:ok, new_counter}
  end

  @impl ConflictFreeReplicatedDataType
  @doc """
  Retrieves the current value of the `GCounter`.

  This function simply calls the `value/1` function to calculate the total value of the counter.
  """
  def retrieve_value(counter) do
    value(counter)
  end

  @impl ConflictFreeReplicatedDataType
  @doc """
  Generates an increment effect.

  This function takes an increment value and returns it wrapped in an `{:increment, value}` tuple.
  """
  def generate_effect({:increment, value}, _counter) do
    {:ok, {:increment, value}}
  end

  @impl ConflictFreeReplicatedDataType
  @doc """
  Indicates that the `GCounter` does not require state for effect generation.

  This function always returns `false`, as the `GCounter`'s increment operation does not
  depend on the current state of the counter.
  """
  def requires_state_for_effect({:increment, _value}) do
    false
  end

  @impl ConflictFreeReplicatedDataType
  @doc """
  Checks if two `GCounter` states are equal.

  This function simply compares the two counter states for equality.
  """
  def are_equal(counter1, counter2) do
    counter1 == counter2
  end
end