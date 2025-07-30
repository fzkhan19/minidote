defmodule GCounter do
  @behaviour ConflictFreeReplicatedDataType
  @moduledoc """
  `GCounter` (Grow-only Counter) is a CRDT that only allows increments.
  It is represented as a map where keys are node IDs and values are the counts
  from that node.
  """

  @doc """
  Creates a new, empty `GCounter`.
  """
  @impl ConflictFreeReplicatedDataType
  def new do
    %{}
  end

  @doc """
  Updates the `GCounter` by incrementing the count for a given node.
  """
  def update(counter, {node_id, increment}) when is_integer(increment) and increment >= 0 do
    Map.update(counter, node_id, increment, fn current_count -> current_count + increment end)
  end

  @doc """
  Returns the total value of the `GCounter`.
  """
  def value(counter) do
    Enum.reduce(Map.values(counter), 0, fn count, acc -> acc + count end)
  end

  @doc """
  Merges two `GCounter`s.
  For each node, the maximum count from either counter is taken.
  """
  def merge(counter1, counter2) do
    Map.merge(counter1, counter2, fn _key, v1, v2 -> max(v1, v2) end)
  end
  @impl ConflictFreeReplicatedDataType
  def apply_effect({{:increment, increment}, sender_node}, counter) when is_integer(increment) and increment >= 0 do
    new_counter = Map.update(counter, sender_node, increment, fn current_count -> current_count + increment end)
    {:ok, new_counter}
  end

  @impl ConflictFreeReplicatedDataType
  def retrieve_value(counter) do
    value(counter)
  end

  @impl ConflictFreeReplicatedDataType
  def generate_effect({:increment, value}, _counter) do
    {:ok, {:increment, value}}
  end

  @impl ConflictFreeReplicatedDataType
  def requires_state_for_effect({:increment, _value}) do
    false
  end

  @impl ConflictFreeReplicatedDataType
  def are_equal(counter1, counter2) do
    counter1 == counter2
  end
end