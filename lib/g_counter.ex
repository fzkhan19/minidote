defmodule GCounter do
  @behaviour ConflictFreeReplicatedDataType

  @moduledoc """
  Implements a `GCounter` (Grow-only Counter) Conflict-Free Replicated Data Type.

  A `GCounter` is a CRDT that only allows increment operations. It is represented
  as a map where keys are node IDs (representing different replicas) and values
  are the counts contributed by that node. This ensures that the counter can only
  grow, and conflicts are resolved by taking the sum of all individual node counts.
  """

  @type t :: %{node() => non_neg_integer()}
  @type operation_payload :: {:increment, non_neg_integer()}
  @type propagation_effect :: {:increment, non_neg_integer()}

  @doc """
  Creates a new, empty `GCounter`.

  The initial state is an empty map, representing that no increments have been
  made from any node.

  ## Returns
    - `t()`: An empty map representing a new GCounter.
  """
  @impl ConflictFreeReplicatedDataType
  @spec new() :: t()
  def new() do
    %{}
  end

  @doc """
  Retrieves the total value of the `GCounter`.

  This function calculates the sum of all counts from all nodes in the counter.

  ## Parameters
    - `counter`: The internal state of the GCounter (`t()`).

  ## Returns
    - `non_neg_integer()`: The total numerical value of the counter.
  """
  @impl ConflictFreeReplicatedDataType
  @spec retrieve_value(t()) :: non_neg_integer()
  def retrieve_value(counter) do
    Enum.reduce(Map.values(counter), 0, fn count, acc -> acc + count end)
  end

  @doc """
  Applies an increment effect to the `GCounter`.

  This function takes the current counter state, an increment value, and the ID of the node
  that initiated the increment. It then updates the counter state by adding the increment
  to the count associated with the given node ID. If the node is not yet present, it's
  initialized with the increment value.

  ## Parameters
    - `{{:increment, increment}, sender_node}`: The effect and the node that sent it.
      - `increment`: The non-negative integer value to increment by.
      - `sender_node`: The node from which the increment originated.
    - `counter`: The current internal state of the GCounter (`t()`).

  ## Returns
    - `{:ok, t()}`: The updated internal state after applying the increment.
  """
  @impl ConflictFreeReplicatedDataType
  @spec apply_effect({propagation_effect(), node()}, t()) :: {:ok, t()}
  def apply_effect({{:increment, increment}, sender_node}, counter) when is_integer(increment) and increment >= 0 do
    new_counter = Map.update(counter, sender_node, increment, fn current_count -> current_count + increment end)
    {:ok, new_counter}
  end

  @doc """
  Generates an increment effect.

  This function takes an increment value and returns it wrapped in an `{:increment, value}` tuple.
  The effect is simply the increment value itself, as G-Counters only support increments.

  ## Parameters
    - `{:increment, value}`: The increment operation, where `value` is the amount to increment by.
    - `_counter`: The current internal state of the counter (not used for effect generation).

  ## Returns
    - `{:ok, propagation_effect()}`: A tuple containing the generated increment effect.
  """
  @impl ConflictFreeReplicatedDataType
  @spec generate_effect(operation_payload(), t()) :: {:ok, propagation_effect()}
  def generate_effect({:increment, value}, _counter) do
    {:ok, {:increment, value}}
  end

  @doc """
  Indicates that the `GCounter` does not require state for effect generation.

  This function always returns `false`, as the `GCounter`'s increment operation does not
  depend on the current state of the counter. The effect is simply the value to add.

  ## Parameters
    - `_operation_payload`: The operation payload (not used).

  ## Returns
    - `boolean()`: `false` because the state is not required for effect generation.
  """
  @impl ConflictFreeReplicatedDataType
  @spec requires_state_for_effect(operation_payload()) :: boolean()
  def requires_state_for_effect({:increment, _value}) do
    false
  end

  @doc """
  Checks if two `GCounter` states are equal.

  Equality is determined by a direct comparison of their internal map representations.

  ## Parameters
    - `counter1`: The first GCounter internal state.
    - `counter2`: The second GCounter internal state.

  ## Returns
    - `boolean()`: `true` if the states are identical, `false` otherwise.
  """
  @impl ConflictFreeReplicatedDataType
  @spec are_equal(t(), t()) :: boolean()
  def are_equal(counter1, counter2) do
    counter1 == counter2
  end
end
