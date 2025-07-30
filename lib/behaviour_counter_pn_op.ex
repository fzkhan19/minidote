defmodule PositiveNegativeCounter do
  @behaviour ConflictFreeReplicatedDataType

  @moduledoc """
  An operation-based PN-Counter (Positive-Negative Counter) Conflict-Free Replicated Data Type.

  The PN-Counter allows for both increments and decrements, and ensures strong eventual
  consistency. It achieves this by maintaining two internal vector clocks:
  - `P` (positive_vc): A vector clock that tracks increments for each replica.
  - `N` (negative_vc): A vector clock that tracks decrements for each replica.

  The current value of the counter is derived by summing all values in `P` and
  subtracting the sum of all values in `N`. Operations (increments and decrements)
  generate effects that are propagated to other replicas. When an effect is applied,
  the corresponding vector clock (P or N) is updated for the originating replica.
  """

  @type internal_state :: {Vector_Clock.t(), Vector_Clock.t()}
  @type operation_payload :: {:increment, non_neg_integer()} | {:decrement, non_neg_integer()}
  @type propagation_effect :: {:inc_effect, non_neg_integer()} | {:dec_effect, non_neg_integer()}

  @doc """
  Initializes a new PN-Counter with empty positive and negative vector clocks.

  ## Returns
    - `internal_state()`: A tuple containing two empty maps, representing the initial
                          positive and negative vector clocks.
  """
  @spec new() :: internal_state()
  def new() do
    {%{}, %{}}
  end

  @doc """
  Retrieves the current value of the PN-Counter.

  The value is calculated by summing all increments (from the positive vector clock)
  and subtracting all decrements (from the negative vector clock).

  ## Parameters
    - `{positive_vc, negative_vc}`: The internal state of the PN-Counter,
                                   consisting of the positive and negative vector clocks.

  ## Returns
    - `integer()`: The current numerical value of the counter.
  """
  @spec retrieve_value(internal_state()) :: integer()
  def retrieve_value({positive_vc, negative_vc}) do
    positive_sum = Enum.reduce(positive_vc, 0, fn {_, count}, acc -> acc + count end)
    negative_sum = Enum.reduce(negative_vc, 0, fn {_, count}, acc -> acc + count end)
    positive_sum - negative_sum
  end

  @doc """
  Generates a propagation effect for an increment operation.

  This function creates an effect that can be applied locally or propagated
  to other replicas to increment the counter.

  ## Parameters
    - `{:increment, value}`: The increment operation, where `value` is the amount to increment by.
    - `_state`: The current internal state of the counter (not used for effect generation).

  ## Returns
    - `{:ok, propagation_effect()}`: A tuple containing the generated increment effect.
  """
  @spec generate_effect(operation_payload(), internal_state()) :: {:ok, propagation_effect()}
  def generate_effect({:increment, value}, _state) do
    {:ok, {:inc_effect, value}}
  end

  @doc """
  Generates a propagation effect for a decrement operation.

  This function creates an effect that can be applied locally or propagated
  to other replicas to decrement the counter.

  ## Parameters
    - `{:decrement, value}`: The decrement operation, where `value` is the amount to decrement by.
    - `_state`: The current internal state of the counter (not used for effect generation).

  ## Returns
    - `{:ok, propagation_effect()}`: A tuple containing the generated decrement effect.
  """
  @spec generate_effect(operation_payload(), internal_state()) :: {:ok, propagation_effect()}
  def generate_effect({:decrement, value}, _state) do
    {:ok, {:dec_effect, value}}
  end

  @doc """
  Applies an increment propagation effect to the PN-Counter's internal state.

  The `value` of the effect is added to the positive vector clock for the `node_id`
  from which the effect originated.

  ## Parameters
    - `{{:inc_effect, value}, node_id}`: The increment effect and the ID of the node
                                       that generated the effect.
    - `{positive_vc, negative_vc}`: The current internal state of the PN-Counter.

  ## Returns
    - `{:ok, internal_state()}`: The updated internal state after applying the increment.
  """
  @spec apply_effect({propagation_effect(), node()}, internal_state()) :: {:ok, internal_state()}
  def apply_effect({{:inc_effect, value}, node_id}, {positive_vc, negative_vc}) do
    updated_positive_vc = Enum.reduce(1..value, positive_vc, fn _, acc -> Vector_Clock.increment(acc, node_id) end)
    {:ok, {updated_positive_vc, negative_vc}}
  end

  @doc """
  Applies a decrement propagation effect to the PN-Counter's internal state.

  The `value` of the effect is added to the negative vector clock for the `node_id`
  from which the effect originated.

  ## Parameters
    - `{{:dec_effect, value}, node_id}`: The decrement effect and the ID of the node
                                       that generated the effect.
    - `{positive_vc, negative_vc}`: The current internal state of the PN-Counter.

  ## Returns
    - `{:ok, internal_state()}`: The updated internal state after applying the decrement.
  """
  @spec apply_effect({propagation_effect(), node()}, internal_state()) :: {:ok, internal_state()}
  def apply_effect({{:dec_effect, value}, node_id}, {positive_vc, negative_vc}) do
    updated_negative_vc = Enum.reduce(1..value, negative_vc, fn _, acc -> Vector_Clock.increment(acc, node_id) end)
    {:ok, {positive_vc, updated_negative_vc}}
  end

  @doc """
  Indicates whether the CRDT requires its current state to generate an effect.

  For an operation-based CRDT like PN-Counter, the current state is not strictly
  needed to generate the effect of an operation, as the effect itself captures
  the change.

  ## Parameters
    - `_operation_payload`: The operation payload (not used).

  ## Returns
    - `boolean()`: `false` because the state is not required for effect generation.
  """
  @spec requires_state_for_effect(operation_payload()) :: boolean()
  def requires_state_for_effect(_operation_payload) do
    false
  end

  @doc """
  Checks if two PN-Counter states are equal.

  Equality is determined by a direct comparison of their internal representations
  (the positive and negative vector clocks).

  ## Parameters
    - `state_a`: The first PN-Counter internal state.
    - `state_b`: The second PN-Counter internal state.

  ## Returns
    - `boolean()`: `true` if the states are identical, `false` otherwise.
  """
  @spec are_equal(internal_state(), internal_state()) :: boolean()
  def are_equal(state_a, state_b) do
    state_a == state_b
  end
end
