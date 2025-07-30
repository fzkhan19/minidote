defmodule PositiveNegativeCounter do
  @behaviour ConflictFreeReplicatedDataType

  # This module implements the PositiveNegativeCounter CRDT.
  # It uses two internal maps (vector clocks) to track increments and decrements.

  @moduledoc """
  An operation-based PN-Counter CRDT.
  This implementation uses two vector clocks, one for increments (P) and one for decrements (N).
  """

  @type internal_state :: {map, map}

  @doc """
  Initializes a new PN-Counter with empty positive and negative vector clocks.
  """
  def new() do
    {%{}, %{}}
  end

  @doc """
  Retrieves the current value of the PN-Counter by subtracting the sum of the negative vector clock from the sum of the positive vector clock.
  """
  def retrieve_value({positive_vc, negative_vc}) do
    positive_sum = Enum.reduce(positive_vc, 0, fn {_, count}, acc -> acc + count end)
    negative_sum = Enum.reduce(negative_vc, 0, fn {_, count}, acc -> acc + count end)
    positive_sum - negative_sum
  end

  @doc """
  Generates an increment effect with the given value.
  """
  def generate_effect({:increment, value}, _state) do
    {:ok, {:inc_effect, value}}
  end

  @doc """
  Generates a decrement effect with the given value.
  """
  def generate_effect({:decrement, value}, _state) do
    {:ok, {:dec_effect, value}}
  end

  @doc """
  Applies an increment effect to the PN-Counter by incrementing the positive vector clock.
  """
  def apply_effect({{:inc_effect, value}, node_id}, {positive_vc, negative_vc}) do
    updated_positive_vc = Enum.reduce(1..value, positive_vc, fn _, acc -> Vector_Clock.increment(acc, node_id) end)
    {:ok, {updated_positive_vc, negative_vc}}
  end

  @doc """
  Applies a decrement effect to the PN-Counter by incrementing the negative vector clock.
  """
  def apply_effect({{:dec_effect, value}, node_id}, {positive_vc, negative_vc}) do
    updated_negative_vc = Enum.reduce(1..value, negative_vc, fn _, acc -> Vector_Clock.increment(acc, node_id) end)
    {:ok, {positive_vc, updated_negative_vc}}
  end

  @doc """
  Indicates that this CRDT does not require state for effect generation.
  """
  def requires_state_for_effect(_operation_payload) do
    false
  end

  @doc """
  Checks if two PN-Counter states are equal.
  """
  def are_equal(state_a, state_b) do
    state_a == state_b
  end

end
