defmodule Counter_PN_OB do
  @behaviour CRDT

  @moduledoc """
  An operation-based PN-Counter CRDT.
  This implementation uses two vector clocks, one for increments (P) and one for decrements (N).
  """

  @type t :: {map, map}

  def new() do
    {%{}, %{}}
  end

  def value({p_vc, n_vc}) do
    p_sum = Enum.reduce(p_vc, 0, fn {_, count}, acc -> acc + count end)
    n_sum = Enum.reduce(n_vc, 0, fn {_, count}, acc -> acc + count end)
    p_sum - n_sum
  end

  def downstream({:increment, amount}, _state) do
    {:ok, {:inc, amount}}
  end

  def downstream({:decrement, amount}, _state) do
    {:ok, {:dec, amount}}
  end

  def update({{:inc, amount}, node}, {p_vc, n_vc}) do
    new_p_vc = Enum.reduce(1..amount, p_vc, fn _, acc -> Vector_Clock.increment(acc, node) end)
    {:ok, {new_p_vc, n_vc}}
  end

  def update({{:dec, amount}, node}, {p_vc, n_vc}) do
    new_n_vc = Enum.reduce(1..amount, n_vc, fn _, acc -> Vector_Clock.increment(acc, node) end)
    {:ok, {p_vc, new_n_vc}}
  end

  def require_state_downstream(_update) do
    false
  end

  def equal(state1, state2) do
    state1 == state2
  end

  def update({:inc, amount}, {p_vc, n_vc}) do
    # Use current node when not provided
    node = node()
    new_p_vc = Enum.reduce(1..amount, p_vc, fn _, acc -> Vector_Clock.increment(acc, node) end)
    {:ok, {new_p_vc, n_vc}}
  end

  def update({:dec, amount}, {p_vc, n_vc}) do
    node = node()
    new_n_vc = Enum.reduce(1..amount, n_vc, fn _, acc -> Vector_Clock.increment(acc, node) end)
    {:ok, {p_vc, new_n_vc}}
  end

  # Keep existing functions for distributed operations
  def update({{:inc, amount}, node}, {p_vc, n_vc}) do
    new_p_vc = Enum.reduce(1..amount, p_vc, fn _, acc -> Vector_Clock.increment(acc, node) end)
    {:ok, {new_p_vc, n_vc}}
  end

  def update({{:dec, amount}, node}, {p_vc, n_vc}) do
    new_n_vc = Enum.reduce(1..amount, n_vc, fn _, acc -> Vector_Clock.increment(acc, node) end)
    {:ok, {p_vc, new_n_vc}}
  end
end
