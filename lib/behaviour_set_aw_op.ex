defmodule Set_AW_OB do
  @behaviour CRDT

  @moduledoc """
  An operation-based Add-Wins Observed-Remove Set CRDT.
  """

  @type t :: map()

  def new() do
    %{}
  end

  def value(state) do
    Map.keys(state) |> MapSet.new()
  end

  def downstream({:add, element}, _state) do
    {:ok, {:add, element}}
  end

  def downstream({:remove, element}, state) do
    if Map.has_key?(state, element) do
      {:ok, {:rm, {element, Map.get(state, element)}}}
    else
      {:error, :element_not_found}
    end
  end

  # Handle updates with explicit node information
  def update({{:add, element}, node}, state) do
    vc = Map.get(state, element, Vector_Clock.new())
    {:ok, Map.put(state, element, Vector_Clock.increment(vc, node))}
  end

  def update({{:rm, {element, remove_vc}}, _node}, state) do
    case Map.get(state, element) do
      nil ->
        # Element not present
        {:ok, state}

      element_vc ->
        # Remove only if remove_vc dominates element_vc (causally after)
        if Vector_Clock.leq(element_vc, remove_vc) do
          {:ok, Map.delete(state, element)}
        else
          # Add wins - remove was concurrent/before add
          {:ok, state}
        end
    end
  end

  # Handle updates without explicit node (use current node)
  def update({:add, element}, state) do
    node = node()
    vc = Map.get(state, element, Vector_Clock.new())
    {:ok, Map.put(state, element, Vector_Clock.increment(vc, node))}
  end

  def update({:rm, {element, remove_vc}}, state) do
    case Map.get(state, element) do
      nil ->
        {:ok, state}

      element_vc ->
        if Vector_Clock.leq(element_vc, remove_vc) do
          {:ok, Map.delete(state, element)}
        else
          {:ok, state}
        end
    end
  end

  def require_state_downstream({:add, _}) do
    false
  end

  def require_state_downstream({:remove, _}) do
    true
  end

  def equal(state1, state2) do
    state1 == state2
  end
end
