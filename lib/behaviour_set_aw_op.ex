defmodule AddWinsSet do
  @behaviour ConflictFreeReplicatedDataType

  @moduledoc """
  An operation-based Add-Wins Observed-Remove Set CRDT.
  """

  @type internal_state :: map()

  def initialize() do
    %{}
  end

  def retrieve_value(current_state) do
    Map.keys(current_state) |> MapSet.new()
  end

  def generate_effect({:add_element, item_to_add}, _current_state) do
    {:ok, {:add_effect, item_to_add}}
  end

  def generate_effect({:remove_element, item_to_remove}, current_state) do
    if Map.has_key?(current_state, item_to_remove) do
      {:ok, {:remove_effect, {item_to_remove, Map.get(current_state, item_to_remove)}}}
    else
      {:error, :item_not_found}
    end
  end

  # Handle updates with explicit node information
  def apply_effect({{:add_effect, item}, node_id}, current_state) do
    version_clock = Map.get(current_state, item, Vector_Clock.new())
    {:ok, Map.put(current_state, item, Vector_Clock.increment(version_clock, node_id))}
  end

  def apply_effect({{:remove_effect, {item, removal_vc}}, _node_id}, current_state) do
    case Map.get(current_state, item) do
      nil ->
        # Item not present
        {:ok, current_state}

      item_version_clock ->
        # Remove only if removal_vc dominates item_version_clock (causally after)
        if Vector_Clock.leq(item_version_clock, removal_vc) do
          {:ok, Map.delete(current_state, item)}
        else
          # Add wins - remove was concurrent/before add
          {:ok, current_state}
        end
    end
  end

  # Handle updates without explicit node (use current node)
  def apply_effect({:add_effect, item}, current_state) do
    current_node = node()
    version_clock = Map.get(current_state, item, Vector_Clock.new())
    {:ok, Map.put(current_state, item, Vector_Clock.increment(version_clock, current_node))}
  end

  def apply_effect({:remove_effect, {item, removal_vc}}, current_state) do
    case Map.get(current_state, item) do
      nil ->
        {:ok, current_state}

      item_version_clock ->
        if Vector_Clock.leq(item_version_clock, removal_vc) do
          {:ok, Map.delete(current_state, item)}
        else
          {:ok, current_state}
        end
    end
  end

  def requires_state_for_effect({:add_element, _}) do
    false
  end

  def requires_state_for_effect({:remove_element, _}) do
    true
  end

  def are_equal(state_one, state_two) do
    state_one == state_two
  end
end
