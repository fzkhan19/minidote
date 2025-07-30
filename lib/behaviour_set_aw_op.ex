defmodule AddWinsSet do
  @behaviour ConflictFreeReplicatedDataType

  # This module implements the Add-Wins Observed-Remove Set CRDT.
  # In this implementation, adds always win over removes.

  @moduledoc """
  An operation-based Add-Wins Observed-Remove Set CRDT.
  """

  @type internal_state :: map()

  @doc """
  Initializes a new Add-Wins Set with an empty map as the internal state.
  """
  def new() do
    %{}
  end

  @doc """
  Retrieves the current value of the Add-Wins Set, which is a MapSet of the keys in the current state.
  """
  def retrieve_value(current_state) do
    Map.keys(current_state) |> MapSet.new()
  end

  @doc """
  Generates an add effect for the given item.
  """
  def generate_effect({:add_element, item_to_add}, _current_state) do
    {:ok, {:add_effect, item_to_add}}
  end

  @doc """
  Generates a remove effect for the given item.
  Returns an error if the item is not found in the current state.
  """
  def generate_effect({:remove_element, item_to_remove}, current_state) do
    if Map.has_key?(current_state, item_to_remove) do
      {:ok, {:remove_effect, {item_to_remove, Map.get(current_state, item_to_remove)}}}
    else
      {:error, :item_not_found}
    end
  end

  # Handle updates with explicit node information
  @doc """
  Applies an add effect to the Add-Wins Set.
  Increments the version clock for the item in the current state.
  """
  def apply_effect({{:add_effect, item}, node_id}, current_state) do
    version_clock = Map.get(current_state, item, Vector_Clock.new())
    {:ok, Map.put(current_state, item, Vector_Clock.increment(version_clock, node_id))}
  end

  @doc """
  Applies a remove effect to the Add-Wins Set.
  Removes the item from the current state only if the removal's version clock dominates the item's version clock.
  """
  def apply_effect({{:remove_effect, {item, removal_vc}}, _node_id}, current_state) do
    case Map.get(current_state, item) do
      nil ->
        # Item not present, nothing to remove
        {:ok, current_state}

      item_version_clock ->
        # Remove only if removal_vc dominates item_version_clock (causally after)
        if Vector_Clock.leq(item_version_clock, removal_vc) do
          {:ok, Map.delete(current_state, item)}
        else
          # Add wins - remove was concurrent/before add, so ignore the remove
          {:ok, current_state}
        end
    end
  end

  @doc """
  Applies an add effect to the Add-Wins Set (without explicit node ID).
  Increments the version clock for the item in the current state using the current node's ID.
  """
  def apply_effect({:add_effect, item}, current_state) do
    current_node = node()
    version_clock = Map.get(current_state, item, Vector_Clock.new())
    {:ok, Map.put(current_state, item, Vector_Clock.increment(version_clock, current_node))}
  end

  @doc """
  Applies a remove effect to the Add-Wins Set (without explicit node ID).
  Removes the item from the current state only if the removal's version clock dominates the item's version clock.
  """
  def apply_effect({:remove_effect, {item, removal_vc}}, current_state) do
    case Map.get(current_state, item) do
      nil ->
        # Item not present, nothing to remove
        {:ok, current_state}

      item_version_clock ->
        if Vector_Clock.leq(item_version_clock, removal_vc) do
          {:ok, Map.delete(current_state, item)}
        else
          {:ok, current_state}
        end
    end
  end

  @doc """
  Indicates that an add element operation does not require state for effect generation.
  """
  def requires_state_for_effect({:add_element, _}) do
    false
  end

  @doc """
  Indicates that a remove element operation requires state for effect generation.
  """
  def requires_state_for_effect({:remove_element, _}) do
    true
  end

  @doc """
  Checks if two Add-Wins Set states are equal.
  """
  def are_equal(state_one, state_two) do
    state_one == state_two
  end
end
