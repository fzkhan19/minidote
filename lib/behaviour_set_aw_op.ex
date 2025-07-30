defmodule AddWinsSet do
  @behaviour ConflictFreeReplicatedDataType

  @moduledoc """
  An operation-based Add-Wins Observed-Remove Set (AW-Set) Conflict-Free Replicated Data Type.

  In an AW-Set, an element, once added, remains in the set unless all operations
  that added it are "undone" by a corresponding remove operation that causally
  succeeds the add. If an add and a remove operation for the same element are
  concurrent, the add operation "wins", meaning the element remains in the set.

  This implementation uses a map where keys are elements and values are vector clocks.
  The vector clock associated with an element represents the causal history of its
  addition. A remove operation includes the vector clock of the element at the time
  of removal, and an element is only truly removed if the removal's vector clock
  dominates the element's current vector clock.
  """

  @type internal_state :: %{any() => Vector_Clock.t()}
  @type operation_payload :: {:add_element, any()} | {:remove_element, any()}
  @type propagation_effect :: {:add_effect, any()} | {:remove_effect, {any(), Vector_Clock.t()}}

  @doc """
  Initializes a new Add-Wins Set with an empty map as the internal state.

  ## Returns
    - `internal_state()`: An empty map representing a new AW-Set.
  """
  @spec new() :: internal_state()
  def new() do
    %{}
  end

  @doc """
  Retrieves the current value of the Add-Wins Set.

  The value is represented as a `MapSet` containing all elements currently present
  in the internal state.

  ## Parameters
    - `current_state`: The internal state of the AW-Set.

  ## Returns
    - `MapSet.t()`: A MapSet of the elements in the set.
  """
  @spec retrieve_value(internal_state()) :: MapSet.t()
  def retrieve_value(current_state) do
    Map.keys(current_state) |> MapSet.new()
  end

  @doc """
  Generates a propagation effect for an `:add_element` operation.

  For an add operation, the effect simply contains the element to be added.

  ## Parameters
    - `{:add_element, item_to_add}`: The add operation and the element to add.
    - `_current_state`: The current internal state (not used for add effect generation).

  ## Returns
    - `{:ok, propagation_effect()}`: A tuple containing the generated add effect.
  """
  @spec generate_effect({:add_element, any()}, internal_state()) :: {:ok, propagation_effect()}
  def generate_effect({:add_element, item_to_add}, _current_state) do
    {:ok, {:add_effect, item_to_add}}
  end

  @doc """
  Generates a propagation effect for a `:remove_element` operation.

  For a remove operation, the effect includes both the element to be removed and
  its current associated vector clock from the local state. This vector clock
  is crucial for enforcing the "add-wins" semantic during effect application.

  ## Parameters
    - `{:remove_element, item_to_remove}`: The remove operation and the element to remove.
    - `current_state`: The current internal state of the set.

  ## Returns
    - `{:ok, propagation_effect()}`: A tuple containing the remove effect if the item is found.
    - `{:error, :item_not_found}`: If the item to remove is not present in the set.
  """
  @spec generate_effect({:remove_element, any()}, internal_state()) :: {:ok, propagation_effect()} | {:error, :item_not_found}
  def generate_effect({:remove_element, item_to_remove}, current_state) do
    if Map.has_key?(current_state, item_to_remove) do
      {:ok, {:remove_effect, {item_to_remove, Map.get(current_state, item_to_remove)}}}
    else
      {:error, :item_not_found}
    end
  end

  @doc """
  Applies a propagation effect to the Add-Wins Set's internal state.

  This function handles both `:add_effect` and `:remove_effect`.
  - For `:add_effect`, the element's vector clock is incremented for the originating node.
  - For `:remove_effect`, the element is removed only if the `removal_vc` (vector clock
    at the time of removal) causally dominates the element's current vector clock in the set.
    This ensures the "add-wins" semantic: a remove only succeeds if it causally succeeds
    all additions of that element.

  ## Parameters
    - `effect_tuple`: A tuple `{{effect_type, payload}, node_id}` representing the propagation effect.
      - `effect_type`: Either `:add_effect` or `:remove_effect`.
      - `payload`: The data associated with the effect (element for add, `{element, removal_vc}` for remove).
      - `node_id`: The ID of the node that generated the effect.
    - `current_state`: The current internal state of the AW-Set.

  ## Returns
    - `{:ok, internal_state()}`: The updated internal state after applying the effect.
  """
  @spec apply_effect({propagation_effect(), node()}, internal_state()) :: {:ok, internal_state()}
  def apply_effect({{:add_effect, item}, node_id}, current_state) do
    version_clock = Map.get(current_state, item, Vector_Clock.new())
    {:ok, Map.put(current_state, item, Vector_Clock.increment(version_clock, node_id))}
  end

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
  Indicates whether the CRDT requires its current state to generate an effect for a given operation.

  - For an `:add_element` operation, the state is not required because the effect
    only carries the element to be added.
  - For a `:remove_element` operation, the state is required because the effect
    needs to capture the current version clock of the element being removed to
    enforce the "add-wins" semantic.

  ## Parameters
    - `operation_payload`: The operation payload.

  ## Returns
    - `boolean()`: `true` if the state is required, `false` otherwise.
  """
  @spec requires_state_for_effect(operation_payload()) :: boolean()
  def requires_state_for_effect({:add_element, _}) do
    false
  end

  def requires_state_for_effect({:remove_element, _}) do
    true
  end

  @doc """
  Checks if two Add-Wins Set states are equal.

  Equality is determined by a direct comparison of their internal map representations.

  ## Parameters
    - `state_one`: The first AW-Set internal state.
    - `state_two`: The second AW-Set internal state.

  ## Returns
    - `boolean()`: `true` if the states are identical, `false` otherwise.
  """
  @spec are_equal(internal_state(), internal_state()) :: boolean()
  def are_equal(state_one, state_two) do
    state_one == state_two
  end
end
