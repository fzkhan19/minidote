defmodule ORSet do
  @behaviour ConflictFreeReplicatedDataType
  @moduledoc """
  `ORSet` (Observed-Remove Set) is a Conflict-Free Replicated Data Type (CRDT)
  that allows elements to be added and removed. It tracks additions with a unique
  "tag" for each element, and removals by keeping track of the tags of removed elements.

  An OR-Set ensures that an element, once added, remains in the set unless all
  occurrences of its tags are removed. This means that adding the same element
  multiple times (with different tags) will require multiple corresponding removes
  to fully eliminate it from the set. Concurrent add and remove operations for
  the same element are resolved by ensuring that the element remains in the set
  if any of its tags are still "active" (not marked as removed).
  """

  @type t :: %{elements: MapSet.t(), tombstones: MapSet.t()}
  @type element :: any()
  @type tag :: any() # Typically a unique identifier like {node(), reference()}
  @type operation_payload :: {:add, element(), tag()} | {:remove, element()}
  @type propagation_effect :: {:add, element(), tag()} | {:remove, MapSet.t()}

  @impl ConflictFreeReplicatedDataType
  @doc """
  Creates a new, empty `ORSet`.

  ## Returns
    - `t()`: A new ORSet with empty `elements` and `tombstones` MapSets.
  """
  @spec new() :: t()
  def new do
    %{elements: MapSet.new(), tombstones: MapSet.new()}
  end

  @doc """
  Adds an element to the `ORSet` with a unique tag.
  """
  # This function is not part of the behaviour, but is used by generate_effect.
  # Its documentation will be moved to comments to avoid compiler warnings.
  # @spec add(t(), element(), tag()) :: t()
  def add(%{elements: elements} = or_set, element, tag) do
    %{or_set | elements: MapSet.put(elements, {element, tag})}
  end

  @doc """
  Removes an element from the `ORSet`.
  This marks all instances of the element (identified by their tags) as removed.
  """
  # This function is not part of the behaviour, but its logic is used by generate_effect.
  # Its documentation will be moved to comments to avoid compiler warnings.
  # @spec remove(t(), element()) :: t()
  def remove(%{elements: elements, tombstones: tombstones} = or_set, element) do
    # Find all tags associated with the element that are currently in the set
    tags_to_remove =
      elements
      |> Enum.filter(fn {el, _tag} -> el == element end)
      |> Enum.map(fn {_el, tag} -> tag end)
      |> MapSet.new()

    %{or_set | tombstones: MapSet.union(tombstones, tags_to_remove)}
  end

  @doc """
  Returns the current set of elements in the `ORSet`.
  Elements whose tags are in the tombstones set are considered removed.
  """
  # This function is not part of the behaviour, but its logic is used by retrieve_value.
  # Its documentation will be moved to comments to avoid compiler warnings.
  # @spec value(t()) :: MapSet.t()
  def value(%{elements: elements, tombstones: tombstones}) do
    elements
    |> Enum.filter(fn {_element, tag} -> not MapSet.member?(tombstones, tag) end)
    |> Enum.map(fn {element, _tag} -> element end)
    |> MapSet.new()
  end

  @doc """
  Merges two `ORSet`s.
  The elements are the union of both sets.
  The tombstones are the union of both tombstone sets.
  """
  # This function is not part of the behaviour, and its logic is implicitly handled by the CRDT contract.
  # Its documentation will be moved to comments to avoid compiler warnings.
  # @spec merge(t(), t()) :: t()
  def merge(or_set1, or_set2) do
    elements = MapSet.union(or_set1.elements, or_set2.elements)
    tombstones = MapSet.union(or_set1.tombstones, or_set2.tombstones)
    %{elements: elements, tombstones: tombstones}
  end

  @impl ConflictFreeReplicatedDataType
  @doc """
  Applies a propagation effect to the `ORSet`.

  This function handles two types of effects:
  - `{:add, element, tag}`: Adds the element with its unique tag to the `elements` set.
  - `{:remove, tags_to_remove}`: Adds the specified tags to the `tombstones` set.

  ## Parameters
    - `effect_tuple`: A tuple representing the propagation effect.
      - `{:add, element, tag}`: The add effect.
      - `{:remove, tags_to_remove}`: The remove effect, containing a MapSet of tags to mark as removed.
    - `or_set`: The current state of the `ORSet` (`t()`).

  ## Returns
    - `{:ok, t()}`: The updated ORSet state.
  """
  @spec apply_effect({propagation_effect(), node()}, t()) :: {:ok, t()}
  def apply_effect({{:add, element, tag}, _sender_node}, %{elements: elements} = or_set) do
    {:ok, %{or_set | elements: MapSet.put(elements, {element, tag})}}
  end

  @impl ConflictFreeReplicatedDataType
  @doc """
  Applies a propagation effect to the `ORSet`.
  This is a second clause for `apply_effect/2` to handle remove effects.
  """
  @spec apply_effect({{:remove, MapSet.t()}, node()}, t()) :: {:ok, t()}
  def apply_effect({{:remove, tags_to_remove}, _sender_node}, %{tombstones: tombstones} = or_set) do
    {:ok, %{or_set | tombstones: MapSet.union(tombstones, tags_to_remove)}}
  end

  @impl ConflictFreeReplicatedDataType
  @doc """
  Retrieves the current set of elements in the `ORSet`.

  Elements whose tags are present in the `tombstones` set are considered removed
  and are filtered out from the `elements` set.

  ## Parameters
    - `or_set`: The current state of the `ORSet` (`t()`).

  ## Returns
    - `MapSet.t()`: A MapSet of the elements currently observed in the set.
  """
  @spec retrieve_value(t()) :: MapSet.t()
  def retrieve_value(or_set) do
    value(or_set)
  end

  @impl ConflictFreeReplicatedDataType
  @doc """
  Generates an add effect for the `ORSet`.

  For an add operation, the effect includes the element and a newly generated
  unique tag.

  ## Parameters
    - `{:add, element, tag}`: The add operation, including the element and its tag.
    - `_or_set`: The current state of the `ORSet` (not used for effect generation).

  ## Returns
    - `{:ok, propagation_effect()}`: A tuple containing the generated add effect.
  """
  @spec generate_effect({:add, element(), tag()}, t()) :: {:ok, propagation_effect()}
  def generate_effect({:add, element, tag}, _or_set) do
    {:ok, {:add, element, tag}}
  end

  @impl ConflictFreeReplicatedDataType
  @doc """
  Generates a remove effect for the `ORSet`.

  For a remove operation, the effect consists of a `MapSet` of all tags currently
  associated with the element being removed in the local `elements` set. These
  tags will be added to the `tombstones` set on all replicas.

  ## Parameters
    - `{:remove, element}`: The remove operation, specifying the element to remove.
    - `or_set`: The current state of the `ORSet` (`t()`).

  ## Returns
    - `{:ok, propagation_effect()}`: A tuple containing the generated remove effect.
  """
  @spec generate_effect({:remove, element()}, t()) :: {:ok, propagation_effect()}
  def generate_effect({:remove, element}, or_set) do
    # Find all tags associated with the element that are currently in the set
    tags_to_remove =
      or_set.elements
      |> Enum.filter(fn {el, _tag} -> el == element end)
      |> Enum.map(fn {_el, tag} -> tag end)
      |> MapSet.new()

    {:ok, {:remove, tags_to_remove}}
  end

  @impl ConflictFreeReplicatedDataType
  @doc """
  Indicates whether the CRDT requires its current state to generate an effect.

  - For an `:add` operation, the state is not required because the effect
    only carries the element and its tag.
  - For a `:remove` operation, the state is required because the effect
    needs to collect all active tags associated with the element being removed.

  ## Parameters
    - `operation_payload`: The operation payload.

  ## Returns
    - `boolean()`: `true` if the state is required, `false` otherwise.
  """
  @spec requires_state_for_effect(operation_payload()) :: boolean()
  def requires_state_for_effect({:add, _element, _tag}) do
    false
  end

  @impl ConflictFreeReplicatedDataType
  @doc """
  Indicates whether the CRDT requires its current state to generate an effect.
  This is a second clause for `requires_state_for_effect/1` to handle remove operations.
  """
  @spec requires_state_for_effect({:remove, element()}) :: boolean()
  def requires_state_for_effect({:remove, _element}) do
    true
  end

  @impl ConflictFreeReplicatedDataType
  @doc """
  Checks if two `ORSet` states are equal.

  Equality is determined by a direct comparison of their internal `elements`
  and `tombstones` MapSets.

  ## Parameters
    - `or_set1`: The first `ORSet` state (`t()`).
    - `or_set2`: The second `ORSet` state (`t()`).

  ## Returns
    - `boolean()`: `true` if the states are identical, `false` otherwise.
  """
  @spec are_equal(t(), t()) :: boolean()
  def are_equal(or_set1, or_set2) do
    or_set1 == or_set2
  end
end
