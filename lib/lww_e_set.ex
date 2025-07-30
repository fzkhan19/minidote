defmodule LWWEWSet do
  @behaviour ConflictFreeReplicatedDataType
  @moduledoc """
  `LWWEWSet` (Last-Write-Wins Element-Wise Set) is a Conflict-Free Replicated Data Type (CRDT)
  that allows elements to be added and removed. It's similar to an `ORSet` but uses LWW semantics
  for concurrent additions and removals of the same element. Each element is stored with a set of
  (timestamp, node_id) pairs for additions and a similar set for removals.

  An element is considered present in the set if its latest addition timestamp is greater than
  its latest removal timestamp. If timestamps are equal, the addition wins. This CRDT provides
  a strong form of eventual consistency for sets where both adds and removes are significant.
  """

  @type t :: %{adds: %{any() => MapSet.t()}, removes: %{any() => MapSet.t()}}
  @type element :: any()
  @type tag :: {non_neg_integer(), node()} # {timestamp, node_id}
  @type operation_payload :: {:add_element, element(), non_neg_integer()} | {:remove_element, element(), non_neg_integer()}
  @type propagation_effect :: {:add_element, element(), tag()} | {:remove_element, element(), tag()}

  @impl ConflictFreeReplicatedDataType
  @doc """
  Creates a new, empty `LWWEWSet`.

  The initial state consists of two empty maps: `adds` and `removes`.
  `adds` stores elements mapped to a set of tags (timestamp, node_id) representing additions.
  `removes` stores elements mapped to a set of tags (timestamp, node_id) representing removals.

  ## Returns
    - `t()`: A new LWWEWSet with empty `adds` and `removes` maps.
  """
  @spec new() :: t()
  def new do
    %{adds: %{}, removes: %{}}
  end

  @doc """
  Adds an element to the `LWWEWSet` with a given timestamp and node ID.

  This function records an addition of an element by associating it with a unique
  tag (timestamp and node ID). It updates the `adds` map of the set.

  ## Parameters
    - `set`: The current state of the `LWWEWSet` (`t()`).
    - `element`: The element to add.
    - `timestamp`: The timestamp of the addition.
    - `node_id`: The ID of the node where the addition occurred.

  ## Returns
    - `t()`: The updated LWWEWSet state.
  """
  @spec add(t(), element(), non_neg_integer(), node()) :: t()
  def add(%{adds: adds} = set, element, timestamp, node_id) do
    new_adds = Map.update(adds, element, MapSet.new([{timestamp, node_id}]), fn existing ->
      MapSet.put(existing, {timestamp, node_id})
    end)
    %{set | adds: new_adds}
  end

  @doc """
  Removes an element from the `LWWEWSet` with a given timestamp and node ID.

  This function records a removal of an element by associating it with a unique
  tag (timestamp and node ID). It updates the `removes` map of the set.

  ## Parameters
    - `set`: The current state of the `LWWEWSet` (`t()`).
    - `element`: The element to remove.
    - `timestamp`: The timestamp of the removal.
    - `node_id`: The ID of the node where the removal occurred.

  ## Returns
    - `t()`: The updated LWWEWSet state.
  """
  @spec remove(t(), element(), non_neg_integer(), node()) :: t()
  def remove(%{removes: removes} = set, element, timestamp, node_id) do
    new_removes = Map.update(removes, element, MapSet.new([{timestamp, node_id}]), fn existing ->
      MapSet.put(existing, {timestamp, node_id})
    end)
    %{set | removes: new_removes}
  end

  @doc """
  Returns the current set of elements in the `LWWEWSet`.

  An element is considered present if its latest addition timestamp is greater than
  its latest removal timestamp. If timestamps are equal, the addition wins.

  ## Parameters
    - `set`: The current state of the `LWWEWSet` (`t()`).

  ## Returns
    - `MapSet.t()`: A MapSet of the elements currently observed in the set.
  """
  @spec value(t()) :: MapSet.t()
  def value(%{adds: adds, removes: removes}) do
    adds
    |> Enum.filter(fn {element, add_tags} ->
      latest_add_tag =
        add_tags
        |> Enum.max_by(fn {ts, _node} -> ts end)

      case Map.fetch(removes, element) do
        {:ok, remove_tags} ->
          latest_remove_tag =
            remove_tags
            |> Enum.max_by(fn {ts, _node} -> ts end)

          # If latest add timestamp is greater than latest remove timestamp, or
          # if timestamps are equal and (arbitrarily) add wins
          elem(latest_add_tag, 0) >= elem(latest_remove_tag, 0)
        :error ->
          # No remove tags for this element, so it's present
          true
      end
    end)
    |> Enum.map(fn {element, _tags} -> element end)
    |> MapSet.new()
  end

  @doc """
  Merges two `LWWEWSet`s.

  For each element, the `adds` are the union of both sets of add tags.
  The `removes` are the union of both sets of remove tags. This ensures that
  all historical additions and removals are preserved during the merge.

  ## Parameters
    - `set1`: The first `LWWEWSet` state (`t()`).
    - `set2`: The second `LWWEWSet` state (`t()`).

  ## Returns
    - `t()`: The merged `LWWEWSet` state.
  """
  @spec merge(t(), t()) :: t()
  def merge(set1, set2) do
    merged_adds = Map.merge(set1.adds, set2.adds, fn _k, v1, v2 -> MapSet.union(v1, v2) end)
    merged_removes = Map.merge(set1.removes, set2.removes, fn _k, v1, v2 -> MapSet.union(v1, v2) end)
    %{adds: merged_adds, removes: merged_removes}
  end

  @impl ConflictFreeReplicatedDataType
  @doc """
  Applies a propagation effect to the `LWWEWSet`.

  This function handles both `:add_element` and `:remove_element` effects.
  It delegates to the internal `add/4` and `remove/4` functions to update
  the `adds` and `removes` maps respectively.

  ## Parameters
    - `effect_tuple`: A tuple `{{effect_type, element, timestamp}, node_id}` representing the propagation effect.
      - `effect_type`: Either `:add_element` or `:remove_element`.
      - `element`: The element concerned by the effect.
      - `timestamp`: The timestamp associated with the effect.
      - `node_id`: The ID of the node that generated the effect.
    - `set`: The current state of the `LWWEWSet` (`t()`).

  ## Returns
    - `{:ok, t()}`: The updated LWWEWSet state.
  """
  @spec apply_effect({propagation_effect(), node()}, t()) :: {:ok, t()}
  def apply_effect({{:add_element, element, timestamp}, node_id}, set) do
    {:ok, add(set, element, timestamp, node_id)}
  end

  def apply_effect({{:remove_element, element, timestamp}, node_id}, set) do
    {:ok, remove(set, element, timestamp, node_id)}
  end

  @impl ConflictFreeReplicatedDataType
  @doc """
  Retrieves the current set of elements in the `LWWEWSet`.

  This function delegates to the internal `value/1` function to calculate
  the observable set of elements based on the Last-Write-Wins rule.

  ## Parameters
    - `set`: The current state of the `LWWEWSet` (`t()`).

  ## Returns
    - `MapSet.t()`: A MapSet of the elements currently observed in the set.
  """
  @spec retrieve_value(t()) :: MapSet.t()
  def retrieve_value(set) do
    value(set)
  end

  @impl ConflictFreeReplicatedDataType
  @doc """
  Generates a propagation effect for an operation on the `LWWEWSet`.

  For both `:add_element` and `:remove_element` operations, the effect
  includes the element and the timestamp. The `node()` is implicitly
  added during `apply_effect`.

  ## Parameters
    - `operation_payload`: The operation to perform.
      - `{:add_element, element, timestamp}`: Add operation.
      - `{:remove_element, element, timestamp}`: Remove operation.
    - `_set`: The current state of the `LWWEWSet` (not used for effect generation).

  ## Returns
    - `{:ok, propagation_effect()}`: A tuple containing the generated effect.
  """
  @spec generate_effect(operation_payload(), t()) :: {:ok, propagation_effect()}
  def generate_effect({:add_element, element, timestamp}, _set) do
    {:ok, {:add_element, element, timestamp}}
  end

  def generate_effect({:remove_element, element, timestamp}, _set) do
    {:ok, {:remove_element, element, timestamp}}
  end

  @impl ConflictFreeReplicatedDataType
  @doc """
  Indicates whether the CRDT requires its current state to generate an effect.

  For the `LWWEWSet`, generating an effect for either an add or remove operation
  does not require the current state of the set. The effect is simply the element
  and its associated timestamp.

  ## Parameters
    - `operation_payload`: The operation payload.

  ## Returns
    - `boolean()`: `true` because the state is not required for effect generation.
  """
  @spec requires_state_for_effect(operation_payload()) :: boolean()
  def requires_state_for_effect(_operation_payload) do
    false
  end

  @impl ConflictFreeReplicatedDataType
  @doc """
  Checks if two `LWWEWSet` states are equal.

  Equality is determined by a direct comparison of their internal `adds` and `removes` maps.

  ## Parameters
    - `set1`: The first `LWWEWSet` state (`t()`).
    - `set2`: The second `LWWEWSet` state (`t()`).

  ## Returns
    - `boolean()`: `true` if the states are identical, `false` otherwise.
  """
  @spec are_equal(t(), t()) :: boolean()
  def are_equal(set1, set2) do
    set1 == set2
  end
end
