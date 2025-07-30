defmodule ORSet do
  @behaviour ConflictFreeReplicatedDataType
  @moduledoc """
  `ORSet` (Observed-Remove Set) is a CRDT that allows elements to be added and removed.
  It tracks additions with a unique "tag" for each element, and removals by
  keeping track of the tags of removed elements.
  """

  @doc """
  Creates a new, empty `ORSet`.
  """
  @impl ConflictFreeReplicatedDataType
  def new do
    %{elements: MapSet.new(), tombstones: MapSet.new()}
  end

  @doc """
  Adds an element to the `ORSet` with a unique tag.
  """
  def add(%{elements: elements} = or_set, element, tag) do
    %{or_set | elements: MapSet.put(elements, {element, tag})}
  end

  @doc """
  Removes an element from the `ORSet`.
  This marks all instances of the element (identified by their tags) as removed.
  """
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
  def merge(or_set1, or_set2) do
    elements = MapSet.union(or_set1.elements, or_set2.elements)
    tombstones = MapSet.union(or_set1.tombstones, or_set2.tombstones)
    %{elements: elements, tombstones: tombstones}
  end
  @impl ConflictFreeReplicatedDataType
  def apply_effect({{:add, element, tag}, _sender_node}, %{elements: elements} = or_set) do
    {:ok, %{or_set | elements: MapSet.put(elements, {element, tag})}}
  end

  @impl ConflictFreeReplicatedDataType
  def apply_effect({{:remove, tags_to_remove}, _sender_node}, %{tombstones: tombstones} = or_set) do
    {:ok, %{or_set | tombstones: MapSet.union(tombstones, tags_to_remove)}}
  end

  @impl ConflictFreeReplicatedDataType
  def retrieve_value(or_set) do
    value(or_set)
  end

  @impl ConflictFreeReplicatedDataType
  def generate_effect({:add, element, tag}, _or_set) do
    {:ok, {:add, element, tag}}
  end

  @impl ConflictFreeReplicatedDataType
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
  def requires_state_for_effect({:add, _element, _tag}) do
    false
  end

  @impl ConflictFreeReplicatedDataType
  def requires_state_for_effect({:remove, _element}) do
    true
  end

  @impl ConflictFreeReplicatedDataType
  def are_equal(or_set1, or_set2) do
    or_set1 == or_set2
  end
end