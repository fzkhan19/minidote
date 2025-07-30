defmodule LWWEWSet do
  @moduledoc """
  `LWWEWSet` (Last-Write-Wins Element-Wise Set) is a CRDT that allows elements
  to be added and removed. It's similar to an `ORSet` but uses LWW semantics
  for concurrent additions and removals of the same element.
  Each element is stored with a set of (timestamp, node_id) pairs for additions
  and a similar set for removals.
  """

  @doc """
  Creates a new, empty `LWWEWSet`.
  """
  def new do
    %{adds: %{}, removes: %{}}
  end

  @doc """
  Adds an element to the `LWWEWSet` with a given timestamp and node ID.
  """
  def add(%{adds: adds} = set, element, timestamp, node_id) do
    new_adds = Map.update(adds, element, MapSet.new([{timestamp, node_id}]), fn existing ->
      MapSet.put(existing, {timestamp, node_id})
    end)
    %{set | adds: new_adds}
  end

  @doc """
  Removes an element from the `LWWEWSet` with a given timestamp and node ID.
  """
  def remove(%{removes: removes} = set, element, timestamp, node_id) do
    new_removes = Map.update(removes, element, MapSet.new([{timestamp, node_id}]), fn existing ->
      MapSet.put(existing, {timestamp, node_id})
    end)
    %{set | removes: new_removes}
  end

  @doc """
  Returns the current set of elements in the `LWWEWSet`.
  An element is present if its latest addition timestamp is greater than its
  latest removal timestamp. If timestamps are equal, addition wins.
  """
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
  For each element, the adds are the union of both sets of add tags.
  The removes are the union of both sets of remove tags.
  """
  def merge(set1, set2) do
    merged_adds = Map.merge(set1.adds, set2.adds, fn _k, v1, v2 -> MapSet.union(v1, v2) end)
    merged_removes = Map.merge(set1.removes, set2.removes, fn _k, v1, v2 -> MapSet.union(v1, v2) end)
    %{adds: merged_adds, removes: merged_removes}
  end
end