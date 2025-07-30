defmodule AWSet do
    use GenServer

    @moduledoc """
    An implementation of an Add-Wins Set (AWSet) CRDT using a GenServer.

    This module provides basic functionality for an AWSet, including adding and deleting elements.
    Conflicts are resolved in favor of additions if an element is added and deleted concurrently.
    The set maintains internal `added_set` and `deleted_set` to track elements and their unique IDs.

    Note: This implementation uses `GenServer` for state management and does not directly
    implement the `ConflictFreeReplicatedDataType` behaviour as defined in the project.
    Integration with the broader Minidote CRDT framework would require adapting this
    module to conform to that behaviour.
    """

    # @type t :: {MapSet.t(), MapSet.t()} # Internal state representation {added_set, deleted_set}
    @type element :: any()
    @type id :: integer() # Unique ID for each element instance

    @doc """
    Creates a new, empty `AWSet`.

    Initializes the set with an empty `MapSet` for both added and deleted elements.

    ## Returns
      - `{:ok, pid, MapSet.t()}`: A tuple containing the PID of the new GenServer process
                                and its initial application view (empty MapSet).
    """
    @spec new() :: {:ok, pid(), MapSet.t()}
    def new() do
        new(MapSet.new())
    end

    @doc """
    Creates a new `AWSet` with an initial set of elements.

    Initializes the set with the provided `initial_set` for added elements and an empty
    `MapSet` for deleted elements. Each element in the `initial_set` is assigned a unique ID.

    ## Parameters
      - `initial_set`: A `MapSet.t()` of initial elements.

    ## Returns
      - `{:ok, pid, MapSet.t()}`: A tuple containing the PID of the new GenServer process
                                and its initial application view.
    """
    @spec new(MapSet.t()) :: {:ok, pid(), MapSet.t()}
    def new(initial_set) do
        add = Enum.map(initial_set, fn e -> {System.unique_integer(), e} end)
        {:ok, pid} = GenServer.start(__MODULE__, {add, []})
        app_view = initial_set
        {pid, app_view}
    end

    @doc """
    Adds an element to the `AWSet`.

    This function sends an `:add` message to the GenServer, which will assign a unique
    ID to the element and add it to the set.

    ## Parameters
      - `pid`: The PID of the `AWSet` GenServer.
      - `element`: The element to add.

    ## Returns
      - `any()`: The result of the GenServer call (typically an update message).
    """
    @spec add(pid(), element()) :: any()
    def add(pid, element) do
        update = GenServer.call(pid, {:add, element})
    end

    @doc """
    Deletes an element from the `AWSet`.

    This function sends a `:delete` message to the GenServer, which will mark
    all occurrences of the element (identified by their unique IDs) as deleted.

    ## Parameters
      - `pid`: The PID of the `AWSet` GenServer.
      - `element`: The element to delete.

    ## Returns
      - `any()`: The result of the GenServer call (typically an update message).
    """
    @spec delete(pid(), element()) :: any()
    def delete(pid, element) do
        update = GenServer.call(pid, {:delete, element})
    end

    @doc """
    Applies an update to the `AWSet`.

    This function sends an `:update` message to the GenServer. This is used internally
    to propagate changes within the CRDT.

    ## Parameters
      - `pid`: The PID of the `AWSet` GenServer.
      - `update`: The update message (e.g., `{:add, id, element}`, `{:del, deleted_entries_set}`).

    ## Returns
      - `MapSet.t()`: The updated application view of the set.
    """
    @spec update(pid(), {:add, id(), element()} | {:del, MapSet.t()}) :: MapSet.t()
    def update(pid, update) do
        app_view = GenServer.call(pid, {:update, update})
    end

    @impl true
    @doc """
    Initializes the `AWSet` GenServer.

    ## Parameters
      - `{added_set, deleted_set}`: A tuple containing the initial `added_set` (elements with IDs)
                                  and `deleted_set` (elements with IDs that are marked for deletion).

    ## Returns
      - `{:ok, state}`: The initial state of the GenServer.
    """
    @spec init({list(), list()}) :: {:ok, {list(), list()}}
    def init({added_set, deleted_set}) do
        add = Enum.map(added_set, fn e -> {System.unique_integer(), e} end)
        del = Enum.map(deleted_set, fn e -> {System.unique_integer(), e} end)
        {:ok, {add, del}}
    end

    @impl true
    @doc """
    Handles the `{:add, element}` call to add an element.

    Assigns a unique integer ID to the element and prepares an `{:add, id, element}`
    update message.

    ## Parameters
      - `{:add, element}`: The call message.
      - `from`: The caller's `GenServer.from()` tuple.
      - `state`: The current state of the GenServer.

    ## Returns
      - `{:reply, update, state}`: A reply containing the update message and the unchanged state.
    """
    @spec handle_call({:add, element()}, GenServer.from(), {list(), list()}) ::
                      {:reply, {:add, id(), element()}, {list(), list()}}
    def handle_call({:add, element}, from, state = {added_set, deleted_set}) do
        # Make new elements unique by assigning a unique id
        update = {:add, System.unique_integer(), element}
        {:reply, update, state}
    end

    @impl true
    @doc """
    Handles the `{:delete, element}` call to delete an element.

    Finds all matching elements in the `added_set` and prepares a `{:del, matching_elements}`
    update message.

    ## Parameters
      - `{:delete, element}`: The call message.
      - `from`: The caller's `GenServer.from()` tuple.
      - `state`: The current state of the GenServer.

    ## Returns
      - `{:reply, update, state}`: A reply containing the update message and the unchanged state.
    """
    @spec handle_call({:delete, element()}, GenServer.from(), {list(), list()}) ::
                      {:reply, {:del, MapSet.t()}, {list(), list()}}
    def handle_call({:delete, element}, from, state = {added_set, deleted_set}) do
        # Find all equal elements and save them as deleted
        matching_elements = MapSet.filter(added_set, fn {_id, el} -> el === element end)
        update = {:del, matching_elements}
        {:reply, update, state}
    end

    @impl true
    @doc """
    Handles the `{:update, {:add, id, element}}` call to apply an add update.

    Adds the new element with its ID to the `added_set` and calculates the new
    application view by taking the difference between `new_added` and `deleted_set`.

    ## Parameters
      - `{:update, {:add, id, element}}`: The call message.
      - `from`: The caller's `GenServer.from()` tuple.
      - `state`: The current state of the GenServer.

    ## Returns
      - `{:reply, app_view, new_state}`: A reply containing the updated application view and new state.
    """
    @spec handle_call({:update, {:add, id(), element()}}, GenServer.from(), {list(), list()}) ::
                      {:reply, MapSet.t(), {MapSet.t(), list()}}
    def handle_call({:update, {:add, id, element}}, from, {added_set, deleted_set}) do
        new_added = MapSet.put(added_set, {id, element})
        # remove the id and filter duplicates
        app_view = MapSet.difference(new_added,MapSet.new(deleted_set)) |> Enum.map(fn {_id, element} -> element end) |> MapSet.new()
        {:reply, app_view, {new_added, deleted_set}}
    end

    @impl true
    @doc """
    Handles the `{:update, {:del, deleted_entries_set}}` call to apply a delete update.

    Merges the `deleted_entries_set` into the `deleted_set` and calculates the new
    application view by taking the difference between `added_set` and `new_deleted`.

    ## Parameters
      - `{:update, {:del, deleted_entries_set}}`: The call message.
      - `from`: The caller's `GenServer.from()` tuple.
      - `state`: The current state of the GenServer.

    ## Returns
      - `{:reply, app_view, new_state}`: A reply containing the updated application view and new state.
    """
    @spec handle_call({:update, {:del, MapSet.t()}}, GenServer.from(), {list(), list()}) ::
                      {:reply, MapSet.t(), {list(), MapSet.t()}}
    def handle_call({:update, {:del, deleted_entries_set}}, from, {added_set, deleted_set}) do
        new_deleted = MapSet.union(MapSet.new(deleted_set), deleted_entries_set)
        # remove the id and filter duplicates
        app_view = MapSet.difference(MapSet.new(added_set),new_deleted) |> Enum.map(fn {_id, element} -> element end) |> MapSet.new()
        {:reply, app_view, {added_set, new_deleted}}
    end

    @impl true
    @doc """
    Handles any unexpected `GenServer.call` messages.

    Logs the unexpected call and returns an `:unexpected_input` error.

    ## Parameters
      - `info`: The unexpected call message.
      - `from`: The caller's `GenServer.from()` tuple.
      - `state`: The current state of the GenServer.

    ## Returns
      - `{:reply, :unexpected_input, state}`: A reply indicating an unexpected input and the unchanged state.
    """
    @spec handle_call(any(), GenServer.from(), {list(), list()}) ::
                      {:reply, :unexpected_input, {list(), list()}}
    def handle_call(info, from, state) do
        IO.inspect("Unexpected call:")
        IO.inspect(info)
        {:reply, :unexpected_input, state}
    end
end
