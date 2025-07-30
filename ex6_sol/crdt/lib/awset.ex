defmodule AWSet do
    use GenServer

    def new() do
        new(MapSet.new())
    end

    def new(initial_set) do
        {:ok, pid} = GenServer.start(AWSet, {initial_set, MapSet.new()})
        app_view = initial_set
        {pid, app_view}
    end

    def add(pid, element) do
        update = GenServer.call(pid, {:add, element})
    end

    def delete(pid, element) do
        update = GenServer.call(pid, {:delete, element})
    end

    def update(pid, update) do
        app_view = GenServer.call(pid, {:update, update})
    end

    @impl true
    def init({added_set, deleted_set}) do
        add = Enum.map(added_set, fn e -> {System.unique_integer(), e} end)
        del = Enum.map(deleted_set, fn e -> {System.unique_integer(), e} end)
        {:ok, {add, del}}
    end

    @impl true
    def handle_call({:add, element}, from, state = {added_set, deleted_set}) do
        # Make new elements unique by assigning a unique id
        update = {:add, System.unique_integer(), element}
        {:reply, update, state}
    end

    @impl true
    def handle_call({:delete, element}, from, state = {added_set, deleted_set}) do
        # Find all equal elements and save them as deleted
        matching_elements = MapSet.filter(added_set, fn {_id, element} -> element === element end)
        update = {:del, matching_elements}
        {:reply, update, state}
    end

    @impl true
    def handle_call({:update, {:add, id, element}}, from, {added_set, deleted_set}) do
        new_added = MapSet.put(added_set, {id, element})
        # remove the id and filter duplicates
        app_view = MapSet.difference(new_added,deleted_set) |> Enum.map(fn {_id, element} -> element end) |> MapSet.new()
        {:reply, app_view, {new_added, deleted_set}}
    end

    @impl true
    def handle_call({:update, {:del, deleted_entries_set}}, from, {added_set, deleted_set}) do
        new_deleted = MapSet.union(deleted_set, deleted_entries_set)
        # remove the id and filter duplicates
        app_view = MapSet.difference(added_set,new_deleted) |> Enum.map(fn {_id, element} -> element end) |> MapSet.new()
        {:reply, app_view, {added_set, new_deleted}}
    end

    @impl true
    def handle_call(info, from, state) do
        IO.inspect("Unexpected call:")
        IO.inspect(info)
        {:reply, :unexpected_input, state}
    end
end
