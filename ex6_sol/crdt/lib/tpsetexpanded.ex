defmodule TPSetExp do
    use GenServer

    def new() do
        new(MapSet.new())
    end

    def new(initial_set) do
        {:ok, pid} = GenServer.start(TPSet, {initial_set, MapSet.new()})
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
        # Make new elements unique by assigning a unique id
        add = Enum.map(added_set, fn e -> {System.unique_integer(), e} end)
        del = Enum.map(deleted_set, fn e -> {System.unique_integer(), e} end)
        initial_state = {add, del}
        {:ok, initial_state}
    end

    @impl true
    def handle_call({:add, element}, from, state = {added_set, deleted_set}) do
        # Make new elements unique by assigning a unique id
        assigned_id = System.unique_integer()
        add_update = MapSet.put(added_set, {assigned_id, element})
        {:reply, {add_update, deleted_set}, state}
    end

    @impl true
    def handle_call({:delete, element}, from, state = {added_set, deleted_set}) do
        # Find all equal elements and save them as deleted
        matching_elements = MapSet.filter(added_set, fn {_id, element} -> element === element end)
        del_update = MapSet.union(deleted_set, matching_elements)
        {:reply, {added_set, del_update}, state}
    end

    @impl true
    def handle_call({:update, update = {up_add, up_del}}, from, {added_set, deleted_set}) do
        new_added = MapSet.union(up_add, added_set)
        new_deleted = MapSet.union(up_del, deleted_set)
        # remove the id and filter duplicates
        app_view = MapSet.difference(new_added,new_deleted) |> Enum.map(fn {_id, element} -> element end) |> MapSet.new()
        {:reply, app_view, {new_added, new_deleted}}
    end

    @impl true
    def handle_call(info, from, state) do
        IO.inspect("Unexpected call:")
        IO.inspect(info)
        {:reply, :unexpected_input, state}
    end
end

