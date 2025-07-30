defmodule TPSet do
    use GenServer

    def new() do
        {:ok, pid} = GenServer.start(TPSet, {MapSet.new(), MapSet.new()})
        app_view = MapSet.new()
        {pid, app_view}
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
        initial_state = {added_set, deleted_set}
        {:ok, initial_state}
    end

    @impl true
    def handle_call({:add, element}, from, state = {added_set, deleted_set}) do
        add_update = MapSet.put(added_set, element)
        {:reply, {add_update, deleted_set}, state}
    end

    @impl true
    def handle_call({:delete, element}, from, state = {added_set, deleted_set}) do
        del_update = MapSet.put(deleted_set, element)
        {:reply, {added_set, del_update}, state}
    end

    @impl true
    def handle_call({:update, update = {up_add, up_del}}, from, {added_set, deleted_set}) do
        new_added = MapSet.union(up_add, added_set)
        new_deleted = MapSet.union(up_del, deleted_set)
        app_view = MapSet.difference(new_added,new_deleted)
        {:reply, app_view, {new_added, new_deleted}}
    end

    @impl true
    def handle_call(info, from, state) do
        IO.inspect("Unexpected call:")
        IO.inspect(info)
        {:reply, :unexpected_input, state}
    end
end
