defmodule MVReg do
    use GenServer

    def new() do
        new(:inital_value)
    end

    def new(initial_value) do
        {:ok, pid} = GenServer.start(MVReg, initial_value)
        app_view = initial_value
        {pid, app_view}
    end

    def set(pid, value) do
        update = GenServer.call(pid, {:set, value})
    end

    def update(pid, update) do
        update = GenServer.call(pid, {:update, update})
    end

    @impl true
    def init(initial_value) do
        clock = Vectorclock.increment(Vectorclock.new(), self())
        values = [{clock, initial_value}]
        {:ok, {clock, values}}
    end

    @impl true
    def handle_call({:set, value}, from, state = {clock, values}) do
        new_clock = Vectorclock.increment(clock, self())
        update = {new_clock, value}
        {:reply, update, state}
    end

    @impl true
    def handle_call({:update, {update_clock, value}}, from, state = {clock, values}) do
        # is the update is already included?
        if Vectorclock.leq(update_clock, clock) do
            app_view = Enum.map(values, fn {c,v} -> v end)
            {:reply, app_view, state}
        else
            # Clear overwritten values
            new_clock = Vectorclock.merge(clock, update_clock)
            partial_values = Enum.reject(values, fn {c, v} -> Vectorclock.leq(c, update_clock) end)
            new_values = [{update_clock, value} | partial_values]
            app_view = Enum.map(new_values, fn {c,v} -> v end)
            {:reply, app_view, {new_clock, new_values}}
        end
    end

    @impl true
    def handle_call({:update, {update_clock, value}}, from, {clock, values}) do
    end

end
