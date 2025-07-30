defmodule Vectorclock do

    # default value: 0
    def new() do
        # implicit tracking of infinity
        # explicit tracking of known processes
        %{} # maps   processes -> clock values (int)
    end

    def increment(vectorclock, process) do
        Map.update(vectorclock, process, 1, fn old -> old + 1 end)
    end

    def get(vectorclock, process) do
        Map.get(vectorclock, process, 0)
    end

    def leq(vectorclock1, vectorclock2) do
        # Def: for all i. vc1[i] <= vc2[i]
        Enum.all?( vectorclock1, fn {k, v} -> v <= Map.get(vectorclock2 ,k, 0) end)
    end

    def concurrent?(vc1, vc2) do
        !(leq(vc1, vc2) || leq(vc2, vc1))
    end

    def merge(vc1, vc2) do
        Map.merge(vc1, vc2, fn(_, v1, v2) -> max(v1, v2) end)
    end
end
