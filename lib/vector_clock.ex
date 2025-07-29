defmodule Vector_Clock do
  def new() do
    %{}
  end

  def increment(vcClock, process) do
    Map.update(vcClock, process, 1, &(&1 + 1))
  end

  def get(vcClock, process) do
    Map.get(vcClock, process, 0)
  end

  def leq(vcClock_1, vcClock_2) do
    Enum.all?(vcClock_1, fn {k, v} -> v <= Map.get(vcClock_2, k, 0) end)
  end

  def merge(vcClock_1, vcClock_2) do
    Map.merge(vcClock_1, vcClock_2, fn _k, v1, v2 -> max(v1, v2) end)
  end
end
