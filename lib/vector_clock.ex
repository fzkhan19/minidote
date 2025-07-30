defmodule Vector_Clock do
  @moduledoc """
  A module for managing vector clocks, essential for tracking causal dependencies
  in distributed systems.

  Vector clocks are used to capture the causal history of events across a set of
  concurrent processes. Each process maintains its own logical clock, and updates
  are timestamped with the current state of the vector clock. When processes
  exchange information, they merge their vector clocks to reflect their combined
  causal knowledge.

  This implementation provides functions for creating new clocks, incrementing
  a process's clock, comparing clocks for causality, and merging clocks.
  """

  @type t :: %{node() => non_neg_integer()}

  @doc """
  Creates a new, empty vector clock.

  A new vector clock is represented as an empty map.

  ## Returns
    - `t()`: An empty map representing a new vector clock.
  """
  @spec new() :: t()
  def new() do
    %{}
  end

  @doc """
  Increments the clock value for a specific process in the vector clock.

  If the process is not yet in the vector clock, it is initialized with a value of 1.

  ## Parameters
    - `vcClock`: The current vector clock (`t()`).
    - `process`: The process (node name) whose clock value should be incremented.

  ## Returns
    - `t()`: A new vector clock with the specified process's clock value incremented.
  """
  @spec increment(t(), node()) :: t()
  def increment(vcClock, process) do
    Map.update(vcClock, process, 1, &(&1 + 1))
  end

  @doc """
  Retrieves the clock value for a specific process from the vector clock.

  If the process is not found in the vector clock, it returns 0, indicating
  that no events from that process have been observed yet.

  ## Parameters
    - `vcClock`: The current vector clock (`t()`).
    - `process`: The process (node name) whose clock value should be retrieved.

  ## Returns
    - `non_neg_integer()`: The clock value for the specified process, or 0 if not found.
  """
  @spec get(t(), node()) :: non_neg_integer()
  def get(vcClock, process) do
    Map.get(vcClock, process, 0)
  end

  @doc """
  Compares two vector clocks to determine if `vcClock_1` is less than or equal to `vcClock_2`
  (i.e., `vcClock_1` causally precedes or is concurrent with `vcClock_2`).

  This function checks two conditions:
  1. For every process in `vcClock_1`, its value must be less than or equal to its
     corresponding value in `vcClock_2`.
  2. `vcClock_2` must have at least all the processes present in `vcClock_1`.

  ## Parameters
    - `vcClock_1`: The first vector clock (`t()`).
    - `vcClock_2`: The second vector clock (`t()`).

  ## Returns
    - `boolean()`: `true` if `vcClock_1` is less than or equal to `vcClock_2`, `false` otherwise.
  """
  @spec leq(t(), t()) :: boolean()
  def leq(vcClock_1, vcClock_2) do
    Enum.all?(vcClock_1, fn {k, v} -> v <= Map.get(vcClock_2, k, 0) end)
  end

  @doc """
  Merges two vector clocks into a new one.

  The merged clock contains all processes from both input clocks. For any process
  present in both clocks, the maximum of their respective values is taken. This
  operation represents the union of causal knowledge from two concurrent states.

  ## Parameters
    - `vcClock_1`: The first vector clock (`t()`).
    - `vcClock_2`: The second vector clock (`t()`).

  ## Returns
    - `t()`: A new vector clock representing the merge of the two input clocks.
  """
  @spec merge(t(), t()) :: t()
  def merge(vcClock_1, vcClock_2) do
    Map.merge(vcClock_1, vcClock_2, fn _k, v1, v2 -> max(v1, v2) end)
  end
end
