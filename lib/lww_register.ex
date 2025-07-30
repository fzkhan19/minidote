defmodule LWWRegister do
  @moduledoc """
  `LWWRegister` (Last-Write-Wins Register) is a CRDT that stores a single value,
  where conflicts are resolved by comparing timestamps. The value with the
  latest timestamp wins. If timestamps are equal, a tie-breaking rule (e.g.,
  lexicographical comparison of values) can be applied.
  """

  @doc """
  Creates a new `LWWRegister` with an initial value and timestamp.
  """
  def new(value, timestamp) do
    %{value: value, timestamp: timestamp}
  end

  @doc """
  Updates the `LWWRegister` with a new value and timestamp.
  """
  def update(register, new_value, new_timestamp) do
    if new_timestamp > register.timestamp do
      %{value: new_value, timestamp: new_timestamp}
    else
      register
    end
  end

  @doc """
  Returns the current value of the `LWWRegister`.
  """
  def value(register) do
    register.value
  end

  @doc """
  Merges two `LWWRegister`s.
  The register with the later timestamp wins. If timestamps are equal,
  the value from `register1` is chosen (arbitrary tie-breaking).
  """
  def merge(register1, register2) do
    if register1.timestamp > register2.timestamp do
      register1
    else if register2.timestamp > register1.timestamp do
      register2
    else
      # Tie-breaking: if timestamps are equal, prefer register1's value.
      # A more robust tie-breaking could be lexicographical comparison of values
      # or node ID.
      register1
    end
    end
  end
end